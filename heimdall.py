"""Heimdall is a monitoring, logging, and statistics generating bot.


Heimdall will eventually have the ability to spread across multiple rooms.
The goal is that as well as being able to monitor euphoria.io and provide
accurate logs and statistics on request for the purposes of archiving and
curiosity, it should be able to track the movements of spammers and other
known-problematic individuals.
"""

import argparse
import calendar
import codecs
import html
import json
import operator
import os
import random
import re
import signal
import sqlite3
import string
import sys
import time
import urllib.request

from aylienapiclient import textapi
from datetime import datetime, timedelta, date
from datetime import time as dttime
from typing import *
from urllib.parse import urlparse

import matplotlib.pyplot as plt

from urlextract import URLExtract

import pyimgur

import karelia

class UpdateDone(Exception):
    """Exception meaning that logs are up to date"""
    pass


class KillError(Exception):
    """Exception for when the bot is killed."""
    pass

class Heimdall:
    """Heimdall is the logging and statistics portion of the pantheon.

    Specifically, it maintains a full database of all messages sent
    to every room it's active in. It can also use that database to
    produce statistic readouts on demand.
    """

    def __init__(self, room: Union[str, Tuple], **kwargs) -> None:
        if type(room) == str:
            self.room = room
            self.queue = None
        else:
            self.room = room[0]
            self.queue = room[1]

        self.stealth = kwargs['stealth'] if 'stealth' in kwargs else False
        self.verbose = kwargs['verbose'] if 'verbose' in kwargs else False
        self.force_new_logs = kwargs['new_logs'] if 'new_logs' in kwargs else False
        self.use_logs = kwargs['use_logs'] if 'use_logs' in kwargs else self.room

        BASE_DIR = os.path.dirname(os.path.abspath(__file__))

        if room == 'test_data':
            self.show("Testing mode enabled...", end='')
            self.tests = True
            self.database = os.path.join(BASE_DIR, "data/heimdall/test_data.db")
            self.show(" done")
        else:
            self.tests = kwargs['test'] if 'test' in kwargs else False
            self.database = os.path.join(BASE_DIR, "_heimdall.db")

        self.heimdall = karelia.newBot('Heimdall', self.room)

        self.files = {  'regex': 'data/heimdall/regex',
                        'possible_rooms': 'data/heimdall/possible_rooms.json',
                        'help_text': 'data/heimdall/help_text.json',
                        'block_list': 'data/heimdall/block_list.json',
                        'imgur': 'data/heimdall/imgur.json',
                        'aylien': 'data/heimdall/aylien.json',
                        'summ_list': 'data/heimdall/summ_list.json'}

        self.show("Loading files... ")
        
        for key in self.files:
            self.show("    Loading {}...".format(key), end=' ')
            try:
                if self.files[key].endswith('.json'):
                    with open(self.files[key], 'r') as f:
                        json.loads(f.read())
                self.show("done.")
            except:
                self.show('Unable to find file {}, creating (This will need to be manually edited before Heimdall can run successfully)'.format(self.files[key]))
                with open(self.files[key], 'w') as f:
                    f.write('[]')

        with open(self.files['help_text'], 'r') as f:
            self.show("Loading help text...", end=' ')
            try:
                help_text: Dict[str, str] = json.loads(f.read())
                self.heimdall.stockResponses['shortHelp'] = help_text['short_help']
                self.heimdall.stockResponses['longHelp'] = help_text['long_help'].format(self.room)
                if os.path.basename(os.path.dirname(os.path.realpath(__file__))) != "prod-yggdrasil":
                    self.heimdall.stockResponses['longHelp'] += "\nThis is a testing instance and may not be reliable."
                self.show("done")
            except Exception:
                self.heimdall.log()
                self.show("Error creating help text - see 'Heimdall &{}.log' for details.".format(self.room))

        with open(self.files['regex'], 'r') as f:
            self.show("Loading url regex...", end=' ')
            try:
                self.url_regex: str = f.read()
                self.show("done")
            except:
                self.heimdall.log()
                self.show("Error reading url regex - see 'Heimdall &{}.log' for details.".format(self.room))

        with open(self.files['imgur'], 'r') as f:
            self.show("Reading imgur key, creating Imgur client...", end=' ')
            try:
                self.imgur_key: str = json.loads(f.read())[0]
                self.imgur_client = pyimgur.Imgur(self.imgur_key)
                self.show("done")
            except Exception:
                self.heimdall.log()
                self.show("Error reading imgur key - see 'Heimdall &{}.log' for details.".format(self.room))

        with open(self.files['block_list'], 'r+') as f:
            try:
                self.show("Loading blocklists...", end=' ')
                block_domains: Dict[str, List[str]] = json.loads(f.read())
                if self.room in block_domains:
                    self.block_domains: List[str] = block_domains[self.room]
                else:
                    self.show("using master", end=' ')
                    self.block_domains: List[str] = block_domains['master'][:]
                self.show("done")
            except:
                self.heimdall.log()
                self.show("Error reading block list - see 'Heimdall &{}.log' for details.".format(self.room))

        with open(self.files["aylien"],'r') as f:
            self.show("Loading aylien credentials...", end=' ')
            try:
                aylien_creds = json.loads(f.read())
                self.summariser = textapi.Client(aylien_creds[0], aylien_creds[1])
                self.show("done")
            except:
                self.heimdall.log()
                self.show("Error reading aylien credentials - see 'Heimdall &{self.room}.log for details.")

        with open(self.files["summ_list"], 'r') as f:
            self.show("Loading summarise domains...", end=' ')
            try:
                self.summarise = json.loads(f.read())
                self.show("done")
            except:
                self.heimdall.log()
                self.show("Error reading summarise domain list - see 'Heimdall &{self.room}.log for details.")

        self.extractor = URLExtract()

        self.connect_to_database()
        self.show("Connecting to database...", end=' ')
        if self.force_new_logs:
            self.show("done\nDeleting messages...", end=' ')
            self.write_to_database('''DELETE FROM messages WHERE room IS ?''', values=(self.room,))
        self.show("done\nCreating tables...", end=' ')
        self.check_or_create_tables()
        self.show("done")
        
        if not self.tests:
            self.heimdall.connect(True)
            self.show("Getting logs...")
            self.get_room_logs()
            self.show("Done.")

        try:
            self.c.execute('''SELECT COUNT(*) FROM messages WHERE room IS ?''', (self.room,))
            self.total_messages_all_time = self.c.fetchone()[0]
        except:
            self.total_messages_all_time = 0
            self.heimdall.log()

        self.conn.close()

        self.show("Ready")
        if not self.tests:
            self.heimdall.disconnect()

    def write_to_database(self, statement, **kwargs):
        values = kwargs['values'] if 'values' in kwargs else ()
        mode = kwargs['mode'] if 'mode' in kwargs else "execute"
        
        if self.queue is not None:
            send = (statement, values, mode,)
            self.queue.put(send)
        
        else:
            if mode == "execute":
                self.c.execute(statement, values)
            elif mode == "executemany":
                self.c.executemany(statement, values)
            else:
                pass

        self.conn.commit()

    def connect_to_database(self):
        self.conn = sqlite3.connect(self.database)
        self.c = self.conn.cursor()
        self.check_or_create_tables()

    def show(self, *args, **kwargs):
        """Only print if self.verbose"""
        if self.verbose:
            print(*args, **kwargs)

    def check_or_create_tables(self):
        """Tries to create tables. If it fails, assume tables already exist."""
        self.write_to_database('''  CREATE TABLE IF NOT EXISTS messages(
                            content text,
                            id text,
                            parent text,
                            senderid text,
                            sendername text,
                            normname text,
                            time real,
                            room text,
                            globalid text
                        )''')
        self.write_to_database('''CREATE UNIQUE INDEX IF NOT EXISTS globalid ON messages(globalid)''')
        self.write_to_database('''CREATE TABLE IF NOT EXISTS aliases(master text, alias text)''')
        self.write_to_database('''CREATE UNIQUE INDEX IF NOT EXISTS master ON aliases(alias)''')

    def get_room_logs(self):
        """Create or update logs of the room.

        Specifically, request logs in batches of 1000 (the maximum allowed
        by the Heim API), then attempt to store these in the database.
        Continue to do this until an insert operation fails with an
        IntegrityError (meaning the insert cannot be performed due to
        a unique index on the message id column, in which case the logs
        we have are up to date) or until the returned list of messages
        has length less than 1000, indicating that the end of the room's
        history has been reached.
        """
        self.heimdall.send({'type': 'log', 'data': {'n': 1000}})
        
        # Query gets the most recent message sent so that we have something to compare to. If Heimdall is running in stand-alone mode, the sqlite3.IntegrityError that gets raised to signal that the logs are up to date will be received, but if it is writing to the database via Forseti, it has no way to receive that exception, so we have to check manually for that usecase.
        try:
            self.c.execute('''SELECT * FROM messages WHERE room IS ? ORDER BY time DESC''', (self.room,))
            latest = self.c.fetchone()
            latest_id = latest[8] if latest != None else None
        except sqlite3.OperationalError:
            latest_id = None
        self.show(f"{self.room}: {latest_id}")
        update_done = False

        while True:
            try:
                while True:
                    reply = self.heimdall.parse()
                    # Only progress if we receive something worth storing
                    if reply['type'] == 'log-reply' or reply['type'] == 'send-event':
                        data = []
                        break

                # Logs and single messages are structured differently.
                if reply['type'] == 'log-reply':
                    # Check if the log-reply is empty, i.e. the last log-reply contained exactly the first 1000 messages in the room's history
                    if len(reply['data']['log']) == 0:
                        raise UpdateDone
                    elif len(reply['data']['log']) < 1000:
                        update_done = True
                    else:
                        self.heimdall.send({'type': 'log', 'data': {'n': 1000, 'before': reply['data']['log'][0]['id']}})

                    disp = reply['data']['log'][0]
                    self.show('    ({} in &{})[{}] {}'.format( datetime.utcfromtimestamp(disp['time']).strftime("%Y-%m-%d %H:%M"),
                                                        self.room, disp['sender']['name'].translate(self.heimdall.non_bmp_map),
                                                        disp['content'].split('\n')[0][0:80].translate(self.heimdall.non_bmp_map)))

                    # Append the data in this message to the data list ready for executemany
                    for message in reply['data']['log']:
                        if latest_id == f"{self.room}{message['id']}": update_done = True
                        if not 'parent' in message:
                            message['parent'] = ''
                        data.append((   message['content'], message['id'], message['parent'],
                                        message['sender']['id'], message['sender']['name'],
                                        self.heimdall.normaliseNick(message['sender']['name']),
                                        message['time'], self.room, self.room+message['id']))

                    # Attempts to insert all the messages in bulk. If it fails, it will
                    # break out of the loop and we will assume that the logs are now
                    # up to date.
                    try:
                        self.write_to_database('''INSERT OR FAIL INTO messages VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)''', values=data, mode="executemany")
                    except sqlite3.IntegrityError:
                        raise UpdateDone

                    if update_done:
                        raise UpdateDone

                else:
                    self.insert_message(reply)
 
            except UpdateDone:
                break

    def insert_message(self, message):
        """Inserts a new message into the database of messages"""
        if 'data' in message:
            if not 'parent' in message['data']:
                message['data']['parent'] = ''
            data = (message['data']['content'], message['data']['id'], message['data']['parent'],
                    message['data']['sender']['id'], message['data']['sender']['name'],
                    self.heimdall.normaliseNick(message['data']['sender']['name']),
                    message['data']['time'], self.room, self.room+message['data']['id'])

        else:
            if not 'parent' in message:
                message['parent'] = ''
            data = (message['content'].replace('&', '{ampersand}'), message['id'], message['parent'],
                    message['sender']['id'], message['sender']['name'],
                    self.heimdall.normaliseNick(message['sender']['name']),
                    message['time'], self.room, self.room+message['id'])
 
        self.write_to_database('''INSERT OR FAIL INTO messages VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)''', values=data)

    def next_day(self, day):
        """Returns the timestamp of UTC midnight on the day following the timestamp given"""
        one_day = 60*60*24
        tomorrow = int(calendar.timegm(datetime.utcfromtimestamp(day).date().timetuple()) + one_day)
        return(tomorrow)

    def date_from_timestamp(self, timestamp):
        """Return human-readable date from timestamp"""
        return(datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d"))

    def get_urls(self, content):
        """Gets page titles for urls in content

        Processes the content give, strip all the utls out using regex,
        remove any in the known_urls list, then get the page title for
        each, truncating it if necessary, and returning a string
        containing them all.
        """

        urls = self.extractor.find_urls(content)
        known_urls = []
        for url in urls:
            for known_url in self.block_domains:
                if known_url in url:
                    known_urls.append(url)

        return([url for url in urls if not url in known_urls])

    def get_page_titles(self, urls):
        response = ""
        if len(urls) > 0:
            for match in urls:
                try:
                    url = 'http://' + match if not '://' in match else match
                    title = str(urllib.request.urlopen(url).read()).split('<title>')[1].split('</title>')[0]
                    title = html.unescape(codecs.decode(title, 'unicode_escape')).strip()
                    clear_title = title if len(title) <= 75 else '{}...'.format(title[0:72].strip())
                    response += "Title: {}\n".format(clear_title)
                except: pass

        return response

    def get_position(self, nick):
        """Returns the rank the supplied nick has by number of messages"""
        self.c.execute('''SELECT normname, count FROM (SELECT normname, COUNT(*) as count FROM messages WHERE room IS ? GROUP BY normname) ORDER BY count DESC''', (self.use_logs,))
        normnick = self.heimdall.normaliseNick(nick)
        position = 0
        while True:
            position += 1
            result = self.c.fetchone()
            if result == None:
                break
            if result[0] == normnick:
                return(position)

        return("unknown")

    def get_user_at_position(self, position):
        """Returns the user at the specified position"""
        self.c.execute('''SELECT sendername FROM (SELECT sendername, normname, COUNT(*) as count FROM messages WHERE room IS ? GROUP BY normname) ORDER BY count DESC''', (self.use_logs,))

        # Check to see they've passed a number
        try:
            position = int(position)
            assert position != 0
        except:
            return("The position you specified was invalid.")

        # In case they pass a number larger than the number of users
        try:
            for i in range(position): name = self.c.fetchone()[0]
        except:
            return("You requested a position which doesn't exist. There have been {} uniquely-named posters in &{}.".format(i, self.use_logs))
        return("The user at position {} is {}".format(position, name))

    def graph_data(self, data_x, data_y, title):
        """Graphs the data passed to it and returns a graph"""
        f, ax = plt.subplots(1)
        plt.title(title)
        ax.plot(data_x, data_y)
        plt.gcf().autofmt_xdate()
        ax.xaxis.set_major_locator(plt.MaxNLocator(10))
        ax.set_ylim(ymin=0)
        return(f)

    def save_graph(self, fig):
        """Saves the provided graph with a random filename"""
        filename = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))+".png"
        fig.savefig(filename)
        return(filename)

    def upload_and_delete_graph(self, filename):
        """Uploads passed file to imgur and deletes it"""
        try:
            url = self.imgur_client.upload_image(filename).link
        except:
            self.heimdall.log()
            url = "Imgur upload failed, sorry."
        os.remove(filename)
        return(url)

    def look_for_room_links(self, content):
        """Looks for and saves all possible rooms in message"""
        new_possible_rooms = set([room[1:] for room in content.split() if room[0] == '&'])
        with open(self.files['possible_rooms'], 'r') as f:
            possible_rooms = set(json.loads(f.read()))
        possible_rooms.union(new_possible_rooms)
        with open(self.files['possible_rooms'], 'w') as f:
            f.write(json.dumps(list(possible_rooms)))

    def get_user_stats(self, user, **kwargs):
        """Retrieves, formats and sends stats for user"""
        # First off, we'll get a known-good version of the requester name
        normnick = self.heimdall.normaliseNick(user)
        strict = not kwargs['aliases'] if 'aliases' in kwargs else True

        warning = ""
        if not strict:
            self.c.execute('''SELECT master FROM aliases WHERE alias = ?''',(normnick,))
            try:
                master = self.c.fetchall()[0][0]
            except:
                master = normnick
            self.c.execute('''SELECT alias FROM aliases WHERE master = ?''',(master,))
            reply = self.c.fetchall()
            if len(reply) == 0:
                warning = f"--aliases was ignored, since no aliases for user {user} are known. To correct, please post `!alias @{user.replace(' ','')}` in any room where @Heimdall is present."
                aliases = [normnick]
            else:
                aliases = [alias[0] for alias in reply]
        else:
            aliases = [normnick]

        # Query gets the number of messages sent. `','.join(['?']*len(aliases))` is used so that there are enough question marks for the number of aliases
        self.c.execute(f'''SELECT count(*) FROM messages WHERE room IS ? AND normname IN ({', '.join(['?']*len(aliases))})''', (self.use_logs, *aliases,))
        count = self.c.fetchone()[0]

        if count == 0:
            return('User @{} not found.'.format(user.replace(' ','')))

        # Query gets the earliest message sent
        self.c.execute(f'''SELECT * FROM messages WHERE room IS ? AND normname IN ({', '.join(['?']*len(aliases))}) ORDER BY time ASC''', (self.use_logs, *aliases,))
        earliest = self.c.fetchone()

        # Query gets the most recent message sent
        self.c.execute('''SELECT * FROM messages WHERE room IS ? AND normname IS ? ORDER BY time DESC''', (self.use_logs, normnick,))
        latest = self.c.fetchone()

        days = {}
        self.c.execute('''SELECT time, COUNT(*) FROM messages WHERE room IS ? AND normname IS ? GROUP BY CAST(time / 86400 AS INT)''', (self.use_logs, normnick,))
        daily_messages = self.c.fetchall()
        days = {}
        dates = [datetime.utcfromtimestamp(int(x)).strftime("%Y-%m-%d") for x in range(int(earliest[6]), int(time.time()), 60*60*24)]

        for date in dates:
            days[date] = 0

        for message in daily_messages:
            day = datetime.utcfromtimestamp(message[0]).strftime("%Y-%m-%d")
            days[day] = message[1]

        try: messages_today = days[datetime.utcfromtimestamp(datetime.today().timestamp()).strftime("%Y-%m-%d")]
        except: messages_today = 0
        days_by_busyness = [(k, days[k]) for k in sorted(days, key=days.get, reverse=True)]
        busiest_day = days_by_busyness[0]

        # Calculate when the first message was sent, when the most recent message was sent, and the averate messages per day.
        first_message_sent = self.date_from_timestamp(earliest[6])
        last_message_sent = self.date_from_timestamp(latest[6])

        # number_of_days only takes the average of days between the first message and the most recent message
        number_of_days = (datetime.strptime(last_message_sent, "%Y-%m-%d") - datetime.strptime(first_message_sent, "%Y-%m-%d")).days
        
        days_since_first_message = (datetime.today() - datetime.strptime(first_message_sent, "%Y-%m-%d")).days
        days_since_last_message = (datetime.today() - datetime.strptime(last_message_sent, "%Y-%m-%d")).days

        if first_message_sent == self.date_from_timestamp(time.time()): first_message_sent = "Today"
        else: "{} days ago, on {}".format(first_message_sent, days_since_first_message)
        
        if last_message_sent == self.date_from_timestamp(time.time()): last_message_sent = "Today"
        else: "{} days ago, on {}".format(last_message_sent, days_since_last_message)

        number_of_days = number_of_days if number_of_days > 0 else 1

        days = sorted(days.items())

        last_28_days = days[-28:]

        title = "Messages by {}, last 28 days".format(user)
        data_x = [day[0] for day in last_28_days]
        data_y = [day[1] for day in last_28_days]
        if self.tests:
            last_28_url = "url_goes_here"
        else:
            last_28_graph = self.graph_data(data_x, data_y, title)
            last_28_file = self.save_graph(last_28_graph)
            last_28_url = self.upload_and_delete_graph(last_28_file)

        title = "Messages by {}, all time".format(user)
        data_x = [day[0] for day in days]
        data_y = [day[1] for day in days]
        if self.tests:
            all_time_url = "url_goes_here"
        else:
            all_time_graph = self.graph_data(data_x, data_y, title)
            all_time_file = self.save_graph(all_time_graph)
            all_time_url = self.upload_and_delete_graph(all_time_file)

        # Get requester's position.
        position = self.get_position(normnick)
        self.c.execute('''SELECT COUNT(normname) FROM (SELECT normname, COUNT(*) as count FROM messages WHERE room IS ? GROUP BY normname) ORDER BY count DESC''', (self.use_logs,))
        no_of_posters = self.c.fetchone()[0]
        # Collate and send the lot.
        return("""
User:\t\t\t\t\t{}
Messages:\t\t\t\t{}
Messages Sent Today:\t\t{}
First Message Date:\t\t{}
First Message:\t\t\t{}
Most Recent Message:\t{}
Average Messages/Day:\t{}
Busiest Day:\t\t\t\t{}, with {} messages
Ranking:\t\t\t\t\t{} of {}.
{} {}""".format(user, count, messages_today, first_message_sent, earliest[0], last_message_sent, int(count / number_of_days), busiest_day[0], busiest_day[1], position, no_of_posters, all_time_url, last_28_url))

    def get_room_stats(self):
        """Gets and sends stats for rooms"""
        self.c.execute('''SELECT count(*) FROM messages WHERE room IS ?''', (self.use_logs,))
        count = self.c.fetchone()[0]

        # Calculate top ten posters of all time
        self.c.execute('''SELECT sendername,normname,COUNT(normname) AS freq FROM messages WHERE room IS ? GROUP BY normname ORDER BY freq DESC LIMIT 10''', (self.use_logs,))
        results = self.c.fetchall()
        top_ten = ""
        for i, result in enumerate(results):
            top_ten += "{:2d}) {:<7}\t{}\n".format(i+1, int(result[2]), result[0])

        # Get activity over the last 28 days
        lower_bound = self.next_day(time.time()) - (60*60*24*28)
        self.c.execute('''SELECT time, COUNT(*) FROM messages WHERE room IS ? AND time > ? GROUP BY CAST(time / 86400 AS INT)''', (self.use_logs, lower_bound,))
        last_28_days = self.c.fetchall()
        days = last_28_days[:]
        for day in days:
            last_28_days.append((self.next_day(day[0])-60*60*24, day[1],))
        days = last_28_days[:]
        for day in days:
            last_28_days.append((self.next_day(day[0])-60*60*24, day[1],))
            last_28_days.remove(day)
        per_day_last_four_weeks = int(sum([count[1] for count in last_28_days])/28)
        last_28_days.sort(key=operator.itemgetter(1))
        busiest = (datetime.utcfromtimestamp(last_28_days[-1][0]).strftime("%Y-%m-%d"), last_28_days[-1][1])
        last_28_days.sort(key=operator.itemgetter(0))

        midnight = calendar.timegm(datetime.utcnow().date().timetuple())
        messages_today = 0
        if midnight in [tup[0] for tup in last_28_days]:
            messages_today = dict(last_28_days)[midnight]

        self.c.execute('''SELECT time, COUNT(*) FROM messages WHERE room IS ? GROUP BY CAST(time/86400 AS INT)''', (self.use_logs,))
        messages_by_day = self.c.fetchall()

        title = "Messages in &{}, last 28 days".format(self.use_logs)
        data_x = [date.fromtimestamp(int(day[0])) for day in last_28_days]
        data_y = [day[1] for day in last_28_days]
        if self.tests:
            last_28_url = 'last_28_url'
        else:
            last_28_graph = self.graph_data(data_x, data_y, title)
            last_28_file = self.save_graph(last_28_graph)
            last_28_url = self.upload_and_delete_graph(last_28_file)

        title = "Messages in &{}, all time".format(self.use_logs)
        data_x = [date.fromtimestamp(int(day[0])) for day in messages_by_day]
        data_y = [day[1] for day in messages_by_day]
        if self.tests:
            all_time_url = 'all_time_url'
        else:
            all_time_graph = self.graph_data(data_x, data_y, title)
            all_time_file = self.save_graph(all_time_graph)
            all_time_url = self.upload_and_delete_graph(all_time_file)

        return(f"There have been {count} posts in &{self.use_logs} ({messages_today} today), averaging {per_day_last_four_weeks} posts per day over the last 28 days (the busiest was {busiest[0]} with {busiest[1]} messages sent).\n\nThe top ten posters are:\n{top_ten}\n{all_time_url} {last_28_url}")

    def get_rank_of_user(self, user):
        """Gets and sends the position of the supplied user"""
        position = self.get_position(user)
        return("Position {}".format(position))

    def get_message(self):
        """Gets messages from heim"""
        self.conn.commit()
        message = self.heimdall.parse()

        if message == "Killed":
            raise KillError

        return(message)

    def parse(self, message):
        if message['type'] == 'send-event' or message['type'] == 'send-reply':
            self.insert_message(message)
            self.total_messages_all_time += 1
            if self.total_messages_all_time % 25000 == 0:
                self.heimdall.send("Congratulations on making the {}th post in &{}!".format(self.total_messages_all_time, self.room), message['data']['id'])

            if message['type'] == 'send-reply': return
            
            if message['data']['content'].split(' ')[0] != "!ignore":
                self.look_for_room_links(message['data']['content'])
                urls = self.get_urls(message['data']['content'])
                summs = [url for url in urls if urlparse(url).netloc in self.summarise]
                urls = [url for url in urls if not url in summs]
                self.heimdall.send(self.get_page_titles(urls),message['data']['id'])
                for summ in summs:
                    self.heimdall.send("{}\n{}".format(self.get_page_titles([summ]), ' '.join(self.summariser.Summarize({"url": summ, "sentences_number": 2})['sentences'])), message['data']['id'])

            comm = message['data']['content'].split()
            if len(comm) > 0 and len(comm[0][0]) > 0 and comm[0][0] == "!":
                if comm[0] == "!stats":
                    if len(comm) > 1 and comm[1][0] == "@":
                        self.heimdall.send(self.get_user_stats(comm[1][1:]), message['data']['id'])
                    elif len(comm) == 1:
                        self.heimdall.send(self.get_user_stats(message['data']['sender']['name']), message['data']['id'])
                    else:
                        self.heimdall.send("Sorry, I didn't understand that. Syntax is !stats or !stats @user", message['data']['id'])

                elif comm[0] == "!roomstats":
                    if len(comm) > 1:
                        self.heimdall.send("Sorry, only stats for the current room are supported.", message['data']['id'])
                    else:
                        self.heimdall.send(self.get_room_stats(), message['data']['id'])
                
                elif comm[0] == "!rank":
                    if len(comm) > 1 and comm[1][0] == "@":
                        self.heimdall.send(self.get_rank_of_user(comm[1][1:]), message['data']['id'])
                    elif len(comm) > 1:
                        try:
                            pos = int(comm[1])
                            self.heimdall.send(self.get_user_at_position(pos), message['data']['id'])
                        except ValueError:
                            self.heimdall.send("Sorry, no name or number detected. Syntax is !rank (@user|<number>)", message['data']['id'])
                    else:
                        self.heimdall.send(self.get_rank_of_user(message['data']['sender']['name']), message['data']['id'])

                elif comm[0] == "!summ" or comm[0] == "!summarise":
                    if self.get_urls(comm[1]) == [comm[1]]:
                        self.heimdall.send(' '.join(self.summariser.Summarize({"url": comm[1], "sentences_number": 2})['sentences']), message['data']['id'])
                        summ_domain = urlparse(comm[1]).netloc
                        if not summ_domain in self.summarise:
                            with open(self.files["summ_list"],'r') as f:
                                self.summarise = json.loads(f.read())
                            self.summarise.append(summ_domain)
                            with open(self.files["summ_list"], 'w') as f:
                                f.write(json.dumps(self.summarise))

                elif comm[0] == "!alias":
                    pass                

    def main(self):
        """Main loop"""
        try:
            self.heimdall.connect()
            self.connect_to_database()
            while True:
                self.parse(self.get_message())
        except KillError:
            self.heimdall.log()
            self.conn.commit()
            self.conn.close()
            self.heimdall.disconnect()
            raise KillError
        except Exception:
            self.heimdall.log()
            self.conn.close()
            self.heimdall.disconnect()
        finally:
            time.sleep(1)
        
def on_sigint(signum, frame):
    try:
        heimdall.conn.commit()
        heimdall.conn.close()
        heimdall.heimdall.disconnect()
    finally:
        sys.exit()

def main(room, **kwargs):
    signal.signal(signal.SIGINT, on_sigint)

    while True:
        stealth = kwargs['stealth'] if 'stealth' in kwargs else False
        new_logs = kwargs['new_logs'] if 'new_logs' in kwargs else False
        use_logs = kwargs['use_logs'] if 'use_logs' in kwargs and kwargs['use_logs'] is not None else room if type(room) is str else room[0]
        verbose = kwargs['verbose'] if 'verbose' in kwargs else 'False'

        heimdall = Heimdall(room, stealth=stealth, new_logs=new_logs, use_logs=use_logs, verbose=verbose)
        
        try: 
            heimdall.main()
        except KillError:
            raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("room", nargs='?')
    parser.add_argument("--stealth", help="If enabled, bot will not present on nicklist", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true", dest="verbose")
    parser.add_argument("--force-new-logs", help="If enabled, Heimdall will delete any current logs for the room", action="store_true", dest="new_logs")
    parser.add_argument("--use-logs", type=str, dest="use_logs")
    args = parser.parse_args()

    room = args.room
    stealth = args.stealth
    new_logs = args.new_logs
    use_logs = args.use_logs
    verbose = args.verbose

    main(room, stealth=stealth, new_logs=new_logs, use_logs=use_logs, verbose=verbose)
