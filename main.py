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

from datetime import datetime, timedelta, date
from datetime import time as dttime

import matplotlib.pyplot as plt

import pyimgur

import karelia

class UpdateDone (Exception):
    """Exception meaning that logs are up to date"""
    pass


class KillError (Exception):
    """Exception for when the bot is killed."""
    pass

class Heimdall:
    """Heimdall is the logging and statistics portion of the pantheon.

    Specifically, it maintains a full database of all messages sent
    to every room it's active in. It can also use that database to
    produce statistic readouts on demand.
    """

    def __init__(self, room, stealth, verbosity):
        self.room = room
        self.stealth = stealth
        self.verbose = verbosity
        self.heimdall = karelia.newBot('Heimdall', self.room)
        
        self.heimdall.stockResponses['shortHelp'] = "/me is a stats and logging bot"
        self.heimdall.stockResponses['longHelp'] = """
I am Heimdall, the one who watches. I see you. To invoke me powers:

    !stats will return a set of statistics about the one who summons me
    !stats @user shall direct my gaze upon @user instead

    !rank shall cause me to say where you do stand in the ranking of our citizens
    !rank @user shall again direct my gaze upon @user

    !roomstats causes me to ponder the fine and worthy history of &{}
    
    I am watched over by the one known as Pouncy Silverkitten, and my inner workings may be seen at https://github.com/PouncySilverkitten/heimdall
    """.format(self.room)
        self.url_regex = r"""(?i)\b((?:[a-z][\w-]+:(?:/{1,3}|[a-z0-9%])|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>\[\]]+|\(([^\s()<>\[\]]+|(\([^\s()<>\[\]]+\)))*\))+(?:\(([^\s()<>\[\]]+|(\([^\s()<>\[\]]+\)))*\)|[^\s`!(){};:'".,<>?\[\]]))"""

        self.get_imgur_key()
        self.imgur_client = pyimgur.Imgur(self.imgur_key)

        self.start_time = time.time()
        
        self.show("Connecting to euphoria...", end='')
        self.heimdall.connect(self.stealth)
        self.show(' done in {} seconds\nConnecting to database...'.format(round(time.time()-self.start_time, 2)), end='')

        self.start_time = time.time()
        self.conn = sqlite3.connect('{}.db'.format(self.room))
        self.c = self.conn.cursor()
        self.check_or_create_tables()
        self.show(' done in {} seconds.\nPulling logs...'.format(round(time.time()-self.start_time, 2)))

        self.start_time = time.time()
        self.get_room_logs()

        self.show('Done in {} seconds\nAdding nicks to table...'.format(round(time.time()-self.start_time, 2)), end='')
        self.conn.commit()
        
        self.c.execute('''SELECT COUNT(*) FROM {}'''.format(self.room))
        self.total_messages_all_time = self.c.fetchone()[0]

        self.conn.close()

        self.show("Ready")
        self.heimdall.disconnect()

    def get_imgur_key(self):
        """Returns imgur API key from file"""
        with open('imgur.json', 'r') as f:
            self.imgur_key = json.loads(f.read())[0]

    def show(self, text, end='\n'):
        """Only print if self.verbose"""
        if self.verbose:
            print(text, end)

    def check_or_create_tables(self):
        """Tries to create tables. If it fails, assume tables already exist."""

        try:
            self.c.execute('''  CREATE TABLE {}(
                                content text,
                                id text,
                                parent text,
                                senderid text,
                                sendername text,
                                normname text,
                                time real
                            )'''.format(self.room))
            self.c.execute('''CREATE UNIQUE INDEX messageID ON {}(id)'''.format(self.room))
        except sqlite3.OperationalError:
            self.show(' existing tables found...', end='')
        try:
            self.c.execute('''   CREATE TABLE {}posters(
                            name text,
                            normname text,
                            count real
                           )'''.format(self.room))
            self.c.execute('''CREATE UNIQUE INDEX normname ON {}posters(normname)'''.format(self.room))
            self.conn.commit()
        except sqlite3.OperationalError: pass
        try:
            self.c.execute('''CREATE TABLE rooms(name text, password integer)''')
            self.c.execute('''CREATE UNIQUE INDEX name ON rooms(name)''')
            self.conn.commit()
        except sqlite3.OperationalError: pass

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
        self.names = set()
        self.heimdall.send({'type': 'log', 'data': {'n': 1000}})

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
                    # Check if the log-reply is empty, i.e. the last log-reply
                    # contained exactly the first 1000 messages in the room's
                    # history
                    if len(reply['data']['log']) == 0:
                        raise UpdateDone
           
                    disp = reply['data']['log'][0]
                    self.show('    ({})[{}] {}'.format( datetime.utcfromtimestamp(disp['time']).strftime("%Y-%m-%d %H:%M"),
                                                        disp['sender']['name'].translate(self.heimdall.non_bmp_map),
                                                        disp['content'].translate(self.heimdall.non_bmp_map)))
        
                    # Append the data in this message to the data list ready for executemany
                    for message in reply['data']['log']:
                        if not 'parent' in message:
                            message['parent'] = ''
                        data.append((   message['content'], message['id'], message['parent'],
                                        message['sender']['id'], message['sender']['name'],
                                        self.heimdall.normaliseNick(message['sender']['name']),
                                        message['time']))
                        self.names.add(message['sender']['name'])
                    # Attempts to insert all the messages in bulk. If it fails, it will
                    # break out of the loop and we will assume that the logs are now
                    # up to date.
                    try:
                        self.c.executemany('''INSERT OR FAIL INTO {} VALUES(?, ?, ?, ?, ?, ?, ?)'''.format(self.room), data)
                    except sqlite3.IntegrityError:
                        raise UpdateDone
            
                    if len(reply['data']['log']) != 1000:
                        raise UpdateDone
                    else:
                        self.heimdall.send({'type': 'log', 'data': {'n': 1000, 'before': reply['data']['log'][0]['id']}})
                   
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
                    message['data']['time'])

        else:
            if not 'parent' in message:
                message['parent'] = ''
            data = (message['content'].replace('&', '{ampersand}'), message['id'], message['parent'],
                    message['sender']['id'], message['sender']['name'],
                    self.heimdall.normaliseNick(message['sender']['name']),
                    message['time'])
       
        self.c.execute('''INSERT OR FAIL INTO {} VALUES(?, ?, ?, ?, ?, ?, ?)'''.format(self.room), data)
        self.conn.commit()


    def next_day(self, day):
        """Returns the timestamp of midnight on the day following the timestamp given"""
        one_day = 60*60*24
        tomorrow = int(calendar.timegm(date.fromtimestamp(day).timetuple()) + one_day)
        return(tomorrow)

    def get_urls(self, content):
        """Gets page titles for urls in content

        Processes the content give, strip all the utls out using regex,
        remove any in the known_urls list, then get the page title for
        each, truncating it if necessary, and returning a string 
        containing them all.
        """
        response = ""
        known_urls = [  'imgur.com',
                        'google.com']
        urls = [url for url in re.findall(self.url_regex, content) if not url in '\n'.join(known_urls)]
        if len(urls) > 0:
            for match in urls:
                try:
                    url = 'http://' + match if not '://' in match else match
                    title = str(urllib.request.urlopen(url).read()).split('<title>')[1].split('</title>')[0]
                    title = html.unescape(codecs.decode(title, 'unicode_escape')).strip()
                    clear_title = title if len(title) <= 75 else '{}...'.format(title[0:72].strip())
                    response += "Title: {} \n".format(clear_title)
                except: pass

        return response

    def get_position(self, nick):
        """Returns the rank the supplied nick has by number of messages"""
        self.c.execute('''SELECT normname, count FROM (SELECT normname, COUNT(*) as count FROM {} GROUP BY normname) ORDER BY count DESC'''.format(self.room))
        normnick = self.heimdall.normaliseNick(nick)
        position = 0
        while True:
            position += 1
            result = self.c.fetchone()
            if result[0] == normnick:
                return(position)
            
        return("unknown")

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
        url = self.imgur_client.upload_image(filename).link
        os.remove(filename)
        return(url)

    def look_for_room_links(self, content):
        """Looks for and saves all possible rooms in message"""
        new_possible_rooms = set([room[1:] for room in content.split() if room[0] == '&'])
        with open('possible_rooms.json', 'r+') as f:
            possible_rooms = set(json.loads(f.read()))
            possible_rooms.union(new_possible_rooms)
            f.write(json.dumps(list(possible_rooms)))

    def get_user_stats(self, user, message_id):
        """Retrieves, formats and sends stats for user"""
        # First off, we'll get a known-good version of the requester name
        normnick = self.heimdall.normaliseNick(user)

        # Query gets the number of messages sent
        self.c.execute('''SELECT count(*) FROM {} WHERE normname is ?'''.format(self.room), (normnick,))
        count = self.c.fetchone()[0]

        if count == 0:
            self.heimdall.send('User @{} not found.'.format(user), message_id['data']['id'])
            return

        # Query gets the earliest message sent
        self.c.execute('''SELECT * FROM {} WHERE normname IS ? ORDER BY time ASC'''.format(self.room), (normnick,))
        earliest = self.c.fetchone()

        # Query gets the most recent message sent
        self.c.execute('''SELECT * FROM {} WHERE normname IS ? ORDER BY time DESC'''.format(self.room), (normnick,))
        latest = self.c.fetchone()

        days = {}
        self.c.execute('''SELECT time, COUNT(*) FROM {} WHERE normname IS ? GROUP BY CAST(time / 86400 AS INT)'''.format(self.room), (normnick,))
        daily_messages = self.c.fetchall()
        days = {}
        for message in daily_messages:
            day = datetime.utcfromtimestamp(message[0]).strftime("%Y-%m-%d")
            days[day] = message[1]

        try: messages_today = days[datetime.utcfromtimestamp(datetime.today().timestamp()).strftime("%Y-%m-%d")]
        except: messages_today = 0
        days_by_busyness = [(k, days[k]) for k in sorted(days, key=days.get, reverse=True)]
        busiest_day = days_by_busyness[0]

        # Calculate when the first message was sent, when the most recent message was sent, and the averate messages per day.
        first_message_sent = datetime.utcfromtimestamp(earliest[6]).strftime("%Y-%m-%d")
        last_message_sent = datetime.utcfromtimestamp(latest[6]).strftime("%Y-%m-%d")
        number_of_days = (datetime.strptime(last_message_sent, "%Y-%m-%d") - datetime.strptime(first_message_sent, "%Y-%m-%d")).days
        if last_message_sent == datetime.utcfromtimestamp(time.time()).strftime("%Y-%m-%d"): last_message_sent = "Today"
        number_of_days = number_of_days if number_of_days > 0 else 1

        last_28_days = sorted(days.items())[::-1][:28]
        title = "Messages by {}, last 28 days".format(user)
        data_x = [day[0] for day in last_28_days]
        data_y = [day[1] for day in last_28_days]
        last_28_graph = self.graph_data(data_x, data_y, title)
        last_28_file = self.save_graph(last_28_graph)
        last_28_url = self.upload_and_delete_graph(last_28_file)

        messages_all_time = days.items()
        title = "Messages by {}, all time".format(user)
        data_x = [day[0] for day in messages_all_time]
        data_y = [day[1] for day in messages_all_time]
        all_time_graph = self.graph_data(data_x, data_y, title)
        all_time_file = self.save_graph(all_time_graph)
        all_time_url = self.upload_and_delete_graph(all_time_file)

        # Get requester's position.
        position = self.get_position(normnick)
        self.c.execute('''SELECT count(*) FROM {}posters'''.format(self.room))
        no_of_posters = self.c.fetchone()[0]
        # Collate and send the lot.
        self.heimdall.send("""
User:\t\t\t\t\t{}
Messages:\t\t\t\t{}
Messages Sent Today:\t\t{}
First Message Date:\t\t{} days ago, on {}
First Message:\t\t\t{}
Most Recent Message:\t{}
Average Messages/Day:\t{}
Busiest Day:\t\t\t\t{}, with {} messages
Ranking:\t\t\t\t\t{} of {}.
{} {}""".format(user, count, messages_today, number_of_days, first_message_sent, earliest[0], last_message_sent, int(count / number_of_days), busiest_day[0], busiest_day[1], position, no_of_posters, all_time_url, last_28_url), message_id)

    def get_room_stats(self, message_id):
        """Gets and sends stats for rooms"""
        self.c.execute('''SELECT count(*) FROM {}'''.format(self.room))
        count = self.c.fetchone()[0]

        # Calculate top ten posters of all time
        self.c.execute('''SELECT * FROM {}posters ORDER BY count DESC LIMIT 10'''.format(self.room))
        results = self.c.fetchall()
        top_ten = ""
        for i, result in enumerate(results):
            top_ten += "{:2d}) {:<7}\t{}\n".format(i+1, int(result[2]), result[0])

        # Get activity over the last 28 days
        lower_bound = datetime.now() + timedelta(-28)
        lower_bound = time.mktime(lower_bound.timetuple())
        self.c.execute('''SELECT time, COUNT(*) FROM {} WHERE time > ? GROUP BY CAST(time / 86400 AS INT)'''.format(self.room), (lower_bound,))
        last_28_days = self.c.fetchall()
        per_day_last_four_weeks = int(sum([count[1] for count in last_28_days])/28)
        last_28_days.sort(key=operator.itemgetter(1))
        busiest = (datetime.utcfromtimestamp(last_28_days[-1][0]).strftime("%Y-%m-%d"), last_28_days[-1][1])
        last_28_days.sort(key=operator.itemgetter(0))

        midnight = time.mktime(datetime.combine(date.today(), dttime.min).timetuple())
        for tup in last_28_days:
            if tup[0] > midnight:
                messages_today = tup[1]
                break

        self.c.execute('''SELECT time, COUNT(*) FROM {} GROUP BY CAST(time/86400 AS INT)'''.format(self.room))
        messages_by_day = self.c.fetchall()

        title = "Messages in &{}, last 28 days".format(self.room)
        data_x = [date.fromtimestamp(int(day[0])) for day in last_28_days]
        data_y = [day[1] for day in last_28_days]
        last_28_graph = self.graph_data(data_x, data_y, title)
        last_28_file = self.save_graph(last_28_graph)
        last_28_url = self.upload_and_delete_graph(last_28_file)

        title = "Messages in &{}, all time".format(self.room)
        data_x = [date.fromtimestamp(int(day[0])) for day in messages_by_day]
        data_y = [day[1] for day in messages_by_day]
        all_time_graph = self.graph_data(data_x, data_y, title)
        all_time_file = self.save_graph(all_time_graph)
        all_time_url = self.upload_and_delete_graph(all_time_file)

        self.heimdall.send("There have been {} posts in &{} ({} today), averaging {} posts per day over the last 28 days (the busiest was {} with {} messages sent).\n\nThe top ten posters are:\n{}\n {} {}".format(count, self.room, messages_today, per_day_last_four_weeks, busiest[0], busiest[1], top_ten, all_time_url, last_28_url), message_id)

    def get_rank_of_user(self, user, message_id):
        """Gets and sends the position of the supplied user"""
        position = self.get_position(user)
        self.heimdall.find(str(int(position)), message_id)

    def get_parse_message(self):
        self.conn.commit()
        message = self.heimdall.parse()

        if message['type'] == 'send-event' or message['type'] == 'send-reply':
            self.insert_message(message)
            self.total_messages_all_time += 1
            if self.total_messages_all_time % 25000 == 0:
                self.heimdall.send("Congratulations on making the {}th post in &{}!".format(self.total_messages_all_time, self.room), message['data']['id'])

            self.look_for_room_links(message['data']['content'])
            self.get_urls(message['data']['content'])

            comm = message['data']['content'].split()
            if comm[0][0] == "!":
                if comm[0] == "!stats":
                    if len(comm) > 1 and comm[1][0] == "@":
                        self.get_user_stats(comm[1][0])
                    else:
                        self.get_user_stats(message['data']['sender']['name'], message['data']['id'])

                elif comm[0][0] == "!roomstats":
                    self.get_room_stats(message['data']['id'])
                
                elif comm[0] == "!rank":
                    if len(comm) > 1 and comm[0][1] == "@":
                        self.get_rank_of_user(comm[1][1:], message['data']['id'])
                    else:
                        self.get_rank_of_user(message['data']['sender']['name'], message['data']['id'])

    def main(self):
        """Main loop"""
        while True:
            try:
                self.conn.connect()
                self.heimdall.commit()
                self.get_parse_message()
            except Exception:
                self.heimdall.log()
                self.conn.close()
                self.heimdall.disconnect()
            finally:
                time.sleep(1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("room")
    parser.add_argument("--stealth", help="If enabled, bot will not present on nicklist", action="store_true")
    args = parser.parse_args()

    room = args.room
    stealth = args.stealth

    heimdall = Heimdall(room, stealth, True)

    def onSIGINT(signum, frame):
        """Handles sigints"""
    
        try:
            heimdall.conn.commit()
            heimdall.conn.close()
        finally:
            sys.exit()

    signal.signal(signal.SIGINT, onSIGINT)

    heimdall.main()

if __name__ == '__main__':
    main()
