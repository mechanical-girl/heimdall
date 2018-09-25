"""
Heimdall is a monitoring, logging, and statistics generating bot.

Heimdall will eventually have the ability to spread across multiple rooms.
The goal is that as well as being able to monitor euphoria.io and provide
accurate logs and statistics on request for the purposes of archiving and
curiosity, it should be able to track the movements of spammers and other
known-problematic individuals.
"""

import argparse
import calendar
import codecs
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
from datetime import date, datetime
from datetime import time as dttime
from datetime import timedelta
from typing import Dict, List, Tuple, Union

import karelia
import matplotlib
import matplotlib.pyplot as plt

import pyimgur


class UpdateDone(Exception):
    """Exception meaning that logs are up to date"""
    pass


class UnknownMode(Exception):
    """Heimdall.write_to_database() has received an unknown mode"""
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
        self.database = os.path.join(BASE_DIR, "_heimdall.db")
        if os.path.basename(os.path.dirname(os.path.realpath(__file__))) == "prod-yggdrasil":
            self.prod = True
        else:
            self.prod = False

        self.heimdall = karelia.bot('Heimdall', self.room)

        self.files = {
            'possible_rooms': 'data/heimdall/possible_rooms.json',
            'help_text': 'data/heimdall/help_text.json',
            'imgur': 'data/heimdall/imgur.json',
            'messages_delivered': 'data/heimdall/messages_delivered.json'
        }

        self.show("Loading files... ")

        for key in self.files:
            self.show(f"    Loading {key}...", end=' ')
            try:
                if self.files[key].endswith('.json'):
                    with open(self.files[key], 'r') as f:
                        json.loads(f.read())
                self.show("done.")
            except FileNotFoundError:
                self.show(f'Unable to find file {self.files[key]}, creating (This will need to be manually edited before Heimdall can run successfully)')
                with open(self.files[key], 'w') as f:
                    f.write('[]')

            except json.decoder.JSONDecodeError:
                self.show(f"Unable to read JSON from file {self.files[key]}. This suggests that the JSON in this file has been corrupted. The file will need to be manually edited before Heimdall can run successfully.")

        with open(self.files['help_text'], 'r') as f:
            self.show("Loading help text...", end=' ')
            try:
                help_text: Dict[str, str] = json.loads(f.read())
                self.heimdall.stock_responses['short_help'] = help_text['short_help']
                self.heimdall.stock_responses['long_help'] = help_text['long_help'].format(self.room)
                if os.path.basename(os.path.dirname(os.path.realpath(__file__))) != "prod-yggdrasil":
                    self.heimdall.stock_responses['long_help'] += "\nThis is a testing instance and may not be reliable."
                self.show("done")
            except Exception:
                self.heimdall.log()
                self.show(f"Error creating help text - see 'Heimdall &{self.room}.log' for details.")

        with open(self.files['imgur'], 'r') as f:
            self.show("Reading imgur key, creating Imgur client...", end=' ')
            try:
                self.imgur_key: str = json.loads(f.read())[0]
                self.imgur_client = pyimgur.Imgur(self.imgur_key)
                self.show("done")
            except Exception:
                self.heimdall.log()
                self.show(f"Error reading imgur key - see 'Heimdall &{self.room}.log' for details.")


        self.connect_to_database()
        self.show("Connecting to database...", end=' ')
        if self.force_new_logs:
            self.show("done\nDeleting messages...", end=' ')
            self.write_to_database('''DELETE FROM messages WHERE room IS ?''', values=(self.room, ))
            self.show("done\nCreating tables...", end=' ')
        self.check_or_create_tables()
        self.show("done")

        if self.prod:
            self.heimdall.connect(True)
            self.show("Getting logs...")
            self.get_room_logs()
            self.show("Done.")

        try:
            self.c.execute('''SELECT COUNT(*) FROM messages WHERE room IS ?''', (self.room, ))
            self.total_messages_all_time = self.c.fetchone()[0]
        except:
            self.total_messages_all_time = 0
            self.heimdall.log()

        self.conn.close()

        self.show("Ready")
        if self.prod:
            self.heimdall.disconnect()

    def write_to_database(self, statement, **kwargs):
        """Optionally, pass values=values, mode=mode."""
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
                raise UnknownMode

        self.conn.commit()

    def connect_to_database(self):
        self.conn = sqlite3.connect(self.database)
        self.c = self.conn.cursor()
        self.check_or_create_tables()

    def show(self, *args, **kwargs):
        """
        Only print if self.verbose or verbose=True in kwargs

        >>> h = Heimdall('test')
        >>> h.show('Test', override=True, end='')
        Test 
        >>> h.show('Tes', end='t')
        >>> h.show('Test')
        >>> h.verbose = True
        >>> h.show('Test')
        Test 
        <BLANKLINE>
        >>> h.show('Test', override=False)
        Test 
        <BLANKLINE>
        """

        override = True if 'override' in kwargs and kwargs['override'] else False
        end = kwargs['end'] if 'end' in kwargs else '\n'
        if self.verbose or override:
            print(*args, end)

    def check_or_create_tables(self):
        """
        Tries to create tables. If it fails, assume tables already exist.
        """

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
        self.write_to_database('''CREATE TABLE IF NOT EXISTS aliases(master text, alias text, normalias text)''')
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
            self.c.execute('''SELECT * FROM messages WHERE room IS ? ORDER BY time DESC''', (self.room, ))
            latest = self.c.fetchone()
            latest_id = latest[8] if latest is not None else None
        except sqlite3.OperationalError:
            latest_id = None
        self.show(f"{self.room}: {latest_id}")
        update_done = False

        while True:
            try:
                while True:
                    reply = self.heimdall.parse()
                    # Only progress if we receive something worth storing
                    if reply.type == 'log-reply' or reply.type == 'send-event':
                        data = []
                        break

                # Logs and single messages are structured differently.
                if reply.type == 'log-reply':
                    # Check if the log-reply is empty, i.e. the last log-reply contained exactly the first 1000 messages in the room's history
                    if len(reply.data.log) == 0:
                        raise UpdateDone
                    elif len(reply.data.log) < 1000:
                        update_done = True
                    else:
                        self.heimdall.send({'type': 'log', 'data': {'n': 1000, 'before': reply.data.log[0]['id']}})
 
                        disp = reply.data.log[0]

                        safe_content = disp['content'].split('\n')[0][0:80].translate(self.heimdall.non_bmp_map)
                        self.show(f"    ({datetime.utcfromtimestamp(disp['time']).strftime('%Y-%m-%d %H:%M')} in &{self.room})[{disp['sender']['name'].translate(self.heimdall.non_bmp_map)}] {safe_content}")
                    # Append the data in this message to the data list ready for executemany
                    for message in reply.data.log:
                        if latest_id == f"{self.room}{message['id']}":
                            update_done = True
                        if 'parent' not in message:
                            message['parent'] = ''
                        data.append(
                            (message['content'], message['id'],
                             message['parent'], message['sender']['id'],
                             message['sender']['name'],
                             self.heimdall.normalise_nick(
                                 message['sender']['name']), message['time'],
                             self.room, self.room + message['id']))

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
        if isinstance(message, karelia.Packet):
            if 'parent' not in dir(message.data):
                message.data.parent = ''

            data = (message.data.content, message.data.id,
                    message.data.parent, message.data.sender.id,
                    message.data.sender.name,
                    self.heimdall.normalise_nick(message.data.sender.name),
                    message.data.time, self.room,
                    self.room + message.data.id)

        else:
            if 'parent' not in message:
                message['parent'] = ''

            data = (message['content'].replace('&', '{ampersand}'), message['id'], message['parent'],
                    message['sender']['id'], message['sender']['name'],
                    self.heimdall.normalise_nick(message['sender']['name']),
                    message['time'], self.room, self.room + message['id'])

        self.write_to_database('''INSERT INTO messages VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)''', values=data)

    def next_day(self, day):
        """
        Returns the timestamp of UTC midnight on the day following the timestamp given

        >>> h = Heimdall('test')
        >>> import time
        >>> h.next_day(1534774799)
        1534809600
        """

        one_day = 60 * 60 * 24
        tomorrow = int(calendar.timegm(datetime.utcfromtimestamp(day).date().timetuple()) + one_day)
        return (tomorrow)

    def date_from_timestamp(self, timestamp):
        """
        Return human-readable date from timestamp

        >>> h = Heimdall('test')
        >>> h.date_from_timestamp(1534774799)
        '2018-08-20'
        """
        return (datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d"))

    def get_position(self, nick):
        """Returns the rank the supplied nick has by number of messages"""
        master_nick = self.heimdall.normalise_nick(self.get_master_nick_of_user(nick))
        position = 1
        pairs = self.get_count_user_pairs()
        pair = next(pairs)
        while True:
            if pair is None:
                return None
            elif self.heimdall.normalise_nick(pair[1]) == master_nick:
                return position
            position += 1
            pair = next(pairs)

    def get_master_nick_of_user(self, user):
        """For a given user, returns their 'master nick' if aliases are known for them, else their username"""
        normnick = self.heimdall.normalise_nick(user)
        self.c.execute('''SELECT master FROM aliases WHERE normalias = ?''', (normnick,))
        try:
            master_nick = self.c.fetchall()[0][0]
            return master_nick
        except IndexError:
            return user

    def get_user_at_position(self, position):
        """Returns the user at the specified position"""

        # Check to see they've passed a number
        try:
            position = int(position)
            assert position != 0
        except:
            return "The position you specified was invalid."

        pairs = self.get_count_user_pairs()

        self.c.execute('''SELECT COUNT(*) FROM (SELECT COUNT(*) AS amount, CASE master IS NULL WHEN TRUE THEN sendername ELSE master END AS name FROM messages LEFT JOIN aliases ON normname=normalias WHERE room=? GROUP BY name ORDER BY amount DESC)''', (self.use_logs, ))
        total_posters = self.c.fetchall()[0][0]
        if position > total_posters:
            return f"Position not found; there have been {total_posters} posters in &{self.use_logs}."

        for i in range(position):
            name = next(pairs)[1]

        return "The user at position {} is @{}.".format(position, self.heimdall.normalise_nick(name))

    def graph_data(self, data_x, data_y, title):
        """Graphs the data passed to it and returns a graph"""
        f, ax = plt.subplots(1)
        plt.title(title)
        ax.plot(data_x, data_y)
        plt.gcf().autofmt_xdate()
        ax.xaxis.set_major_locator(plt.MaxNLocator(10))
        ax.set_ylim(ymin=0)
        return f

    def save_graph(self, fig):
        """Saves the provided graph with a random filename"""
        filename = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)) + ".png"
        fig.savefig(filename)
        return filename

    def upload_and_delete_graph(self, filename):
        """Uploads passed file to imgur and deletes it"""
        try:
            url = self.imgur_client.upload_image(filename).link
        except:
            self.heimdall.log()
            url = "Imgur upload failed, sorry."
        os.remove(filename)
        return url

    def get_aliases(self, user):
        normnick = self.heimdall.normalise_nick(user)

        self.c.execute('''SELECT master FROM aliases WHERE normalias = ?''', (normnick, ))
        try:
            master = self.c.fetchall()[0][0]
        except:
            master = normnick
        self.c.execute('''SELECT alias FROM aliases WHERE master = ?''', (master, ))
        reply = self.c.fetchall()
        if not reply:
            return []
        else:
            return [alias[0] for alias in reply]

    def get_user_stats(self, user, options):
        """Retrieves, formats and sends stats for user"""
        # First off, we'll get a known-good version of the requester name
        normnick = self.heimdall.normalise_nick(user)

        if 'aliases' in options:
            aliases = [self.heimdall.normalise_nick(nick) for nick in self.get_aliases(user)]

            if not aliases:
                aliases_used = f"--aliases was ignored, since no aliases for user {user} are known. To correct, please post `!alias @{user.replace(' ','')}` in any room where @Heimdall is present."
                aliases = [normnick]
            else:
                aliases_used = f"{len(aliases)-1} aliases used."

        else:
            aliases_used = "No aliases used."
            aliases = [normnick]

        # Query gets the number of messages sent. `','.join(['?']*len(aliases))` is used so that there are enough question marks for the number of aliases
        self.c.execute(f'''SELECT count(*) FROM messages WHERE room IS ? AND normname IN ({', '.join(['?']*len(aliases))})''', (self.use_logs, *aliases,))
        count = self.c.fetchone()[0]

        if count == 0:
            return ('User @{} not found.'.format(user.replace(' ', '')))

        if options == ['aliases']:
            return "No options specified. Please only use --aliases or -a in conjunction with --messages (or -m), --engagement (-e), --text (-t), or a combination thereof."

        if 'messages' in options:
            # Query gets the earliest message sent
            self.c.execute(f'''SELECT * FROM messages WHERE room IS ? AND normname IN ({', '.join(['?']*len(aliases))}) ORDER BY time ASC''', (self.use_logs, *aliases,))
            earliest = self.c.fetchone()

            # Query gets the most recent message sent
            self.c.execute(f'''SELECT * FROM messages WHERE room IS ? AND normname IN ({', '.join(['?']*len(aliases))}) ORDER BY time DESC''', (self.use_logs, *aliases,))
            latest = self.c.fetchone()

            days = {}
            self.c.execute(f'''SELECT time, COUNT(*) FROM messages WHERE room IS ? AND normname IN ({', '.join(['?']*len(aliases))}) GROUP BY CAST(time / 86400 AS INT)''', (self.use_logs, *aliases,))
            daily_messages = self.c.fetchall()
            days = {}
            dates = [datetime.utcfromtimestamp(int(x)).strftime("%Y-%m-%d") for x in range(int(earliest[6]), int(time.time()), 60 * 60 * 24)]

            for _date in dates:
                days[_date] = 0

            for message in daily_messages:
                day = datetime.utcfromtimestamp(message[0]).strftime("%Y-%m-%d")
                days[day] = message[1]

            try:
                messages_today = days[datetime.utcfromtimestamp(datetime.today().timestamp()).strftime("%Y-%m-%d")]
            except KeyError:
                messages_today = 0

            days_by_busyness = [(k, days[k]) for k in sorted(days, key=days.get, reverse=True)]
            busiest_day = days_by_busyness[0]

            # Calculate when the first message was sent, when the most recent message was sent, and the averate messages per day.
            first_message_sent = self.date_from_timestamp(earliest[6])
            last_message_sent = self.date_from_timestamp(latest[6])

            # number_of_days only takes the average of days between the first message and the most recent message
            number_of_days = (datetime.strptime(last_message_sent, "%Y-%m-%d") - datetime.strptime(first_message_sent, "%Y-%m-%d")).days

            days_since_first_message = (datetime.today() - datetime.strptime(first_message_sent, "%Y-%m-%d")).days
            days_since_last_message = (datetime.today() - datetime.strptime(last_message_sent, "%Y-%m-%d")).days

            if first_message_sent == self.date_from_timestamp(time.time()):
                first_message_sent = "Today"
            else:
                "{} days ago, on {}".format(first_message_sent, days_since_first_message)

            if last_message_sent == self.date_from_timestamp(time.time()):
                last_message_sent = "Today"
            else:
                last_message_sent = "{} days ago, on {}".format(last_message_sent, days_since_last_message)

            try:
                avg_messages_per_day = int(count/number_of_days)
            except ZeroDivisionError:
                avg_messages_per_day = 0

            days = sorted(days.items())

            last_28_days = days[-28:]

            title = "Messages by {}, last 28 days".format(user)
            data_x = [day[0] for day in last_28_days]
            data_y = [day[1] for day in last_28_days]
            if not self.prod:
                last_28_url = "url_goes_here"
            else:
                last_28_graph = self.graph_data(data_x, data_y, title)
                last_28_file = self.save_graph(last_28_graph)
                last_28_url = self.upload_and_delete_graph(last_28_file)

            title = "Messages by {}, all time".format(user)
            data_x = [day[0] for day in days]
            data_y = [day[1] for day in days]
            if not self.prod:
                all_time_url = "url_goes_here"
            else:
                all_time_graph = self.graph_data(data_x, data_y, title)
                all_time_file = self.save_graph(all_time_graph)
                all_time_url = self.upload_and_delete_graph(all_time_file)

            # Get requester's position.
            position = self.get_position(normnick)
            self.c.execute(
                '''SELECT COUNT(normname) FROM (SELECT normname, COUNT(*) as count FROM messages WHERE room IS ? GROUP BY normname) ORDER BY count DESC''',
                (self.use_logs, ))
            no_of_posters = self.c.fetchone()[0]

            message_results = f"""User:\t\t\t\t\t{user}
Messages:\t\t\t\t{count}
Messages Sent Today:\t\t{messages_today}
First Message Date:\t\t{first_message_sent}
First Message:\t\t\t{earliest[0]}
Most Recent Message:\t{last_message_sent}
Average Messages/Day:\t{avg_messages_per_day}
Busiest Day:\t\t\t\t{busiest_day[0]}, with {busiest_day[1]} messages
Ranking:\t\t\t\t\t{position} of {no_of_posters}.
{all_time_url} {last_28_url}

"""

        else:
            message_results = ""

        if 'engagement' in options: 
            engagement_results = f"User engagement:\n{self.get_user_engagement_table(user)}\n"
        else:
            engagement_results = ""

        if 'text' in options:
            self.c.execute('''SELECT COUNT(*) from messages WHERE room IS ? AND normname IS ? AND parent IS ?''', (self.use_logs, normnick, '',))
            tlts = round((self.c.fetchall()[0][0] * 100) / count, 2)
            text_results = f"TLTs %:\t{tlts}\n\n"
            self.c.execute(f'''SELECT content FROM messages WHERE room IS ? AND normname IN ({', '.join(['?']*len(aliases))}) ORDER BY random() LIMIT 1000''', (self.use_logs, *aliases,))
            messages = [message[0] for message in self.c.fetchall()]

            average_message_words = 0
            average_message_characters = 0
            sample_size = len(messages)
 
            for message in messages:
                average_message_words += len(message.split())
                average_message_characters += len(message)
            average_message_words /= sample_size
            average_message_characters /= sample_size

            text_results += f"Average words per message:\t\t{int(average_message_words)}\nAverage characters per message:\t{int(average_message_characters)}\n(Sample size of {sample_size})\n\n" 

        else:
            text_results = ""

        # Collate and send the lot.
        return (f"""{message_results}{engagement_results}{text_results}{aliases_used}""")

    def get_room_stats(self):
        """Gets and sends stats for rooms"""

        self.c.execute('''SELECT count(*) FROM messages WHERE room IS ?''', (self.use_logs, ))
        count = self.c.fetchone()[0]

        # Calculate top ten posters of all time
        top_ten = ""
        posters = self.get_count_user_pairs()
        i = 0
        for pair in posters:
            i += 1
            top_ten += "{:2d}) {:<7}\t{}\n".format(i, int(pair[0]), pair[1])
            if i == 10:
                break

        self.c.execute('''SELECT COUNT(*) FROM (SELECT COUNT(*) AS amount, CASE master IS NULL WHEN TRUE THEN sendername ELSE master END AS name FROM messages LEFT JOIN aliases ON normname=normalias WHERE room=? GROUP BY name ORDER BY amount DESC)''', (self.use_logs, ))
        total_posters = self.c.fetchall()[0][0]

        # Get activity over the last 28 days
        lower_bound = self.next_day(time.time()) - (60 * 60 * 24 * 28)
        self.c.execute('''SELECT time, COUNT(*) FROM messages WHERE room IS ? AND time > ? GROUP BY CAST(time / 86400 AS INT)''', (self.use_logs, lower_bound,))
        last_28_days = self.c.fetchall()

        days = last_28_days[:]
        last_28_days = []
        for day in days:
            last_28_days.append((self.next_day(day[0]) - 60 * 60 * 24, day[1],))

        per_day_last_four_weeks = int(sum([count[1] for count in last_28_days]) / 28)
        last_28_days.sort(key=operator.itemgetter(1))

        self.c.execute('''SELECT time, COUNT(*) FROM messages WHERE room IS ? GROUP BY CAST(time/86400 AS INT)''', (self.use_logs, ))
        messages_by_day = self.c.fetchall()

        title = "Messages in &{}, last 28 days".format(self.use_logs)
        data_x = [date.fromtimestamp(int(day[0])) for day in last_28_days]
        data_y = [day[1] for day in last_28_days]
        if not self.prod:
            last_28_url = 'last_28_url'
        else:
            last_28_graph = self.graph_data(data_x, data_y, title)
            last_28_file = self.save_graph(last_28_graph)
            last_28_url = self.upload_and_delete_graph(last_28_file)

        title = "Messages in &{}, all time".format(self.use_logs)
        data_x = [date.fromtimestamp(int(day[0])) for day in messages_by_day]
        data_y = [day[1] for day in messages_by_day]
        if not self.prod:
            all_time_url = 'all_time_url'
        else:
            all_time_graph = self.graph_data(data_x, data_y, title)
            all_time_file = self.save_graph(all_time_graph)
            all_time_url = self.upload_and_delete_graph(all_time_file)

        if last_28_days is None:
            return f"There have been {count} posts in &{self.use_logs}, though none in the last 28 days.\n\nThe top ten posters are:\n{top_ten}\n{all_time_url}"
        else:
            if len(last_28_days) > 0:
                busiest = (datetime.utcfromtimestamp(last_28_days[-1][0]).strftime("%Y-%m-%d"), last_28_days[-1][1])
                last_28_days.sort(key=operator.itemgetter(0))

                midnight = calendar.timegm(datetime.utcnow().date().timetuple())
                messages_today = 0
                if midnight in [tup[0] for tup in last_28_days]:
                    messages_today = dict(last_28_days)[midnight]

                busiest_last_28 = f" (the busiest was {busiest[0]} with {busiest[1]} messages sent)"

            else:
                messages_today = 0
                busiest_last_28 = ""

        return f"There have been {count} posts in &{self.use_logs} ({messages_today} today) from {total_posters} posters, averaging {per_day_last_four_weeks} posts per day over the last 28 days{busiest_last_28}.\n\nThe top ten posters are:\n{top_ten}\n{all_time_url} {last_28_url}"

    def get_rank_of_user(self, user):
        """Gets and sends the position of the supplied user"""
        rank = self.get_position(user)
        if rank == None:
            return f"User @{user} not found."
        return f"Position {rank}"

    def get_user_engagement_table(self, user):
        """(self, user) -> table"""

        # Yes, this function is defined inside another function. That's the way I want it. Don't unindent it.
        def formatta(tup, total):
            return f"{'{:4.2f}'.format(round(tup[1]*100/total, 2))}\t\t{tup[0]}\n"

        normnick = self.heimdall.normalise_nick(user)

        aliases = [self.heimdall.normalise_nick(nick) for nick in self.get_aliases(user)]
        if not aliases:
            aliases = [normnick]

        # Get all messages by user
        self.c.execute(f'''SELECT count(*) FROM messages WHERE room IS ? AND normname IN ({', '.join(['?']*len(aliases))})''', (self.use_logs, *aliases,))
        total_count = self.c.fetchall()[0][0]

        # Get the number of parents per user they replied to
        self.c.execute(f'''SELECT sendername, COUNT(*) AS count FROM messages WHERE room IS ? AND id IN (SELECT parent FROM messages WHERE room IS ? AND normname IN ({', '.join(['?']*len(aliases))})) GROUP BY sendername ORDER BY count DESC ''', (self.use_logs, self.use_logs, *aliases,))
        parents_replied_to = [item for item in self.c.fetchall() if self.heimdall.normalise_nick(item[0]) not in aliases][:10]

        self.c.execute(f'''SELECT count(*) FROM messages WHERE normname IS ? AND parent IN (SELECT id FROM messages WHERE room IS ? AND normname IN ({', '.join(['?']*len(aliases))}))''', (self.heimdall.normalise_nick(user), self.use_logs, *aliases,))
        self_replies = self.c.fetchall()[0][0]

        table = ""

        for pair in parents_replied_to:
            table += formatta(pair, total_count)
        table += f"\nSelf-reply rate: {round(self_replies*100/total_count, 2)}"

        return f"{table}"

    def get_count_user_pairs(self):
        """Iterator which yields (posts, user) tuple"""
        self.c.execute('''SELECT COUNT(*) AS amount, CASE master IS NULL WHEN TRUE THEN sendername ELSE master END AS name FROM messages LEFT JOIN aliases ON normname=normalias WHERE room=? GROUP BY name ORDER BY amount DESC''', (self.use_logs, ))
        while True:
            result = self.c.fetchone()
            yield result

    def get_message(self):
        """Gets messages from heim"""
        self.conn.commit()
        message = self.heimdall.parse()

        if message == "Killed":
            raise KillError

        return (message)

    def parse_options(self, options_list):
        """
        Parses options for running stats from the passed list

        >>> h = Heimdall('test')
        >>> h.parse_options(['--aliases','--messages','--engagement','--text'])
        ['aliases', 'messages', 'engagement', 'text']
        >>> h.parse_options(['--messages','--engagement','--text', '--aliases'])
        ['messages', 'engagement', 'text', 'aliases']
        >>> h.parse_options(['--text'])
        ['text']
        >>> h.parse_options(['--aliases','--messages','--engagement','--test'])
        ['aliases', 'messages', 'engagement']
        """
        options = []
        for arg in options_list:
            if arg in ['-a', '--aliases']:
                options.append('aliases')
            if arg in ['-m', '--messages']:
                options.append('messages')
            elif arg in ['-e', '--engagement']:
                options.append('engagement')
            elif arg in ['-t', '--text']:
                options.append('text')

        return options

    def parse(self, message):
        if message.type == 'send-event' or message.type == 'send-reply':
            self.insert_message(message)
            self.total_messages_all_time += 1
            if self.total_messages_all_time % 25000 == 0:
                self.heimdall.reply("Congratulations on making the {}th post in &{}!".format(self.total_messages_all_time, self.room))

            if message.type == 'send-reply' or len(message.data.content.split()) == 0:
                return

            comm = message.data.content.split()

            if len(comm) > 0 and len(comm[0][0]) > 0 and comm[0][0] == "!":
                if comm[0] == "!stats":
                    options = self.parse_options(comm[1:])

                    if len(options) == 0:
                        options = ['messages', 'engagement', 'text']

                    if len(comm) > 1 and comm[1][0] == "@":
                        self.heimdall.reply(self.get_user_stats(comm[1][1:], options))
                    elif len(comm) == 1 or comm[1].startswith('-'):
                        self.heimdall.reply(self.get_user_stats(message.data.sender.name, options))
                    else:
                        self.heimdall.reply("Sorry, I didn't understand that. Syntax is !stats (options) or !stats @user (options)")

                elif comm[0] == "!roomstats":
                    if len(comm) > 1:
                        self.heimdall.reply("Sorry, only stats for the current room are supported.")
                    else:
                        self.heimdall.reply(self.get_room_stats())

                elif comm[0] == "!rank":
                    if len(comm) > 1 and comm[1][0] == "@":
                        self.heimdall.reply(self.get_rank_of_user(comm[1][1:]))
                    elif len(comm) > 1:
                        try:
                            pos = int(comm[1])
                            self.heimdall.reply(self.get_user_at_position(pos))
                        except ValueError:
                            self.heimdall.reply("Sorry, no name or number detected. Syntax is !rank (@user|<number>)")
                    else:
                        self.heimdall.reply(self.get_rank_of_user(message.data.sender.name))

                elif comm[0] == "!alias":
                    while True:
                        msg = self.heimdall.parse()
                        if msg.type == 'send-event' and msg.data.sender.name == "TellBot" and 'bot:' in msg.data.sender.id and msg.data.content.split()[0] == "Aliases":
                            break

                    if '\n' in msg.data.content:
                        nicks = msg.data.content.split('\n')[1].split()[5:]
                    else:
                        nicks = [nick for nick in msg.data.content.split()[4:]]

                    if nicks[-2] == "and":
                        nicks.pop(len(nicks)-2)

                    nicks = [nick[:-1] for nick in nicks[:-1]] + [nicks[-1]]

                    if nicks[0] == "you":
                        del nicks[0]
                        nicks.append(message.data.sender.name)

                    master_nick = None

                    for nick in nicks:
                        self.c.execute('''SELECT COUNT(*) FROM aliases WHERE master IS ?''', (nick, ))
                        if self.c.fetchall()[0] != 0:
                            master_nick = nick
                            break

                    if master_nick is None:
                        master_nick = nicks[0]

                    for nick in nicks:
                        try:
                            normnick = self.heimdall.normalise_nick(nick)
                            self.write_to_database('''INSERT INTO aliases VALUES(?, ?, ?)''', values=(master_nick, nick, normnick))
                        except sqlite3.IntegrityError:
                            pass

                elif comm[0] == "!master":
                    if len(comm) == 3 and comm[1].startswith('@') and comm[2].startswith('@'):
                        user = comm[1][1:]
                        old_master = self.get_master_nick_of_user(user)
                        new_master = comm[2][1:]
                        aliases = self.get_aliases(old_master)
                        if new_master in aliases:
                            self.c.execute('DELETE FROM aliases WHERE master=?', (old_master,))
                            new_aliases = [(new_master, nick, self.heimdall.normalise_nick(nick),) for nick in aliases]
                            self.write_to_database('''INSERT INTO aliases VALUES(?, ?, ?)''', values=new_aliases, mode='executemany')
                            self.heimdall.reply('Remastered @th\'s aliases to @totallyhuman')
                        else:
                            self.heimdall.reply("New master not found in user's aliases")
                    else:
                        self.heimdall.reply("Syntax is !master @alias @newmaster")

    def main(self):
        """Main loop"""

        try:
            self.heimdall.connect()
            self.connect_to_database()
            while True:
                self.parse(self.get_message())
        except KeyboardInterrupt:
            sys.exit(0)
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
        except KeyboardInterrupt:
            sys.exit(0)
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
