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
import logging
import multiprocessing as mp
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
from logging import DEBUG, FileHandler
from typing import Dict, List, Tuple, Union

import karelia
import matplotlib.pyplot as plt
from websocket._exceptions import WebSocketConnectionClosedException

import loki
import pyimgur

test_funcs = []
prod_funcs = []


def test(func):
    test_funcs.append(func)
    return func


def prod(func):
    prod_funcs.append(func)
    return func
    signal.signal(signal.SIGINT, on_sigint)


class UpdateDone(Exception):
    """Exception meaning that logs are up to date"""
    pass


class UnknownMode(Exception):
    """Heimdall.write_to_database() has received an unknown mode"""
    pass


class KillError(Exception):
    """Exception for when the bot is killed."""
    pass

class DebugFileHandler(FileHandler):
    def __init__(self, filename, mode='a', encoding=None, delay=False):
        FileHandler.__init__(self, filename, mode, encoding, delay)

    def emit(self, record):
        if not record.levelno == DEBUG:
            return
        FileHandler.emit(self, record)


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

        log_format = logging.Formatter(f'\n\n--------------------\n%(asctime)s - &{self.room}: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        debug_handler = DebugFileHandler('Heimdall_debug.log')
        debug_handler.setFormatter(log_format)
        debug_handler.setLevel(logging.DEBUG)
        self.logger.addHandler(debug_handler)

        exc_handler = logging.FileHandler('Heimdall.log')
        exc_handler.setFormatter(log_format)
        exc_handler.setLevel(logging.WARN)
        self.logger.addHandler(exc_handler)

        self.logger.debug('Logging configured successfully')

        self.stealth = kwargs['stealth'] if 'stealth' in kwargs else False
        self.verbose = kwargs['verbose'] if 'verbose' in kwargs else False
        self.force_new_logs = kwargs['new_logs'] if 'new_logs' in kwargs else False
        self.use_logs = kwargs['use_logs'] if 'use_logs' in kwargs else self.room
        self.test_funcs = test_funcs
        self.prod_funcs = prod_funcs
        self.dcal = kwargs['disconnect_after_log'] if 'disconnect_after_log' in kwargs else False

        self.logger.debug('Flags handled successfully')

        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.database = os.path.join(BASE_DIR, "_heimdall.db")
        if os.path.basename(os.path.dirname(os.path.realpath(__file__))) == "prod-yggdrasil" or ('force_prod' in kwargs and kwargs['force_prod']):
            self.prod_env = True
        else:
            self.prod_env = False

        self.logger.debug(f'Running in {"production" if self.prod_env else "test"} environment')

        self.heimdall = karelia.bot('Heimdall', self.room)
        self.heimdall.on_kill = sys.exit

        if self.prod_env:
            self.loki = loki.Loki(self.heimdall.normalise_nick, self.database, False, self.queue)
        else:
            self.loki = loki.Loki(self.heimdall.normalise_nick, self.database, True)

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
            except:
                self.logger.exception("Error creating help text.")
                self.show(f"Error creating help text - see 'Heimdall &{self.room}.log' for details.")

        with open(self.files['imgur'], 'r') as f:
            self.show("Reading imgur key, creating Imgur client...", end=' ')
            try:
                self.imgur_key: str = json.loads(f.read())[0]
                self.imgur_client = pyimgur.Imgur(self.imgur_key)
                self.show("done")
            except:
                self.logger.exception("Failed to create imgur client.")
                self.show(f"Error reading imgur key - see 'Heimdall &{self.room}.log' for details.")

        self.show("Connecting to database...", end=' ')
        self.connect_to_database()
        if self.force_new_logs:
            self.show("done\nDeleting messages...", end=' ')
            self.write_to_database('''DELETE FROM messages WHERE room IS ?''', values=(self.room, ))
            self.show("done\nCreating tables...", end=' ')
        self.check_or_create_tables()
        self.show("done")

        self.heimdall.connect(True)
        self.show("Getting logs...", end=' ')
        self.get_room_logs()
        self.show("Done.")

        try:
            self.c.execute('''SELECT COUNT(*) FROM messages WHERE room IS ?''', (self.room, ))
            self.total_messages_all_time = self.c.fetchone()[0]
        except:
            self.total_messages_all_time = 0
            self.logger.warning("Apparently no messages in the room.")

        self.conn.close()
        self.heimdall.disconnect()
        self.show("Ready")

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

        """

        override = True if 'override' in kwargs and kwargs['override'] else False
        end = kwargs['end'] if 'end' in kwargs else '\n'
        if self.verbose or override:
            print(*args, end=end)
        self.logger.debug(*args)

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
                        self.show("Log update done; empty message received.")
                        raise UpdateDone
                    elif len(reply.data.log) < 1000:
                        self.show("Log update done; less than 1000 messages received.")
                        update_done = True
                    else:
                        self.heimdall.send({'type': 'log', 'data': {'n': 1000, 'before': reply.data.log[0]['id']}})

                        disp = reply.data.log[0]

                        safe_content = disp['content'].split('\n')[0][0:80].translate(self.heimdall.non_bmp_map)
                        self.show(f"    ({datetime.utcfromtimestamp(disp['time']).strftime('%Y-%m-%d %H:%M')} in &{self.room})[{disp['sender']['name'].translate(self.heimdall.non_bmp_map)}] {safe_content}")
                    # Append the data in this message to the data list ready for executemany
                    for message in reply.data.log:
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
                    self.c.execute('''SELECT COUNT(*) FROM messages WHERE globalid=?''', (data[0][8],))
                    if self.c.fetchone()[0] == 1:
                        self.show("Log update done; most recent message in ther DB has been reached.")
                        raise UpdateDone
                    self.write_to_database('''INSERT OR FAIL INTO messages VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)''', values=data, mode="executemany")

                    if update_done:
                        raise UpdateDone

                else:
                    self.insert_message(reply)

            except UpdateDone:
                break
            except sqlite3.IntegrityError:
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
        pairs = self.get_count_user_pairs(self.use_logs)
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

    def get_user_at_position(self, position, room_requested):
        """Returns the user at the specified position"""

        # Check to see they've passed a number
        try:
            position = int(position)
            assert position != 0
        except:
            return "The position you specified was invalid."

        self.c.execute('''SELECT COUNT(*) FROM (SELECT COUNT(*) AS amount, CASE master IS NULL WHEN TRUE THEN sendername ELSE master END AS name FROM messages LEFT JOIN aliases ON normname=normalias WHERE room=? GROUP BY name ORDER BY amount DESC)''', (room_requested, ))
        total_posters = self.c.fetchall()[0][0]
        if position > total_posters:
            return f"Position not found; there have been {total_posters} posters in &{self.use_logs}."

        pairs = self.get_count_user_pairs(room_requested)
        for i in range(position):
            name = "".join(next(pairs)[1].split())

        return f"The user at position {position} is @{name}."

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
        if self.prod_env:
            try:
                url = self.imgur_client.upload_image(filename).link
            except:
                self.logger.exception("Imgur upload failed")
                url = "Imgur upload failed, sorry."
        else:
            url = "fake_url_here"

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

    @prod
    def get_user_stats(self):
        """Retrieves, formats and sends stats for user"""
        # First off, we'll get a known-good version of the requester name

        comm = self.heimdall.packet.data.content.split()

        if comm[0] != "!stats":
            return

        self.logger.debug(f'Got a stats request from "{self.heimdall.packet.data.sender.name}"')
        options = []
        user = self.heimdall.packet.data.sender.name

        if len(comm) > 1:
            options = self.parse_options(comm[1:])
            if comm[1].startswith("@"):
                user = self.heimdall.normalise_nick(comm[1][1:])
            elif options == [] or ('@' in [s[0] for s in comm] and not comm[1].startswith("@")):
                self.heimdall.reply("Sorry, I didn't understand that. Syntax is !stats (options) or !stats @user (options)")
                return

        if options == []:
            options = ['messages', 'engagement', 'text']

        normnick = self.heimdall.normalise_nick(user)

        if 'aliases' in options:
            aliases = [self.heimdall.normalise_nick(nick) for nick in self.loki.get_aliases(user)]

            if not aliases:
                self.logger.debug('Aliases requested and ignored')
                aliases_used = f"--aliases was ignored, since no aliases for user {user} are known. To correct, please post `!alias @{user.replace(' ','')}` in any room where @Heimdall is present."
                aliases = [normnick]
            else:
                self.logger.debug('Using aliases')
                aliases_used = f"{len(aliases)-1} aliases used."

        else:
            self.logger.debug('Aliases not requested')
            aliases_used = "No aliases used."
            aliases = [normnick]

        # Query gets the number of messages sent. `','.join(['?']*len(aliases))` is used so that there are enough question marks for the number of aliases
        self.c.execute(f'''SELECT count(*) FROM messages WHERE room IS ? AND normname IN ({', '.join(['?']*len(aliases))})''', (self.use_logs, *aliases,))
        count = self.c.fetchone()[0]

        if count == 0:
            self.heimdall.reply('User @{} not found.'.format(user.replace(' ', '')))
            return

        if options == ['aliases']:
            self.heimdall.reply("No options specified. Please only use --aliases or -a in conjunction with --messages (or -m), --engagement (-e), --text (-t), or a combination thereof.")

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
                last_message_sent = f"{days_since_last_message} days ago, on {last_message_sent}"

            try:
                avg_messages_per_day = int(count/number_of_days)
            except ZeroDivisionError:
                avg_messages_per_day = 0

            days = sorted(days.items())

            last_28_days = days[-28:]

            title = "Messages by {}, last 28 days".format(user)
            data_x = [day[0] for day in last_28_days]
            data_y = [day[1] for day in last_28_days]
            if not self.prod_env:
                last_28_url = "url_goes_here"
            else:
                last_28_graph = self.graph_data(data_x, data_y, title)
                last_28_file = self.save_graph(last_28_graph)
                last_28_url = self.upload_and_delete_graph(last_28_file)

            title = "Messages by {}, all time".format(user)
            data_x = [day[0] for day in days]
            data_y = [day[1] for day in days]
            if not self.prod_env:
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
        self.logger.debug('Sending results')
        self.heimdall.reply(f"""{message_results}{engagement_results}{text_results}{aliases_used}""")

    @test
    def run_queries(self):
        content = self.heimdall.packet.data.content
        if not content.split()[0] in ['!query', '!query-concat'] or self.room != "test":
            return

        split_cont = content.split('!')
        sender = ""

        # Check for criteria
        for cont in split_cont:
            if cont.startswith('query'):
                query_list = cont.split()
            elif cont.startswith('sender'):
                sender = self.heimdall.normalise_nick(cont.split()[1])

        # Check for query types
        if query_list[0] == 'query':
            keywords = [' '.join(query_list[1:])]
        elif query_list[0] == 'query-concat':
            keywords = query_list[1:]

        core = f'''SELECT * FROM messages WHERE room="{self.use_logs}" ORDER BY time ASC'''
        if sender != "":
            query = f'''SELECT content, sendername, normname, time FROM ({core}) WHERE normname="{sender}" ORDER BY time ASC'''
        else:
            query = core

        query = f'''SELECT content, sendername, normname, time FROM ({query}) WHERE content LIKE "%{keywords[0]}%" ORDER BY time ASC'''
        if query.startswith("query-concat ") and len(keywords) > 1:
            for keyword in keywords[1:]:
                query = f'''SELECT content, sendername, normname, time FROM ({query}) WHERE content LIKE "% {keyword} %" ORDER BY time ASC'''

        query = f'''{query} LIMIT 100;'''
        self.c.execute(query)
        results = self.c.fetchall()
        if len(results) == 0:
            self.heimdall.reply("No messages found")
        send = ""
        for result in results:
            send += f"{result[1]}: {result[0]}\n"
        self.heimdall.reply(send)

    @test
    def get_rank(self):
        """Gets and sends the rank of the requested user, or the user at the requested rank"""

        comm = self.heimdall.packet.data.content.split()
        if comm[0] != "!rank":
            return

        if len(comm) > 1 and comm[1][0] == "@":
            # If a user is specified, meaning that comm[1] should have an @ and then a name

            user = comm[1][1:]

            rank = self.get_position(user)
            if rank is None:
                self.heimdall.reply(f"User @{user} not found.")
            else:
                self.heimdall.reply(f"Position {rank}")

        elif len(comm) > 1:
            # Assume that the request is for the user at a certain rank, meaning that comm[1] should be a number
            try:
                pos = int(comm[1])
                self.heimdall.reply(self.get_user_at_position(pos, self.use_logs))
            except ValueError:
                self.heimdall.reply("Sorry, no name or number detected. Syntax is !rank (@user|<number>)")

        else:
            # No parameters offered; we assume that the user wants their own rank
            self.heimdall.reply(f"Position {self.get_position(self.heimdall.packet.data.sender.name)}")

    @prod
    def get_room_stats(self):
        """Gets and sends stats for rooms"""
        try:
            comm = self.heimdall.packet.data.content.split()

            if comm[0] != "!roomstats":
                return

            self.logger.debug(f"Got a roomstats request from {self.heimdall.packet.data.sender.name}")
            if len(comm) == 2 and comm[1].startswith('&'):
                self.c.execute('''SELECT COUNT(*) FROM messages WHERE room IS ?''', (comm[1][1:], ))
                count = self.c.fetchone()[0]
                if count == 0:
                    self.heimdall.reply("I do not operate in that room.")
                    self.logger.debug("Requested a room not logged")
                    return
                else:
                    room_requested = comm[1][1:]

            elif len(comm) == 1:
                room_requested = self.use_logs
                self.c.execute('''SELECT count(*) FROM messages WHERE room IS ?''', (self.use_logs, ))
                count = self.c.fetchone()[0]

            # Calculate top ten posters of all time
            top_ten = ""
            posters = self.get_count_user_pairs(room_requested)
            i = 0
            for pair in posters:
                i += 1
                top_ten += "{:2d}) {:<7}\t{}\n".format(i, int(pair[0]), pair[1])
                if i == 10:
                    break

            self.c.execute('''SELECT COUNT(*) FROM (SELECT COUNT(*) AS amount, CASE master IS NULL WHEN TRUE THEN sendername ELSE master END AS name FROM messages LEFT JOIN aliases ON normname=normalias WHERE room=? GROUP BY name ORDER BY amount DESC)''', (room_requested, ))
            total_posters = self.c.fetchall()[0][0]

            # Get activity over the last 28 days
            lower_bound = self.next_day(time.time()) - (60 * 60 * 24 * 28)
            self.c.execute('''SELECT time, COUNT(*) FROM messages WHERE room IS ? AND time > ? GROUP BY CAST(time / 86400 AS INT)''', (room_requested, lower_bound,))
            last_28_days = self.c.fetchall()

            days = last_28_days[:]
            last_28_days = []
            for day in days:
                last_28_days.append((self.next_day(day[0]) - 60 * 60 * 24, day[1],))

            per_day_last_four_weeks = int(sum([count[1] for count in last_28_days]) / 28)
            last_28_days.sort(key=operator.itemgetter(1))

            self.c.execute('''SELECT time, COUNT(*) FROM messages WHERE room IS ? GROUP BY CAST(time/86400 AS INT)''', (room_requested, ))
            messages_by_day = self.c.fetchall()

            title = "Messages in &{}, last 28 days".format(room_requested)
            data_x = [date.fromtimestamp(int(day[0])) for day in last_28_days]
            data_y = [day[1] for day in last_28_days]
            last_28_graph = self.graph_data(data_x, data_y, title)
            last_28_file = self.save_graph(last_28_graph)
            last_28_url = self.upload_and_delete_graph(last_28_file)

            title = "Messages in &{}, all time".format(room_requested)
            data_x = [date.fromtimestamp(int(day[0])) for day in messages_by_day]
            data_y = [day[1] for day in messages_by_day]
            all_time_graph = self.graph_data(data_x, data_y, title)
            all_time_file = self.save_graph(all_time_graph)
            all_time_url = self.upload_and_delete_graph(all_time_file)

            if last_28_days is None:
                self.heimdall.reply(f"There have been {count} posts in &{room_requested}, though none in the last 28 days.\n\nThe top ten posters are:\n{top_ten}\n{all_time_url}")
                return

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

            self.logger.debug("Request finished, sending now.")
            self.heimdall.reply(f"There have been {count} posts in &{room_requested} ({messages_today} today) from {total_posters} posters, averaging {per_day_last_four_weeks} posts per day over the last 28 days{busiest_last_28}.\n\nThe top ten posters are:\n{top_ten}\n{all_time_url} {last_28_url}")
        except:
            self.logger.exception(f"Exception on roomstats with message {json.dumps(self.heimdall.packet.packet)}")

    def get_user_engagement_table(self, user):
        """(self, user) -> table"""

        # Yes, this function is defined inside another function. That's the way I want it. Don't unindent it.
        def formatta(tup, total):
            return f"{'{:4.2f}'.format(round(tup[1]*100/total, 2))}\t\t{tup[0]}\n"

        normnick = self.heimdall.normalise_nick(user)

        aliases = [self.heimdall.normalise_nick(nick) for nick in self.loki.get_aliases(user)]
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

    def get_count_user_pairs(self, room_requested):
        """Iterator which yields (posts, user) tuple"""
        self.c.execute('''SELECT COUNT(*) AS amount, CASE master IS NULL WHEN TRUE THEN sendername ELSE master END AS name FROM messages LEFT JOIN aliases ON normname=normalias WHERE room=? GROUP BY name ORDER BY amount DESC''', (room_requested, ))
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
        >>> h.parse_options(['-meta'])
        ['aliases', 'messages', 'engagement', 'text']
        >>> h.parse_options(['-m'])
        ['messages']
        >>> h.parse_options(['-m', '--engagement'])
        ['messages', 'engagement']
        """
        options = []
        for arg in options_list:
            if arg in ['-a', '--aliases']:
                options.append('aliases')
            elif arg in ['-m', '--messages']:
                options.append('messages')
            elif arg in ['-e', '--engagement']:
                options.append('engagement')
            elif arg in ['-t', '--text']:
                options.append('text')
            elif arg.startswith('-') and not arg.startswith('--'):
                if 'a' in arg and 'aliases' not in options_list:
                    options.append('aliases')
                if 'm' in arg and 'messages' not in options_list:
                    options.append('messages')
                if 'e' in arg and 'engagement' not in options_list:
                    options.append('engagement')
                if 't' in arg and 'text' not in options_list:
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

            queries = self.loki.parse(message, self.room)
            if queries is not None:
                for query in queries:
                    try:
                        self.logger.debug(f"Running query {query[0]} with values {query[1]}")
                        self.write_to_database(query[0], values=query[1], mode="execute")
                    except sqlite3.IntegrityError:
                        pass

            comm = message.data.content.split()

            if len(comm) > 0 and len(comm[0]) > 0 and comm[0][0] == "!":
                self.logger.debug(f'Received message "{message.data.content}" from user "{message.data.sender.name}".')
                for func in self.prod_funcs:
                    try:
                        func(self)
                    except:
                        self.logger.exception(f"Exception on message {json.dumps(self.heimdall.packet.packet)}")

                if not self.prod_env:
                    for func in self.test_funcs:
                        func(self)

                if comm[0] == '!diag-dump':
                    self.heimdall.reply(f"prod-funcs: {self.prod_funcs}")
                    self.heimdall.reply(f"test-funcs: {self.test_funcs}")
                    self.heimdall.reply(f"prod-env: {self.prod_env}")

                elif comm[0] == "!master":
                    self.logger.debug("Master command received")
                    if len(comm) == 3 and comm[1].startswith('@') and comm[2].startswith('@'):
                        user = comm[1][1:]
                        old_master = self.get_master_nick_of_user(user)
                        new_master = comm[2][1:]
                        aliases = self.loki.get_aliases(old_master)
                        if self.heimdall.normalise_nick(new_master) in [self.heimdall.normalise_nick(alias) for alias in aliases]:
                            self.c.execute('DELETE FROM aliases WHERE master=?', (old_master,))
                            new_aliases = [(new_master, nick, self.heimdall.normalise_nick(nick),) for nick in aliases]
                            self.write_to_database('''INSERT INTO aliases VALUES(?, ?, ?)''', values=new_aliases, mode='executemany')
                            self.heimdall.reply(f'Remastered @{old_master} aliases to @{new_master}')
                        else:
                            self.heimdall.reply("New master not found in user's aliases")
                    else:
                        self.heimdall.reply("Syntax is !master @alias @newmaster")

                elif comm[0] == "!err":
                    self.heimdall.reply(1/0)

    def main(self):
        """Main loop"""

        self.heimdall.connect()
        self.connect_to_database()
        if self.dcal: sys.exit(0)
        while True:
            self.parse(self.get_message())


def on_sigint(signum, frame):
    sys.exit()


def main(room, **kwargs):
    stealth = kwargs['stealth'] if 'stealth' in kwargs else False
    new_logs = kwargs['new_logs'] if 'new_logs' in kwargs else False
    use_logs = kwargs['use_logs'] if 'use_logs' in kwargs and kwargs['use_logs'] is not None else room if type(room) is str else room[0]
    verbose = kwargs['verbose'] if 'verbose' in kwargs else 'False'
    force_prod = kwargs['force_prod'] if 'force_prod' in kwargs else 'False'

    heimdall = Heimdall(room, stealth=stealth, new_logs=new_logs, use_logs=use_logs, verbose=verbose, force_prod=force_prod)

    while True:
        try:
            heimdall.main()
        except KeyboardInterrupt:
            sys.exit(0)
        except KillError:
            heimdall.logger.exception("Heimdall was killed. Check karelia.log for more detail.")
            heimdall.conn.commit()
            heimdall.conn.close()
            heimdall.heimdall.disconnect()
            raise
        except TimeoutError:
            heimdall.logger.exception("Timeout from Heim.")
            heimdall.heimdall.disconnect()
        except:
            heimdall.logger.exception(f"Heimdall crashed on message {json.dumps(heimdall.heimdall.packet.packet)}")
            heimdall.conn.close()
        finally:
            heimdall.heimdall.disconnect()
            time.sleep(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("room", nargs='?')
    parser.add_argument("--stealth", help="If enabled, bot will not present on nicklist", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true", dest="verbose")
    parser.add_argument("--force-new-logs", help="If enabled, Heimdall will delete any current logs for the room", action="store_true", dest="new_logs")
    parser.add_argument("-p", "--force-prod", action="store_true", dest="force_prod")
    parser.add_argument("--use-logs", type=str, dest="use_logs")
    parser.add_argument("--dcal", action="store_true", dest="disconnect_after_log")
    args = parser.parse_args()

    room = args.room
    stealth = args.stealth
    new_logs = args.new_logs
    use_logs = args.use_logs
    verbose = args.verbose
    force_prod = args.force_prod
    disconnect_after_log = args.disconnect_after_log
    main(room, stealth=stealth, new_logs=new_logs, use_logs=use_logs, verbose=verbose, force_prod=force_prod, disconnect_after_log=disconnect_after_log)
