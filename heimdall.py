"""
Heimdall is a monitoring, logging, and statistics generating bot.
Currently on version 1.1.

Heimdall will eventually have the ability to spread across multiple rooms.
The goal is that as well as being able to monitor euphoria.io and provide
accurate logs and statistics on request for the purposes of archiving and
curiosity, it should be able to track the movements of spammers and other
known-problematic individuals.

As of the time of writing, Heimdall achieves the following capabilities:
- `!stats` returns the number of posts made under that nick
"""


import sys
sys.path.append('/home/struan/python/karelia/')

import karelia
from datetime import datetime
import json
import sqlite3
import pprint
import time


class UpdateDone (Exception):
    """Exception meaning that logs are up to date"""
    pass


class KillError (Exception):
    """Exception for when the bot is killed."""
    pass


# Inserts a single message into the database. Takes the message,
# the database name, and a pointer to the connection and cursor
def insertMessage(message, dbName, conn, c):
    """Inserts a new message into the database of messages"""
    if 'data' in message:
        if not 'parent' in message['data']:
            message['data']['parent'] = ''
        data = (message['data']['content'], message['data']['id'], message['data']['parent'],
                message['data']['sender']['id'], message['data']['sender']['name'], heimdall.normaliseNick(
                    message['data']['sender']['name']),
                message['data']['time'])
    else:
        if not 'parent' in message:
            message['parent'] = ''
        data = (message['content'], message['id'], message['parent'],
                message['sender']['id'], message['sender']['name'], heimdall.normaliseNick(
                    message['sender']['name']),
                message['time'])

    c.execute(
        '''INSERT OR FAIL INTO {} VALUES(?, ?, ?, ?, ?, ?, ?)'''.format(dbName), data)
    conn.commit()


# Get logs
room = 'xkcd'

heimdall = karelia.newBot('Heimdall', room)
heimdall.connect(False)

conn = sqlite3.connect('logs.db')
c = conn.cursor()

# Create the table if it doesn't already exist
try:
    c.execute('''CREATE TABLE {}(
                    content text,
                    id text,
                    parent text,
                    senderid text,
                    sendername text,
                    normname text,
                    time real
                 )'''.format(room))
    conn.commit()
    c.execute('''CREATE UNIQUE INDEX messageID ON {}(id)'''.format(room))
    conn.commit()
except:
    pass

# Start pulling logs
heimdall.send({'type': 'log', 'data': {'n': 1000}})
while True:
    try:
        while True:
            reply = heimdall.parse()
            # Only progress if we receive something worth storing
            if reply['type'] == 'log-reply' or reply['type'] == 'send-event':
                data = []
                break

        # Logs and single messages are structured differently.
        if reply['type'] == 'log-reply':
            # Helps keep track of where we are
            disp = reply['data']['log'][0]
            print('({})[{}] {}'.format(datetime.utcfromtimestamp(disp['time']).strftime("%Y-%m-%d %H:%M"),
                                       disp['sender']['name'].translate(heimdall.non_bmp_map), disp['content'].translate(heimdall.non_bmp_map)))

            # Append the data in this message to the data list ready for executemany
            for message in reply['data']['log']:
                if not 'parent' in message:
                    message['parent'] = ''
                data.append((message['content'], message['id'], message['parent'],
                             message['sender']['id'], message['sender']['name'], heimdall.normaliseNick(
                            message['sender']['name']),
                    message['time']))

            # Attempts to insert all the messages in bulk. If it fails, it will
            # break out of the loop and we will assume that the logs are now
            # up to date.
            try:
                c.executemany(
                    '''INSERT OR FAIL INTO {} VALUES(?, ?, ?, ?, ?, ?, ?)'''.format(room), data)
                conn.commit()
            except sqlite3.IntegrityError:
                raise UpdateDone

            # Likewise, if we get fewer messages than we requested, that's
            # because we've reached the end of the room's history
            if len(reply['data']['log']) != 1000:
                raise UpdateDone

            # Otherwise, we'll send a request for more logs.
            else:
                heimdall.send({'type': 'log', 'data': {'n': 1000,
                                                       'before': reply['data']['log'][0]['id']}})

        # If it's just a single message, we can use the insertMessage function
        else:
            try:
                insertMessage(reply, room, conn, c)
            except sqlite3.IntegrityError:
                print("Failed: Message '{}' already exists in DB".format(
                    reply['data']['content']))
    except UpdateDone:
        break

conn.commit()
conn.close()

print('Ready')

while True:
    try:
        conn = sqlite3.connect('logs.db')
        c = conn.cursor()
        while True:
            message = heimdall.parse()
            # If the message is worth storing, we'll store it
            if message['type'] == 'send-event':
                insertMessage(message, room, conn, c)

                # If it's asking for stats... well, let's give them stats.
                if message['data']['content'][0:6] == '!stats':
                    if '@' in message['data']['content']:
                        statsOf = message['data']['content'].split('@')[1].split(' ')[0]
                    else:
                        statsOf = message['data']['sender']['name']

                    # First off, we'll get a known-good version of the requester name
                    normnick = heimdall.normaliseNick(statsOf)

                    # Query gets the number of messages sent
                    c.execute(
                        '''SELECT count(*) FROM {} WHERE normname is "{}"'''.format(
                            room, normnick))
                    count = c.fetchone()[0]

                    # Query gets the earliest message sent
                    c.execute(
                        '''SELECT * FROM {} WHERE normname IS "{}" ORDER BY time ASC'''.format(room, normnick))
                    earliest = c.fetchone()

                    # Calculate when the first message was sent and the averate messages per day.
                    firstMessageSent = datetime.utcfromtimestamp(earliest[6]).strftime("%Y-%m-%d")
                    currentTime = datetime.utcfromtimestamp(time.time()).strftime("%Y-%m-%d")
                    numberOfDays = (datetime.strptime(currentTime, "%Y-%m-%d") - datetime.strptime(firstMessageSent, "%Y-%m-%d")).days

                    # Collate and send the lot.
                    heimdall.send('User {} has sent {} messages under that current nick in the history of the room, beginning {} days ago on {} ("{}") and averaging {} messages per day.'.format(
                        statsOf, str(count), numberOfDays, firstMessageSent, earliest[0], int(count / numberOfDays)), message['data']['id'])

    except sqlite3.IntegrityError:
        conn.close()

conn.close()
