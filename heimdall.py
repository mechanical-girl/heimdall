"""
Heimdall is a monitoring, logging, and statistics generating bot.
Currently on version 1.1.
[![Code Climate](https://codeclimate.com/github/PouncySilverkitten/heimdall/badges/gpa.svg)](https://codeclimate.com/github/PouncySilverkitten/heimdall)

[![build status badge](https://travis-ci.org/PouncySilverkitten/heimdall.svg?branch=master)](https://travis-ci.org/PouncySilverkitten/heimdall) with [![Test Coverage](https://codeclimate.com/github/PouncySilverkitten/heimdall/badges/coverage.svg)](https://codeclimate.com/github/PouncySilverkitten/heimdall/coverage)


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
    if not 'parent' in message:
        message['parent'] = ''
    if 'data' in message:
        data = (message['data']['content'], message['data']['id'], message['data']['parent'],
                message['data']['sender']['id'], message['data']['sender']['name'], heimdall.normaliseNick(
                    message['data']['sender']['name']),
                message['data']['time'])
    else:
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
    c.execute('''CREATE TABLE {} (
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
            insertMessage(message, room, conn, c)

    except UpdateDone:
        break

conn.commit()
conn.close

conn = sqlite3.connect('logs.db')
c = conn.cursor()
print('Ready')

while True:
    try:
        while True:
            message = heimdall.parse()
            # If the message is worth storing, we'll store it
            if message['type'] == 'send-event':
                insertMessage(message, room, conn, c)

                # If it's asking for stats... well, let's give them stats.
                if message['data']['content'] == '!stats':
                    c.execute(
                        '''SELECT count(*) FROM {} WHERE normname is "{}"'''.format(
                            room, heimdall.normaliseNick(message['data']['sender']['name'])))
                    heimdall.send("You have sent {} messages under your current nick in the history of the room.".format(
                        str(c.fetchone()[0])), message['data']['id'])
    except KillError:
        break

conn.close()
