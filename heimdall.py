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
import signal
import argparse
import re
import urllib.request

#Used for getting page titles
url_regex = re.compile(r"""(?i)\b((?:[a-z][\w-]+:(?:/{1,3}|[a-z0-9%])|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>\[\]]+|\(([^\s()<>\[\]]+|(\([^\s()<>\[\]]+\)))*\))+(?:\(([^\s()<>\[\]]+|(\([^\s()<>\[\]]+\)))*\)|[^\s`!(){};:'".,<>?\[\]]))""")


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
    while True:
        try:
            c.execute(
                '''INSERT OR FAIL INTO {} VALUES(?, ?, ?, ?, ?, ?, ?)'''.format(dbName), data)
            conn.commit()
            break
        except sqlite3.OperationalError:
            time.sleep(5)

def updateCount(name, c):
    try:
        c.execute('''INSERT OR FAIL INTO {}posters VALUES(?,?)'''.format(room), (name, 1,))
    except:
        c.execute('''SELECT * FROM {}posters WHERE name is ?'''.format(room), (name,))
        newCount = c.fetchone()[1] + 1
        c.execute('''UPDATE {}posters SET count = ? WHERE name = ?'''.format(room), (newCount, name,))

#Catches URLs
def getUrls(m):
    global urls
    urls.append(m.group(0))

# Handles SIGINTs
def onSIGINT(signum, frame):
    global conn
    conn.close()
    sys.exit(0)

signal.signal(signal.SIGINT, onSIGINT)

parser = argparse.ArgumentParser()
parser.add_argument("room")
parser.add_argument("--stealth", help="If enabled, bot will not present on nicklist", action="store_true")
args = parser.parse_args()

# Get logs
room = args.room

heimdall = karelia.newBot('Heimdall', room)
heimdall.connect(True)

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
    c.execute('''CREATE UNIQUE INDEX messageID ON {}(id)'''.format(room))
except:
    pass
try:
    c.execute('''CREATE TABLE {}posters(
                    name text,
                    count real
                )'''.format(room))
    c.execute('''CREATE UNIQUE INDEX name ON {}posters(name)'''.format(room))
    conn.commit()
except:
    pass

# Start pulling logs
heimdall.send({'type': 'log', 'data': {'n': 1000}})
names = set()

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
                names.add(message['sender']['name'])
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
            try:
                insertMessage(reply, room, conn, c)
            except sqlite3.IntegrityError:
                print("Failed: Message '{}' already exists in DB".format(
                    reply['data']['content']))
    except UpdateDone:
        break

for name in names:
    c.execute('''SELECT COUNT(*) FROM {} WHERE sendername IS ?'''.format(room), (name,))
    count = c.fetchone()[0]
    try:
        c.execute('''INSERT OR FAIL INTO {}posters VALUES(?,?)'''.format(room), (name, count,))
    except:
        c.execute('''UPDATE {}posters SET count = ? WHERE name = ?'''.format(room), (count, name,))


conn.commit()
conn.close()

print('Ready')
heimdall.disconnect()
heimdall.connect(args.stealth)

while True:
    try:
        conn = sqlite3.connect('logs.db')
        c = conn.cursor()
        while True:
            message = heimdall.parse()
            # If the message is worth storing, we'll store it
            if message['type'] == 'send-event':
                insertMessage(message, room, conn, c)
                updateCount(message['data']['sender']['name'], c)

                #Check if the message has URLs
                urls = []
                titles = []
                url_regex.sub(getUrls, message['data']['content'])
                if len(urls) > 0:
                    response = ""
                    for match in urls:
                        try:
                            url = 'http://' + match if not '://' in match else match
                            title = str(urllib.request.urlopen(url).read()).split('<title>')[1].split('</title>')[0]
                            response += "Title: {} \n".format(title)
                        except: pass
                    heimdall.send(response, message['data']['id'])

                # If it's asking for stats... well, let's give them stats.
                if message['data']['content'][0:6] == '!stats':
                    if '@' in message['data']['content']:
                        statsOf = message['data']['content'].split('@')[1].split(' ')[0]
                    else:
                        statsOf = message['data']['sender']['name']

                    # First off, we'll get a known-good version of the requester name
                    normnick = heimdall.normaliseNick(statsOf)

                    # Query gets the number of messages sent
                    c.execute('''SELECT count(*) FROM {} WHERE normname is ?'''.format(room), (normnick,))
                    count = c.fetchone()[0]

                    if count == 0:
                        heimdall.send('User @{} not found.'.format(statsOf), message['data']['id'])
                        continue

                    # Query gets the earliest message sent
                    c.execute('''SELECT * FROM {} WHERE normname IS ? ORDER BY time ASC'''.format(room), (normnick,))
                    earliest = c.fetchone()

                    # Query gets the most recent message sent
                    c.execute('''SELECT * FROM {} WHERE normname IS ? ORDER BY time DESC'''.format(room), (normnick,))
                    latest = c.fetchone()

                    # Calculate when the first message was sen, when the most recent message was sent, and the averate messages per day.
                    firstMessageSent = datetime.utcfromtimestamp(earliest[6]).strftime("%Y-%m-%d")
                    lastMessageSent = datetime.utcfromtimestamp(latest[6]).strftime("%Y-%m-%d")
                    numberOfDays = (datetime.strptime(lastMessageSent, "%Y-%m-%d") - datetime.strptime(firstMessageSent, "%Y-%m-%d")).days
                    if lastMessageSent == datetime.utcfromtimestamp(time.time()).strftime("%Y-%m-%d"): lastMessageSent = "today"
                    numberOfDays = numberOfDays if numberOfDays > 0 else 1

                    # Collate and send the lot.
                    heimdall.send('User {} has sent {} messages under that nick in the history of the room, between {} days ago on {} ("{}") and {}, averaging {} messages per day.'.format(
                        statsOf, str(count), numberOfDays, firstMessageSent, earliest[0], lastMessageSent, int(count / numberOfDays)), message['data']['id'])

                # If it's roomstats they want, well, let's get cracking!
                elif message['data']['content'] == '!roomstats':
                    # Calculate all posts ever
                    c.execute('''SELECT count(*) FROM {}'''.format(room))
                    count = c.fetchone()[0]

                    # Calculate top ten posters of all time
                    c.execute('''SELECT * FROM {}posters ORDER BY count DESC LIMIT 10'''.format(room))
                    results = c.fetchall()
                    topTen = ""
                    for i, result in enumerate(results):
                        topTen += "{:2d}) {:<7} - {}\n".format(i+1, int(result[1]), result[0])

                    # Get requester's position.
                    c.execute('''SELECT * FROM {}posters ORDER BY count DESC'''.format(room))
                    results = c.fetchall()
                    for i, result in enumerate(results):
                        if result[0] == message['data']['sender']['name']:
                            position = i + 1
                            break

                    heimdall.send("There have been {} posts in &{}.\n\nThe top ten posters are:\n{}\nYou are at position {} of {}.".format(count, room, topTen, position, len(results)), message['data']['id'])

    except sqlite3.IntegrityError:
        conn.close()
    except Exception:
        heimdall.log(Exception)
        conn.close()

conn.close()
