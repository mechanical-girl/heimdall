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

import karelia
from datetime import datetime, timedelta, date
from datetime import time as dttime
import json
import sqlite3
import pprint
import time
import signal
import argparse
import re
import urllib.request
import html
import codecs
import sys
import calendar
import matplotlib.pyplot as plt
import pyimgur
import operator
import random
import string
import os

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
                    message['data']['sender']['name']), message['data']['time'])
    else:
        if not 'parent' in message:
            message['parent'] = ''
        data = (message['content'].replace('&', '{ampersand}'), message['id'], message['parent'],
                message['sender']['id'], message['sender']['name'], heimdall.normaliseNick(
                    message['sender']['name']),
                message['time'])
    while True:
        try:
            c.execute('''INSERT OR FAIL INTO {} VALUES(?, ?, ?, ?, ?, ?, ?)'''.format(dbName), data)
            conn.commit()
            break
        except sqlite3.OperationalError:
            time.sleep(5)

def updateCount(name, conn, c):
    try:
        c.execute('''INSERT OR FAIL INTO {}posters VALUES(?,?,?)'''.format(room), (name, heimdall.normaliseNick(name), 1,))
    except:
        c.execute('''SELECT * FROM {}posters WHERE normname is ?'''.format(room), (heimdall.normaliseNick(name),))
        newCount = c.fetchone()[2] + 1
        c.execute('''UPDATE {}posters SET count = ?, name = ? WHERE normname = ?'''.format(room), (newCount, name, heimdall.normaliseNick(name),))
    conn.commit()

def nextDay(day):
    oneDay = 60*60*24
    return(int(calendar.timegm(date.fromtimestamp(day).timetuple())+oneDay))

#Catches URLs
def getUrls(m):
    global urls
    urls.append(m.group(0))

# Handles SIGINTs
def onSIGINT(signum, frame):
    global conn
    conn.close()
    sys.exit(0)


def getPosition(nick):
    c.execute('''SELECT normname, count FROM (SELECT normname, COUNT(*) as count FROM {} GROUP BY normname) ORDER BY count DESC'''.format(room))
    normnick = heimdall.normaliseNick(nick)
    position = 0
    while True:
        position += 1
        result = c.fetchone()
        if result[0] == normnick:
            return(position)
            
    return("unknown")

def graphData(dataX, dataY, title):
    # Plot and upload the messages, last 28 days graph 
    f, ax = plt.subplots(1)
    plt.title(title)
    ax.plot(dataX, dataY)
    plt.gcf().autofmt_xdate()
    ax.xaxis.set_major_locator(plt.MaxNLocator(10))
    ax.set_ylim(ymin=0)
    filename = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))+".png"
    f.savefig(filename)
    url = imgurClient.upload_image(filename).link
    os.remove(filename)
    return(url)
    
with open('imgur.json', 'r') as f:
    imgurClient = pyimgur.Imgur(json.loads(f.read())[0])

signal.signal(signal.SIGINT, onSIGINT)

parser = argparse.ArgumentParser()
parser.add_argument("room")
parser.add_argument("--stealth", help="If enabled, bot will not present on nicklist", action="store_true")
args = parser.parse_args()

# Get logs
room = args.room
messagesPerDay = {}

startTime = time.time()
print('Connecting to euphoria...', end='')

heimdall = karelia.newBot('Heimdall', room)
heimdall.connect(True)

print(' done in {} seconds\nConnecting to database...'.format(round(time.time()-startTime,2)),end='')

startTime = time.time()

conn = sqlite3.connect('{}.db'.format(room))
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
except: print(' existing tables found...', end='')
try:
    c.execute('''CREATE TABLE {}posters(
                    name text,
                    normname text,
                    count real
                )'''.format(room))
    c.execute('''CREATE UNIQUE INDEX normname ON {}posters(normname)'''.format(room))
    conn.commit()

except: pass
try:
    c.execute('''CREATE TABLE rooms(name text, password integer)''')
    c.execute('''CREATE UNIQUE INDEX name ON rooms(name)''')
    conn.commit()
except: pass

print(' done in {} seconds\nPulling logs'.format(round(time.time()-startTime,2)))

# Start pulling logs
startTime = time.time()

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
            if len(reply['data']['log']) == 0:
                raise UpdateDone

            disp = reply['data']['log'][0]
            print('    ({})[{}] {}'.format(datetime.utcfromtimestamp(disp['time']).strftime("%Y-%m-%d %H:%M"),
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
            try: insertMessage(reply, room, conn, c)
            except sqlite3.IntegrityError: pass
    except UpdateDone:
        break

print('Done in {} seconds\nAdding nicks to table...'.format(round(time.time()-startTime,2)), end='')
startTime = time.time()

for name in names:
    c.execute('''SELECT COUNT(*) FROM {} WHERE normname IS ?'''.format(room), (heimdall.normaliseNick(name),))
    count = c.fetchone()[0]
    try:
        c.execute('''INSERT OR FAIL INTO {}posters VALUES(?,?,?)'''.format(room), (name, heimdall.normaliseNick(name), count,))
    except:
        c.execute('''UPDATE {}posters SET count = ? WHERE normname = ?'''.format(room), (count, heimdall.normaliseNick(name),))

print(' done in {} seconds'.format(round(time.time()-startTime,2)))

conn.commit()
conn.close()

print('Ready')
heimdall.disconnect()

while True:
    try:
        heimdall.connect(args.stealth)
        conn = sqlite3.connect('{}.db'.format(room))
        c = conn.cursor()
        c.execute('''SELECT count(*) FROM {}'''.format(room))
        totalMessagesAllTime = c.fetchone()[0] 
        while True:
            conn.commit()
            message = heimdall.parse()
            # If the message is worth storing, we'll store it

            if message['type'] == 'send-event' or message['type'] == 'send-reply':
                insertMessage(message, room, conn, c)
                updateCount(message['data']['sender']['name'], conn, c)
                totalMessagesAllTime += 1

                #Check if the message is a historical one
                if totalMessagesAllTime%25000 == 0:
                    heimdall.send("Congratulations on making the {}th post in &{}!".format(totalMessagesAllTime,room), message['data']['id'])

                #Check if the message has URLs
                urls = []
                titles = []
                url_regex.sub(getUrls, message['data']['content'])
                for url in urls:
                    if "imgur.com/" in url: urls.remove(url)
                if len(urls) > 0:
                    response = ""
                    for match in urls:
                        try:
                            url = 'http://' + match if not '://' in match else match
                            title = str(urllib.request.urlopen(url).read()).split('<title>')[1].split('</title>')[0]
                            title = html.unescape(codecs.decode(title, 'unicode_escape')).strip()
                            clearTitle = title if len(title) <= 75 else '{}...'.format(title[0:72].strip())
                            response += "Title: {} \n".format(clearTitle)
                        except: pass
                    heimdall.send(response, message['data']['id'])

                # Check if the message mentions a room
                if '&' in message['data']['content']:
                    possibleRooms = [room[1:] for room in message['data']['content'].split(' ') if room[0] == '&']
                    for possibleRoom in possibleRooms:
                        spider = karelia.newBot('', possibleRoom)
                        try:
                            spider.connect(True)
                            event = spider.parse()['type']
                            if event == 'bounce-event':
                                c.execute('''INSERT OR FAIL INTO rooms VALUES(?,?)''', (possibleRoom, 1,))
                                conn.commit()
                            else:
                                c.execute('''INSERT OR FAIL INTO rooms VALUES(?,?)''', (possibleRoom, 0,))
                                conn.commit()
                            spider.disconnect()
                        except: pass
                
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
                    
                    # In the interest of finding the busiest day, let's do some quick conversion
                    timestamp = datetime.fromtimestamp(earliest[6])
                    days = {}
                    c.execute('''SELECT time, COUNT(*) FROM {} WHERE normname IS ? GROUP BY CAST(time / 86400 AS INT)'''.format(room), (normnick,))
                    dailyMessages = c.fetchall()
                    days = {}
                    for mess in dailyMessages:
                        day = datetime.utcfromtimestamp(mess[0]).strftime("%Y-%m-%d")
                        days[day] = mess[1]
                    
                    try: messagesToday = days[datetime.utcfromtimestamp(datetime.today().timestamp()).strftime("%Y-%m-%d")]
                    except: messagesToday = 0
                    daysByBusyness =  [(k, days[k]) for k in sorted(days, key=days.get, reverse = True)]  
                    busiestDay = daysByBusyness

                    # Calculate when the first message was sent, when the most recent message was sent, and the averate messages per day.
                    firstMessageSent = datetime.utcfromtimestamp(earliest[6]).strftime("%Y-%m-%d")
                    lastMessageSent = datetime.utcfromtimestamp(latest[6]).strftime("%Y-%m-%d")
                    numberOfDays = (datetime.strptime(lastMessageSent, "%Y-%m-%d") - datetime.strptime(firstMessageSent, "%Y-%m-%d")).days
                    if lastMessageSent == datetime.utcfromtimestamp(time.time()).strftime("%Y-%m-%d"): lastMessageSent = "Today"
                    numberOfDays = numberOfDays if numberOfDays > 0 else 1
                    
                    last28Days = sorted(days.items())[::-1][:28]
                    title = "Messages by {}, last 28 days".format(statsOf)
                    dataX = [day[0] for day in last28Days]
                    dataY = [day[1] for day in last28Days]
                    last28Url = graphData(dataX, dataY, title)

                    messagesAllTime = days.items()
                    title = "Messages by {}, all time".format(statsOf)
                    dataX = [day[0] for day in messagesAllTime]
                    dataY = [day[1] for day in messagesAllTime]
                    allTimeUrl = graphData(dataX, dataY, title)

                    # Get requester's position.
                    position = getPosition(normnick)
                    c.execute('''SELECT count(*) FROM {}posters'''.format(room))
                    noOfPosters = c.fetchone()[0]
                    # Collate and send the lot.
                    heimdall.send("""
User:\t\t\t\t\t{}
Messages:\t\t\t\t{}
Messages Sent Today:\t\t{}
First Message Date:\t\t{} days ago, on {}
First Message:\t\t\t{}
Most Recent Message:\t{}
Average Messages/Day:\t{}
Busiest Day:\t\t\t\t{}, with {} messages
Ranking:\t\t\t\t\t{} of {}.
{} {}""".format(statsOf, str(count), messagesToday, numberOfDays, firstMessageSent, earliest[0], lastMessageSent, int(count / numberOfDays), busiestDay[0][0], busiestDay[0][1], position, noOfPosters, allTimeUrl, last28Url), message['data']['id'])

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
                        topTen += "{:2d}) {:<7}\t{}\n".format(i+1, int(result[2]), result[0])

                    # Get activity over the last 28 days
                    lowerBound = datetime.now() + timedelta(-28)
                    lowerBound = time.mktime(lowerBound.timetuple())
                    c.execute('''SELECT time, COUNT(*) FROM {} WHERE time > ? GROUP BY CAST(time / 86400 AS INT)'''.format(room), (lowerBound,))
                    last28Days = c.fetchall()
                    perDayLastFourWeeks = int(sum([count[1] for count in last28Days])/28)
                    
                    last28Days.sort(key=operator.itemgetter(1))
                    busiest = (datetime.utcfromtimestamp(last28Days[-1][0]).strftime("%Y-%m-%d"),last28Days[-1][1])

                    last28Days.sort(key=operator.itemgetter(0))

                    # Get the number of messages today
                    midnight = time.mktime(datetime.combine(date.today(), dttime.min).timetuple())
                    for tup in last28Days:
                        if tup[0] > midnight:
                            messagesToday = tup[1]
                            break

                    c.execute('''SELECT time, COUNT(*) FROM {} GROUP BY CAST(time/86400 AS INT)'''.format(room))
                    messagesByDay = c.fetchall()

                    # Plot and upload the messages, last 28 days graph
                    title = "Messages in &{}, last 28 days".format(room)
                    dataX = [date.fromtimestamp(int(day[0])) for day in last28Days]
                    dataY = [day[1] for day in last28Days]
                    last28Url = graphData(dataX, dataY, title)

                    # Plot and upload the messages, all time graph
                    title = "Messages in &{}, all time".format(room)
                    dataX = [date.fromtimestamp(int(day[0])) for day in messagesByDay]
                    dataY = [day[1] for day in messagesByDay]
                    allTimeUrl = graphData(dataX, dataY, title)

                    heimdall.send("There have been {} posts in &{} ({} today), averaging {} posts per day over the last 28 days (the busiest was {} with {} messages sent).\n\nThe top ten posters are:\n{}\n {} {}".format(
                        count, room, messagesToday, perDayLastFourWeeks, 
                        busiest[0], busiest[1], 
                        topTen, allTimeUrl, last28Url), message['data']['id'])

                elif message['data']['content'].startswith('!rank'):
                    words = message['data']['content'].split()
                    if len(words) == 1:
                        request = message['data']['sender']['name']
                    else: request = words[1][1:]
                    position = getPosition(request)
                    heimdall.send(str(int(position)), message['data']['id'])

    except sqlite3.IntegrityError:
        conn.close()
        heimdall.log()
    except Exception:
        heimdall.log()
        conn.close()
        heimdall.disconnect()
    finally:
        time.sleep(5)
