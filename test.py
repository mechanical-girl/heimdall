import datetime
import time
import calendar
import matplotlib.pyplot as plt
import sqlite3
import pyimgur
CLIENT_ID = "28b07e24d29d9f6"
im = pyimgur.Imgur(CLIENT_ID)


def nextDay(day):
    oneDay = 60*60*24
    return(int(calendar.timegm(datetime.date.fromtimestamp(day).timetuple())+oneDay))

messagesPerDay = {}

conn = sqlite3.connect("logs.db")
c = conn.cursor()

c.execute('''SELECT * from xkcd ORDER BY time ASC LIMIT 1''')
firstMessage = int(c.fetchone()[6])

firstDate = datetime.date.fromtimestamp(firstMessage)

day = calendar.timegm(datetime.date.fromtimestamp(firstMessage).timetuple())

while time.time() > day+60*60*24:
    c.execute('''SELECT count(*) FROM xkcd WHERE ? <= time AND time < ?''', (int(day),int(nextDay(day))))
    messagesPerDay[day] = int(c.fetchone()[0])
    day = nextDay(day)
    
plt.plot([datetime.date.fromtimestamp(date) for date in messagesPerDay],[messagesPerDay[date] for date in messagesPerDay])
plt.gcf().autofmt_xdate()
plt.savefig('output.png')

upload = im.upload_image("output.png")
print(upload.link)
