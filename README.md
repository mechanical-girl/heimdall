heimdall
======
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

Syntax
======
UpdateDone 
------

Exception meaning that logs are up to date

KillError 
------

Exception for when the bot is killed.

### insertMessage
`insertMessage(message, dbName, conn, c)`: 
Inserts a new message into the database of messages

