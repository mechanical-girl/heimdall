#!/usr/local/bin/python

"""
Karelia is a library of functions for connecting a bot to the Heim chat
platform at euphoria.io
"""

import json
import logging
import os
import random
import re
import sys
import time
import traceback

import websocket
from websocket import create_connection


class Packet:
    def __init__(self, **packet):
        self.packet = packet
        for k, v in packet.items():
            if isinstance(v, dict):
                self.__dict__[k] = Packet(**v)
            else:
                self.__dict__[k] = v


class bot:
    """bot represents a single bot for euphoria.io

    To create a bot which only responds to a single nick, call `karelia.bot(nick, room)`
    which will return a bot object.
    Alternatively, to have a bot respond to multiple names, call
    `karelia.bot([list, of, nicks], room)` which will present as
    the first nick in the list, but respond to stock commands send to all nicks.

    If specific action is required when the bot receives the `!kill` command, a function can be written by the user and assigned to `bot.on_kill`.

    """

    def __init__(self, name, room):
        """Inits the bot object"""
        if not isinstance(name, str):
            self.names = name
        else:
            self.names = [name]
        self.stock_responses = {'ping': 'Pong!',
                               'short_help': '',
                               'long_help': [''],
                               'paused': '/me has been paused',
                               'unpaused': '/me has been unpaused',
                               'killed': '/me has been killed'}
        self.packet = {}
        self.room = room
        self.paused = False
        self.connect_time = time.gmtime()
        self.formatted_connect_time = time.strftime("%a, %d %b %Y %H:%M:%S", time.gmtime())
        websocket.enableTrace(False)
        self.non_bmp_map = dict.fromkeys(
            range(0x10000, sys.maxunicode + 1), 0xfffd)
        self.logger = logging.getLogger(__name__)
        f_handler = logging.FileHandler('karelia.log')
        f_format = logging.Formatter(f'--------------------\n%(asctime)s - &{self.room}: %(message)s\n\n\n', datefmt='%d-%b-%y %H:%M:%S')
        f_handler.setFormatter(f_format)
        self.logger.addHandler(f_handler)


    def on_kill(self):
        pass

    def connect(self, stealth=False):
        """Connects to specified room and sets nick.

        `bot.connect()` will connect to the room and then cause the bot to appear on the nicklist.
        `bot.connect(stealth=True)` will connect to the room, but not set the nick for the bot."""

        self.stealth = stealth

        try:
            self.conn = create_connection(
                "wss://euphoria.io/room/{}/ws?h=0".format(self.room))
        except websocket._exceptions.WebSocketBadStatusException:
            raise ConnectionError('Room not found')

        if not self.stealth:
            self.change_nick()

        for _ in range(3):
            self.conn.recv()

    def change_nick(self, nick=''):
        """ 
        `change_nick` sends the `nick` command to Heim servers.

        If the bot only has a single nick:
        - `bot.change_nick()` will cause the bot to set its nick to the previously specified value
        - `bot.change_nick("nick")` will cause the bot to set its nick to `nick` *and* store `nick` for future reference.

        If the bot has multiple nicks specified:
        - `bot.changenick()` will cause the bot to set its nick to the first nick in its list
        - `bot.changenick("nick")` will cause the bot to set its nick to `nick` *and* store `nick` as the first value in its list

        """

        if nick == '':
            nick = self.names[0]
        elif len(self.names) == 1:
            self.names = [nick]
        else:
            try:
                self.names.remove(nick)
            finally:
                self.names.insert(0, nick)

        self.send({"type": "nick", "data": {"name": nick}})

    def get_uptime(self):
        """Called by the `!uptime` command. Returns time since connect as string."""
        self.updays = 0
        self.uphours = 0
        self.upminutes = 0
        self.upseconds = 0

        self.upticks = time.time() - time.mktime(self.connect_time)
        while self.upticks > 86400:
            self.updays += 1
            self.upticks -= 86400
        while self.upticks > 3600:
            self.uphours += 1
            self.upticks -= 3600
        while self.upticks > 60:
            self.upminutes += 1
            self.upticks -= 60

        self.uptime = "/me has been up since {} UTC ({} days, {} hours, {} minutes)".format(self.formatted_connect_time, self.updays, self.uphours, self.upminutes)
        return(self.uptime)

    def send(self, message, parent=''):
        """
        Unless the bot is paused, sends the supplied message. The parent message can be specified: `send(message, parent = parent_id)`.

        If the `message` argument has type `dict`, it will be sent as a packet. Otherwise, it will be treated as the body of a message.

        With format `send(message, parent):`
        - `message`: either a complete packet, or the a message in string form.
        - `parent`: the id of the message being replied to. If not specified,
        karelia will send the message as a new parent i.e. bottom-level message.

        `bot.send('Top-level message')` will send that as a top-level message.

        `bot.send('It's a reply!','02aa8y85m7hts')` will send that message as
        a reply to the message with id `02aa8y85m7hts`.

        `bot.send({'type': 'log', 'data': {'n':1000}})` will send a log
        request for the thousand most recent messages posted to the room.
        """
        if not self.paused:
            if isinstance(message, dict):
                if message['type'] == 'send':
                    raise MessageAsDict("Passed message with type send")
                self.conn.send(json.dumps(message))
            elif len(message) > 0:
                self.conn.send(json.dumps({'type': 'send',
                                           'data': {'content': message,
                                                     'parent': parent}}))

    def reply(self, message):
        """
        Wrapper around `bot.send()`

        Sends the only argument as a reply to the most recently `parse()`d message.
        """ 
        self.send(message, self.packet.data.id)

    def disconnect(self):
        """Attempts to close the connection at `self.conn`. If unsuccessful, it will log and raise an Exception.""" 
        try:
            self.conn.close()
        except Exception as e:
            self.logger.exception("Unable to disconnect.")
            raise

    def parse(self):
        """
        `parse()` handles the commands specified in the Botrulez
        (github.com/jedevc/botrulez) and those required to stay alive.

        `parse()` is a blocking function - that is, it will always wait until it
        receives a packet from heim before returning.

        On receiving a packet, it will reply to pings (both global and specific),
        offer uptime, pause and unpause the bot, respond to help requests (again,
        both global and specific) and antighost commands, and kills the bot.

        For all commands with a name attached, it will reply if any of the names
        stored in `self.names` match.

        The responses to all botrulez-mandated commands (with the exception of
        uptime, as The Powers That Be disapprove of dissident response formats
        to it) can be altered with the `bot.stock_responses` dict. The following
        values are available:

        | key           | default value             |
        |---------------|---------------------------|
        | 'ping'        | 'Pong!'                   |
        | 'short_help'  | (no response)             |
        | 'long_help'   | (no response)             |
        | 'paused'      | '/me has been paused'     |
        | 'unpaused'    | '/me has been unpaused'   |
        | 'killed'      | '/me has been killed'     |

        Regardless of actions taken, it will return the unaltered packet. If an
        error occurs, it will return an exception.

        Note: as of 2017-03-16 if killed, it will disconnect automatically
        and return the string 'Killed'.
        Note: as of 2018-06-22 if killed, it will log the killer, run `bot.on_kill()`, and then exit.
        """

        incoming = json.loads(self.conn.recv())
        packet = Packet(**incoming)

        if self.packet != packet:
            self.packet = packet

            if self.packet.type == "ping-event":
                self.conn.send(json.dumps({'type': 'ping-reply',
                                           'data': {'time': packet.data.time}}))
                self.logger.warning(f'Replied to a ping-event from {packet.data.time} at {time.time()}, {time.time()-packet.data.time} seconds later.')

            elif self.packet.type == "send-event":

                if len(self.packet.data.content) > 0 and self.packet.data.content[0] == '!':

                    if self.packet.data.content == '!ping':
                        self.reply(self.stock_responses['ping'])
                    elif self.packet.data.content == '!help':
                        self.reply(self.stock_responses['short_help'])
                    elif self.packet.data.content == "!antighost" and not self.stealth:
                        self.change_nick(self.names[0])

                    command = self.packet.data.content.split()[0]

                    try:
                        command_name = self.normalise_nick(self.packet.data.content.split()[1][1:])
                    except IndexError:
                        command_name = ''

                    if command_name in [self.normalise_nick(name) for name in self.names]:
                        if command == '!ping':
                            self.reply(self.stock_responses['ping'])
                        if command == '!uptime':
                            self.reply(self.get_uptime())
                        if command == '!pause':
                            self.reply(self.stock_responses['paused'])
                            self.paused = True
                            self.logger.warning(f'PauseEvent from {self.packet.data.sender.name}.')
                        if command == '!unpause':
                            self.logger.warning(f'UnpauseEvent from {self.packet.data.sender.name}.')
                            self.paused = False
                            self.reply(self.stock_responses['unpaused'])
                        if command == '!help':
                            if isinstance(self.stock_responses['long_help'], str):
                                self.stock_responses['long_help'] = [self.stock_responses['long_help']]
                            for help_message in self.stock_responses['long_help']:
                                sending = help_message.format(self.normalise_nick(self.packet.data.sender.name))
                                self.reply(sending)
                        if command == '!kill':
                            try:
                                self.logger.warning(f'KillEvent from {self.packet.data.sender.name}')
                                self.reply(self.stock_responses['killed'])
                                self.disconnect()
                                self.on_kill()
                            finally:
                                sys.exit(0)

            return(self.packet)

    def normalise_nick(self, nick):
        """Return the known-standard form (i.e., lowercase with no whitespace) of the supplied nick."""
        return(re.sub(r'\s+', '', nick.translate(self.non_bmp_map)).lower())
