"""The hermothr module provides a Hermothr object for use elsewhere"""

import datetime
import json
import multiprocessing as mp
import os
import pprint
import queue
import re
import sqlite3
import sys
import time

import karelia


class Hermothr:
    """The Hermothr object is a self-contained instance of the hermothr bot, connected to a single room"""
    def __init__(self, room, **kwargs):
        if type(room) == str:
            self.room = room
            self.queue = None
        else:
            self.room = room[0]
            self.queue = room[1]

        self.test = True if ('test' in kwargs and kwargs['test']) or room == "test_data" else False
        self.conn = sqlite3.connect('data/hermothr/test_data.db') if self.test else sqlite3.connect('yggdrasil.db')
        self.c = self.conn.cursor()

        self.hermothr = karelia.newBot('Hermóðr', self.room)
        self.not_commands = ['!nnotify', '!herm', '!hermothr']

        self.long_help_template = ""
        self.short_help_template = ""
        self.messages_to_be_delivered = []

        self.thought_delivered = {}
        self.message_body_template = "<{} to {} {} ago in &{}> {}"

        try:
            self.write_to_database('''  CREATE TABLE notifications(
                                            sendername text,
                                            recipient text,
                                            allrecipients text,
                                            time real,
                                            room text,
                                            message text,
                                            globalid text,
                                            delivered int,
                                            id int
                                        )''')
            self.write_to_database('''CREATE UNIQUE INDEX notificationid on notifications(globalid)''')
        except:
            self.hermothr.log()

        try:
            self.write_to_database('''  CREATE TABLE groups(
                                            groupname text,
                                            members text
                                        )''')
            self.write_to_database('''CREATE UNIQUE INDEX groupname ON groups(groupname)''')
        except:
            self.hermothr.log()

    def gen_help_messages(self, count=0):
        """Produces help messages conforming to the templates below"""
        self.long_help_template = """A replacement for the much-missed @NotBot.
Accepted commands are {} (!herm will be used below, but any in the list can be substituted.)
!herm @person (@person_two, @person_three, *group_one, *group_two...) message
    Any combination of nicks and groups can be used.
Use !reply as the child of a notification to reply to the sender:
[Pouncy Silverkitten] checks for mail
    [Hermóðr] <Policy Sisterwritten 08:37:27 ago in &xkcd> Hello :-)
        [Pouncy Silverkitten] !reply Hi!
            [Hermóðr] Will do.
    Nota Bene: any user can !reply to a delivered message. The reply, when delivered, will reflect the nick of the user who replied.
If replying to a message with more than one recipient, a !reply command will send the reply to the sender of the original message, not every recipient.
Use !group and !ungroup to add yourself (or anyone else) to a group that can receive messages just like a person.
!group *group @person (@person_two, @person_three...)
!ungroup *group @person (@person_two, @person_three...)
    Nota Bene: @Hermóðr also obeys the !tgroup and !tungroup commands, so long as they employ the 'basic' syntax described above. It will obey them silently - i.e., it will not reply to them.

Use !grouplist to see all the groups and their members, or !grouplist *group to list the members of a specific group.
    
@Hermóðr also obeys the euphorian bot standards. It\'s likely to have bugs; when you find one, notify Pouncy or log it at https://github.com/PouncySilverkitten/yggdrasil/issues/new. Part of the Yggdrasil Project. {} messages delivered to date."""
        self.short_help_template = 'Use {} to send messages to people who are currently unavailable.'
        self.hermothr.stockResponses['longHelp'] = self.long_help_template.format(', '.join(self.not_commands), count)
        self.hermothr.stockResponses['shortHelp'] = self.short_help_template.format(', '.join(self.not_commands))
        if os.path.basename(os.path.dirname(os.path.realpath(__file__))) != "prod-yggdrasil":
            self.hermothr.stockResponses['longHelp'] += "\nThis is a testing instance and may not be reliable."

    def write_to_database(self, statement, **kwargs):
        values = kwargs['values'] if 'values' in kwargs else ()
        mode = kwargs['mode'] if 'mode' in kwargs else "execute"

        if self.queue is not None:
            send = (statement, values, mode,)
            self.queue.put(send)

        else:
            if mode == "execute":
                self.c.execute(statement, values)
            elif mode == "executemany":
                self.c.execute_many(statement, values)
            else:
                pass
            self.conn.commit()

    def list_groups(self):
        """Produces a string in the form group_name: members"""
        groups_as_string = ""
        groups = self.get_dict_of_groups()
        for group in groups.keys():
            groups_as_string += "{}: {}\n".format(group, groups[group].replace(',', ', '))
        return groups_as_string

    def format_recipients(self, names):
        """Produces a nice list of recipients in human-pleasing format"""
        names_as_string = ""
        for i in range(len(names)):
            if i == len(names) - 1 and not len(names) == 1:
                names_as_string += "& {}".format(names[i])
            elif i == len(names) - 1:
                names_as_string += "{}".format(names[i])
            elif i == len(names) - 2:
                names_as_string += "{} ".format(names[i])
            else:
                names_as_string += "{}, ".format(names[i])
        return names_as_string

    def check_messages_for_sender(self, sender):
        """Returns a list of messages for a given sender"""
        self.c.execute('''SELECT * FROM notifications WHERE delivered IS 0 AND recipient IS ?''', (sender,))
        for_sender = self.c.fetchall()
        return for_sender

    def time_since(self, before):
        """Uses deltas to produce a human-readable description of a time period"""
        now = datetime.datetime.utcnow()
        then = datetime.datetime.utcfromtimestamp(before)

        delta = now - then
        delta_string = str(delta).split('.')[0]
        delta_string = delta_string.replace(':', 'h', 1)
        delta_string = delta_string.replace(':', 'm', 1)
        delta_string = delta_string.replace('0h','')
        return delta_string

    def generate_not_commands(self):
        """Adds or removes `!notify` from the list of not_commands"""
        self.hermothr.send({'type': 'who'})
        while True:
            message = self.hermothr.parse()
            if message['type'] == 'who-reply': break
        if not self.check_for_notbot(message['data']['listing']) and '!notify' not in self.not_commands:
            self.not_commands.append('!notify')
        elif '!notify' in self.not_commands:
            self.not_commands.remove('!notify')
        self.gen_help_messages()

    def check_for_notbot(self, listing):
        """Returns True if @NotBot is present, else returns False"""
        for item in listing:
            if 'bot:' in item['id'] and item['name'] == 'NotBot':
                return True
        return False

    def check_for_messages(self, packet):
        """Produces a formatted, usable list of messages for a nick"""
        sender = self.hermothr.normaliseNick(packet['data']['sender']['name'])
        self.c.execute('''SELECT * FROM notifications WHERE delivered IS 0 AND recipient IS ?''', (sender,))
        messages_for_sender = self.c.fetchall()
        messages = []
        for message in messages_for_sender:
            messages.append((self.message_body_template.format( message[0],
                                                                message[2],
                                                                self.time_since(message[3]),
                                                                message[4],
                                                                message[5]),
                                                                message[6]))

        return messages

    def check_parent(self, parent):
        """Checks if a message_id belongs to a message sent by the bot"""
        self.c.execute('''SELECT COUNT(*) FROM notifications WHERE room IS ? AND delivered IS 1 AND id IS ?''', (self.room, parent,))
        if self.c.fetchone()[0] == 0:
            return False
        return True

    def bland(self, name):
        """Strips whitespace"""
        return re.sub(r'\s+', '', name)

    def read_who_to_notify(self, split_content):
        """ 
        Reads groups and users from a message

        Returns a list of names. If the notnotify is to a group, a list of names
        will still be returned."""
        names = []
        words = []
        message = split_content[1:]
        groups = self.get_dict_of_groups()
        for word in message:
            if word[0] == "@":
                name = word[1:]
                names.append(name)
            elif word[0] == '*':
                group = word[1:]
                if group in groups:
                    names += groups[group].split(',')
            elif len(names) > 0:
                return list(set(names))
            else:
                return None

        if names == []:
            return None
        return list(set(names))

    def get_dict_of_groups(self):
        self.c.execute('''SELECT * FROM groups''')
        groups = dict(self.c.fetchall())
        return groups

    def add_to_group(self, split_contents):
        """Handles !group commands"""
        grouped = []
        not_grouped = []
        del split_contents[0]
        groups = self.get_dict_of_groups()
        if split_contents[0][0] == '*':
            group_name = split_contents[0][1:]
            if group_name in groups:
                members = groups[group_name].split(',')
            else:
                members = []
            del split_contents[0]
            for word in split_contents:
                if word[0] == '@':
                    nick = word[1:]
                    if nick not in members:
                        members.append(nick)
                        grouped.append(nick)
                    else:
                        not_grouped.append(nick)

            members = ','.join(members)
            self.write_to_database('''INSERT OR REPLACE INTO groups VALUES (?, ?)''', values=(group_name, members))

            if "!notify" in self.not_commands:
                if grouped == [] and not_grouped == []:
                    return "Couldn't find anyone to add. Syntax is !group *Group @User (@UserTwo...)"
                elif grouped == []:
                    return "User(s) specified are already in the group."
                elif not_grouped == []:
                    return "Adding {} to group {}.".format(", ".join(grouped), group_name)
                else:
                    return "Adding {} to group {} ({} already added).".format(", ".join(grouped), group_name, ", ".join(not_grouped))

        elif "!notify" in self.not_commands:
            return "Couldn't find a group to add user(s) to. Syntax is !group *Group @User (@UserTwo...)"

    def remove_from_group(self, split_content):
        """Handles !ungroup commands"""
        del split_content[0]
        groups = self.get_dict_of_groups()
        ungrouped = []
        not_ungrouped = []
        if split_content[0][0] == '*':
            group_name = split_content[0][1:]
            
            if not group_name in groups and '!notify' in self.not_commands:
                return "Group {} not found. Use !grouplist to see a list of all groups.".format(group_name)

            members = groups[group_name].split(',')
            del split_content[0]
            
            for word in split_content:
                if word[0] == "@":
                    nick = word[1:]
                    if nick in members:
                        members.remove(nick)
                        ungrouped.append(nick)
                    else:
                        not_ungrouped.append(nick)

            if members == []:
                self.write_to_database('''DELETE FROM groups WHERE groupname IS ?''', values=(group_name,))
            else:
                self.write_to_database('''UPDATE groups SET members=? WHERE groupname IS ?''', values=(','.join(members), group_name,))
            
            if "!notify" in self.not_commands:
                if ungrouped == [] and not_ungrouped == []:
                    return "Couldn't find anyone to remove. Syntax is !ungroup *Group @User (@UserTwo...)"
                elif ungrouped == []:
                    return "No user(s) specified are in the group."
                elif not_ungrouped == []:
                    return "Removing {} from group {}.".format(", ".join(ungrouped), group_name)
                else:
                    return "Removing {} from group {} ({} not in group).".format(", ".join(ungrouped), group_name, ", ".join(not_ungrouped))

        elif "!notify" in self.not_commands:
            return "Couldn't find a group to remove users from. Syntax is !ungroup *Group @User (@UserTwo...)"

    def remove_names(self, split_content):
        """Removes the names of the recipients from the text of a message"""
        recipients = []
        while True:
            if len(split_content) > 0 and split_content[0][0] in ['*', '@']:
                if split_content[0][0] == '@':
                    split_content[0] = split_content[0][1:]
                recipients.append(split_content[0])
                del split_content[0]
            else:
                return ' '.join(split_content), ', '.join(recipients)

    def parse(self, packet):
        """Handles all the commands supported"""
        if packet['type'] == 'join-event' or packet['type'] == 'part-event':
            if packet['data']['name'] == 'NotBot' and packet['data']['id'].startswith('bot:'):
                self.generate_not_commands()

        elif packet['type'] == 'send-reply':
            if packet['data']['content'] in self.thought_delivered:
                content = packet['data']['content']
                message_id = packet['data']['id']
                globalid = self.thought_delivered[content]
                self.write_to_database('''UPDATE notifications SET delivered=1, id=? WHERE globalid IS ?''', values=(message_id, globalid))
                self.c.execute('''SELECT COUNT(*) FROM notifications WHERE room IS ? AND delivered IS 1''', (self.room,))
                self.gen_help_messages(self.c.fetchone()[0])
                del self.thought_delivered[packet['data']['content']]
            packet_id = packet['data']['id']
            packet_name = packet['data']['content'].split()[0][1:]

        elif packet['type'] == 'send-event' and not ('bot:' in packet['data']['sender']['id'] and 'Heimdall' != packet['data']['sender']['name']):
            # Handle a !(not)notify
            split_content = packet['data']['content'].split()
            if split_content[0] in self.not_commands:
                # Returns a list of recipients
                recipients = self.read_who_to_notify(split_content)
                if recipients == None:
                    return "/me couldn't find a person or group to notify there (use !help @Hermóðr to see an example)"
                else:
                    # Returns the message body
                    sane_message, all_recipients = self.remove_names(split_content[1:])

                    if len(sane_message) == 0 or sane_message.isspace():
                        return "/me can't see a message there"

                    sender_name = self.bland(packet['data']['sender']['name'])
                    if sender_name in [self.bland(recipient) for recipient in recipients]:
                        recipients.remove(self.bland(packet['data']['sender']['name']))

                    recipients[:] = [recipient for recipient in recipients if recipient not in [",",""]]

                    if len(recipients) == 0: return "/me won't tell you what you already know"
                    recipients.sort()
                    names_as_string = self.format_recipients(recipients)

                    # Used to get a list for the response - for group and multi-nick notifies
                    for name in recipients:
                        write_packet = (sender_name,
                                        self.hermothr.normaliseNick(name),
                                        all_recipients,
                                        time.time(),
                                        self.room,
                                        sane_message,
                                        "{}{}{}{}".format(  sender_name,
                                                            time.time(),
                                                            self.hermothr.normaliseNick(name),
                                                            all_recipients),
                                        0,
                                        '')
                        self.write_to_database('''INSERT INTO notifications VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)''', values=write_packet)

                    return "/me will notify {}.".format(names_as_string)

            elif split_content[0] == "!reply" and 'parent' in packet['data']:
                parent = packet['data']['parent']
                if self.check_parent(parent):
                    self.c.execute('''SELECT recipient FROM notifications WHERE id IS ?''', (parent,))
                    recipient = self.c.fetchone()[0]
                    sane_message = ' '.join(split_content[1:])

                    if len(sane_message) == 0 or sane_message.isspace():
                        return "/me can't see a message there"

                    sender_name = self.bland(packet['data']['sender']['name'])
                    all_recipients = "you"
                    

                    write_packet = (sender_name,
                                    self.hermothr.normaliseNick(recipient),
                                    all_recipients,
                                    time.time(),
                                    self.room,
                                    sane_message,
                                    "{}{}{}{}".format(  sender_name,
                                                        time.time(),
                                                        self.hermothr.normaliseNick(recipient),
                                                        all_recipients),
                                    0,
                                    '')
                    self.write_to_database('''INSERT INTO notifications VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)''', values=write_packet)
                    return "Will do."

            elif split_content[0] in ["!group", "!tgroup"] and len(split_content) > 1:
                return self.add_to_group(split_content)
            elif split_content[0] in ["!ungroup", "!tungroup"] and len(split_content) > 1:
                return self.remove_from_group(split_content)
            elif len(split_content) == 1 and split_content[0] == '!grouplist':
                return self.list_groups()
            elif split_content[0] == '!grouplist':
                group_name = split_content[1][1:]
                groups = self.get_dict_of_groups()
                if group_name in groups:
                    return '\n'.join(groups[group_name].split(','))
                else:
                    return "Group not found. !grouplist to view."


    def main(self):
        """
        main acts as an input redirector, calling functions as required.

        Currently, `!notnotify @user` and `!notnotify *group` are supported, as well as
        `!group *group @user`, `!ungroup *group @user`, and `!reply message`.

        - `!notnotify` will add a notify for user or group.
        - `!reply`, sent as the child of a notnotification, will send that reply to
        the original sender.
        - `!group` adds the specified user(s) to the specified group
        - `!ungroup` removes the specified user(s) from the specified groups
        """

        message = ""
        while True:
            try:
                self.hermothr.connect()
                self.generate_not_commands()

                while True:
                    packet = self.hermothr.parse()

                    if packet['type'] == 'send-event':
                        messages_for_sender = self.check_for_messages(packet)
                        self.messages_to_be_delivered += [(message[0], packet['data']['id'], message[1]) for message in messages_for_sender]

                        for _ in range(2):
                            if len(self.messages_to_be_delivered) != 0:
                                message, reply, globalid = self.messages_to_be_delivered[0]
                                self.hermothr.send(message, reply)
                                del self.messages_to_be_delivered[0]
                                self.thought_delivered[message] = globalid
    
                    reply = self.parse(packet)
                    if reply is not None:
                        self.hermothr.send(reply, packet['data']['id'])

            except Exception:
                self.hermothr.log()
                time.sleep(2)
            
rooms = ['xkcd', 'music', 'queer', 'bots']

def main(room):
    hermothr = Hermothr(room)
    while True:
        hermothr.main()

if __name__ == "__main__":
    
    for room in rooms:
        instance = mp.Process(target=main, args=(room,))
        instance.daemon = True
        instance.start()
    
    main('test')
