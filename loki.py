from karelia import Packet
import sqlite3


class Loki:
    def __init__(self, normalise, db, should_return):
        self.normalise = normalise
        
        self.conn = sqlite3.connect(db)
        self.c = self.conn.cursor()

        self.should_return = should_return

        if not self.should_return:
            self.queue = queue


    def parse(self, message: Packet, room: str):
        if message.type == 'send-event' and message.data.sender.name == "TellBot" and 'bot:' in message.data.sender.id and message.data.content.startswith("Aliases of"):
            self.c.execute('SELECT normname FROM messages WHERE room = ? AND id = ?''', (room, message.data.parent,))
            sender = self.c.fetchone()[0]
            if message.data.content.split()[3] == 'before':
                aliases = [alias for alias in message.data.content.split('\n')[1].split()[5:]]
                aliases = [alias for alias in aliases]
            else:
                aliases = [alias for alias in message.data.content.split()[4:]]

            try:
                aliases.remove('and')
            except:
                pass

            up_to_date_aliases = [alias.replace('you',sender).replace(',','') for alias in aliases]
    
            master = up_to_date_aliases[0]

            for alias in up_to_date_aliases:
                self.c.execute('''SELECT master FROM aliases WHERE normalias=?''', (self.normalise(alias),))
                try:
                    master = self.c.fetchone()[0]
                    break
                except TypeError:
                    continue

            stored_aliases = set(self.get_aliases(sender))
            correct_aliases = set(up_to_date_aliases)

            add_aliases = correct_aliases - stored_aliases
            remove_aliases = stored_aliases - correct_aliases


            queries = []
            for alias in add_aliases:
                queries.append(('''INSERT INTO ALIASES VALUES(?, ?, ?)''', (master, alias, self.normalise(alias),),))

            for alias in remove_aliases:
                queries.append(('''DELETE FROM aliases WHERE normalias=?''', (self.normalise(alias),),))

            return queries

    def get_aliases(self, user):
        normnick = self.normalise(user)
        self.c.execute('''SELECT master FROM aliases WHERE normalias = ?''', (normnick,))

        try:
            master = self.c.fetchall()[0][0]
        except:
            master = normnick

        self.c.execute('''SELECT alias FROM aliases WHERE master = ?''', (master, ))
        reply = self.c.fetchall()
        if not reply:
            reply = []
        else:
            reply = [alias[0] for alias in reply]
        
        return reply

