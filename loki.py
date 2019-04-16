from karelia import Packet


class Loki:
    def __init__(self):
        pass

    def parse(self, sender: str, message: Packet):
        if message.type == 'send-event' and message.data.content.split()[0] == '!alias' or '!unalias':
            if message.data.content.startswith("Aliases of"):
                if message.data.content.split()[3] == 'before':
                    aliases = [alias for alias in message.data.content.split('\n')[1].split()[5:]]
                    aliases = [alias for alias in aliases]
                else:
                    aliases = [alias for alias in message.data.content.split()[4:]]

                try:
                    aliases.remove('and')
                except:
                    pass

                aliases = [alias.replace('you',sender).replace(',','') for alias in aliases]
                return aliases
