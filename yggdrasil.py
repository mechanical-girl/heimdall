import argparse
import importlib
import json
import multiprocessing as mp
import os
import subprocess
import sys
import time

import forseti
import heimdall
import karelia


class UpdateDone(Exception):
    pass


class KillError(Exception):
    pass


class Yggdrasil:
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("room", nargs='?')
        parser.add_argument(
            "--stealth",
            help="If enabled, bot will not present on nicklist",
            action="store_true")
        parser.add_argument(
            "-v", "--verbose", action="store_true", dest="verbose")
        parser.add_argument(
            "--force-new-logs",
            help=
            "If enabled, Heimdall will delete any current logs for the room",
            action="store_true",
            dest="new_logs")
        parser.add_argument("--use-logs", type=str, dest="use_logs")
        args = parser.parse_args()

        self.room = args.room
        self.stealth = args.stealth
        self.new_logs = args.new_logs
        self.use_logs = args.use_logs
        self.verbose = args.verbose

        with open('rooms.json') as f:
            self.rooms = json.loads(f.read())

        self.queue = mp.Queue()

        self.instances = []
        instance = mp.Process(target=self.run_forseti)
        instance.daemon = True
        instance.name = "forseti"
        self.instances.append(instance)

        for room in self.rooms:
            instance = mp.Process(
                target=self.run_heimdall,
                args=(room, self.stealth, self.new_logs, self.use_logs,
                      self.verbose, self.queue))
            instance.daemon = True
            instance.name = room
            self.instances.append(instance)

    def run_heimdall(self, room, stealth, new_logs, use_logs, verbose, queue):
        if room == "test":
            heimdall.main(
                (room, queue),
                stealth=stealth,
                new_logs=new_logs,
                use_logs="xkcd",
                verbose=verbose)
        else:
            heimdall.main(
                (room, queue),
                stealth=stealth,
                new_logs=new_logs,
                use_logs=use_logs,
                verbose=verbose)

    def run_forseti(self):
        forseti.main(self.queue)

    def on_sigint(self, signum, frame):
        """Gracefully handle sigints"""
        try:
            self.terminate()
        finally:
            sys.exit(0)

    def start(self):
        for instance in self.instances:
            instance.start()

    def stop(self):
        for instance in self.instances:
            instance.terminate()


def on_sigint(signum, frame):
    pass


def main():
    importlib.reload(forseti)
    importlib.reload(heimdall)
    importlib.reload(karelia)

    ygg = Yggdrasil()
    ygg.start()

    ygg.on_sigint = on_sigint

    yggdrasil = karelia.bot('Yggdrasil', 'test')

    with open('_yggdrasil_help.json', 'r') as f:
        yggdrasil.stock_responses['long_help'] = json.loads(f.read())

    yggdrasil.connect()

    try:
        while True:
            try:
                message = yggdrasil.parse()
                if message['type'] == 'send-event':
                    if message['data']['content'] == '!restart @Yggdrasil':
                        yggdrasil.disconnect()
                        ygg.stop()
                        main()

                    elif message['data']['content'] == '!deploy @Yggdrasil':
                        with open(os.devnull, 'w') as devnull:
                            if subprocess.run(
                                ["git", "pull"], stdout=devnull,
                                    stderr=devnull).returncode == 0:
                                yggdrasil.disconnect()
                                ygg.stop()
                                main()
                            else:
                                yggdrasil.send('Pull failed - sorry.',
                                               message['data']['id'])
            except TypeError:
                pass

    except Exception:
        yggdrasil.log()


if __name__ == '__main__':
    try:
        main()
    finally:
        pass
