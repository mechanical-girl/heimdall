import argparse
import logging
import importlib
import json
import multiprocessing as mp
import os
import hermothr
import subprocess
import sys
import time

import karelia

import forseti
import heimdall


class UpdateDone(Exception):
    pass


class KillError(Exception):
    pass


class Yggdrasil:
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("room", nargs='?')
        parser.add_argument("--stealth", help="If enabled, bot will not present on nicklist", action="store_true")
        parser.add_argument("-v", "--verbose", action="store_true", dest="verbose")
        parser.add_argument("--force-new-logs", help="If enabled, Heimdall will delete any current logs for the room", action="store_true", dest="new_logs")
        parser.add_argument("--use-logs", type=str, dest="use_logs")
        parser.add_argument("--fill-in", "-f", action="store_true", dest="fill_in")

        args = parser.parse_args()

        self.room = args.room
        self.stealth = args.stealth
        self.new_logs = args.new_logs
        self.use_logs = args.use_logs
        self.verbose = args.verbose
        self.fill_in = args.fill_in

        with open('rooms.json') as f:
            self.rooms = json.loads(f.read())

        self.queue = mp.Queue()

        log_format = logging.Formatter(f'\n\n--------------------\n%(asctime)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        self.logger = logging.getLogger(__name__)
        handler = logging.FileHandler('Yggdrasil.log')
        handler.setFormatter(log_format)
        self.logger.addHandler(handler)

        self.logger.warning('Yggdrasil yawns and stretches, its roots stretching over the whole of the nine realms.')

        try:

            self.instances = []
            instance = mp.Process(target=self.run_forseti)
            instance.daemon = True
            instance.name = "forseti"
            self.instances.append(instance)
        except:
            self.logger.exception("Error initialising forseti.")

        for room in self.rooms:
            try:
                instance = mp.Process(target=self.run_heimdall, args=(room, self.stealth, self.new_logs, self.use_logs, self.verbose, self.fill_in, self.queue))
                instance.daemon = True
                instance.name = room
                self.instances.append(instance)
            except:
                self.logger.exception(f"Error initialising heimdall in {room}")

            try:

                instance = mp.Process(target=self.run_hermothr, args=())
                instance.daemon = True
                instance.name = f"hermothr_{room}"
                self.instances.append(instance)
            except:
                self.logger.exception(f"Error initialising hermothr in {room}")


    def run_forseti(self):
        try:
            forseti.main(self.queue)
        except:
            self.logger.exception(f"Error initialising forseti")


    def run_heimdall(self, room, stealth, new_logs, use_logs, verbose, fill_in, queue):
        try:
            if room == "test":
                heimdall.main((room, queue), stealth=stealth, new_logs=new_logs, use_logs="xkcd", verbose=verbose, fill_in)
            else:
                heimdall.main((room, queue), stealth=stealth, new_logs=new_logs, use_logs=use_logs, verbose=verbose, fill_in)
        except:
            self.logger.exception(f"Error initialising heimdall in {room}")

    def run_hermothr(self):
        try:
            #hermothr.main(self.queue)
            pass
        except:
            self.logger.exception(f"Error initialising hermothr")

    def on_sigint(self, signum, frame):
        """Gracefully handle sigints"""
        try:
            self.terminate()
        except:
            self.logger.traceback('Failed to exit on sigint')
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


def run_deploy():
    with open(os.devnull, 'w') as devnull:
        pull_result = subprocess.run(["git", "pull"], stdout=devnull, stderr=devnull).returncode
        if pull_result != 0:
            return pull_result

        update_result = subprocess.run(["update.sh"], stdout=devnull, stderr=devnull).returncode
        if update_result != 0:
            return update_result

    return 0


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
            message = yggdrasil.parse()
            if message.type == 'send-event':
                if message.data.content == '!restart @Yggdrasil':
                    yggdrasil.disconnect()
                    ygg.stop()
                    main()

                elif message.data.content == '!deploy @Yggdrasil':
                    if run_deploy() == 0:
                        yggdrasil.disconnect()
                        ygg.stop()
                        main()
                    else:
                        yggdrasil.send('Deploy failed - sorry.', message.data.id)
    except TimeoutError:
        ygg.logger.exception("Timeout from Heim.")
    except Exception:
        ygg.logger.exception(f'Crashed on message f{json.dumps(yggdrasil.packet.packet)}')
    finally:
        ygg.stop()
        yggdrasil.disconnect()
        time.sleep(1)


if __name__ == '__main__':
    while True:
        try:
            main()
        finally:
            pass
