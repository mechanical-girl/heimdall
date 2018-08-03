import multiprocessing as mp
import time
import argparse

import forseti
import heimdall
import karelia

class UpdateDone(Exception):
    pass

class KillError(Exception):
    pass

def on_sigint(signum, frame):
    """Gracefully handle sigints"""
    try:
        heimdall.conn.commit()
        heimdall.conn.close()
        heimdall.heimdall.disconnect()
    finally:
        sys.exit()

def run_forseti(queue):
    forseti.main(queue)

def run_heimdall(room, stealth, new_logs, use_logs, verbose, queue):
    if room == "test": 
        heimdall.main((room, queue), stealth=stealth, new_logs=new_logs, use_logs="xkcd", verbose=verbose)
    else:
        heimdall.main((room, queue), stealth=stealth, new_logs=new_logs, use_logs=use_logs, verbose=verbose)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("room", nargs='?')
    parser.add_argument("--stealth", help="If enabled, bot will not present on nicklist", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true", dest="verbose")
    parser.add_argument("--force-new-logs", help="If enabled, Heimdall will delete any current logs for the room", action="store_true", dest="new_logs")
    parser.add_argument("--use-logs", type=str, dest="use_logs")
    args = parser.parse_args()

    room = args.room
    stealth = args.stealth
    new_logs = args.new_logs
    use_logs = args.use_logs
    verbose = args.verbose

    rooms = ['xkcd', 'music', 'queer', 'bots', 'test']

    queue = mp.Queue()
    instance = mp.Process(target = run_forseti, args=(queue,))
    instance.daemon = True
    instance.name = "forseti"
    instance.start()

    for room in rooms:
        instance = mp.Process(target = run_heimdall, args=(room, stealth, new_logs, use_logs, verbose, queue))
        instance.daemon = True
        instance.name = room
        instance.start()
        
    yggdrasil = karelia.bot('Yggdrasil', 'test')
    yggdrasil.connect()
    while True:
        yggdrasil.parse()

if __name__ == '__main__':
    main()
