import multiprocessing as mp
import time

import forseti
import heimdall
import karelia
import argparse

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

def run_heimdall(room, queue, new_logs):
    if room == "test": 
        heimdall.main((room, queue), verbose=False, new_logs=new_logs, use_logs="xkcd")
    else:
        heimdall.main((room, queue), verbose=False, new_logs=new_logs)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-new-logs", action="store_true", dest="new_logs")
    args = parser.parse_args()

    new_logs = args.new_logs

    rooms = ['xkcd', 'music', 'queer', 'bots', 'test']

    queue = mp.Queue()
    instance = mp.Process(target = run_forseti, args=(queue,))
    instance.daemon = True
    instance.start()

    for room in rooms:
        instance = mp.Process(target = run_heimdall, args=(room, queue, new_logs))
        instance.daemon = True
        instance.start()
        time.sleep(1)
        
    yggdrasil = karelia.bot('Yggdrasil', 'test')
    yggdrasil.connect()
    while True:
        yggdrasil.parse()

if __name__ == '__main__':
    main()
