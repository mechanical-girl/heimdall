import argparse
import signal
import sys
from heimdall import Heimdall, KillError

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("room")
    parser.add_argument("--stealth", help="If enabled, bot will not present on nicklist", action="store_true")
    args = parser.parse_args()

    room = args.room
    stealth = args.stealth

    heimdall = Heimdall(room, stealth=stealth)

    def on_sigint(signum, frame):
        """Handles sigints"""
        try:
            heimdall.conn.commit()
            heimdall.conn.close()
            heimdall.heimdall.disconnect()
        finally:
            sys.exit()

    signal.signal(signal.SIGINT, on_sigint)
    
    while True:
        try:
            heimdall.main()
        except KillError:
            sys.exit()

if __name__ == '__main__':
    main()
