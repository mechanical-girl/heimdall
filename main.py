import argparse
import signal
import sys
from heimdall import Heimdall

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("room")
    parser.add_argument("--stealth", help="If enabled, bot will not present on nicklist", action="store_true")
    args = parser.parse_args()

    room = args.room
    stealth = args.stealth

    heimdall = Heimdall(room, stealth, True)

    def onSIGINT(signum, frame):
        """Handles sigints"""
        try:
            heimdall.conn.commit()
            heimdall.conn.close()
        finally:
            sys.exit()

    signal.signal(signal.SIGINT, onSIGINT)
    
    while True:
        heimdall.main()

if __name__ == '__main__':
    main()
