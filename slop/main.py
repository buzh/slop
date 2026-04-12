import signal
import sys
import os
import urwid as u
from slop.app import SC

def main():
    # Check for --demo flag
    if '--demo' in sys.argv or '--anonymize' in sys.argv:
        os.environ['SLOP_DEMO_MODE'] = '1'
        print("Demo mode enabled - all usernames, accounts, and paths will be anonymized")

    main_screen = SC()
    main_screen.startloop()

def graceful_exit(signal, frame):
    print("Have a nice day!")
    raise u.ExitMainLoop()

def unhandled_input(key: str) -> None:
    if key == 'q':
        raise u.ExitMainLoop()

signal.signal(signal.SIGINT, graceful_exit)

if __name__ == '__main__':
    main()

