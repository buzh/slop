import signal
import urwid as u
from slop.app import SC

def main():
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

