import signal
import sys
import os
import argparse
import urwid as u
from slop.app import SC

def main():
    parser = argparse.ArgumentParser(
        description='slop - A top-like TUI monitor for Slurm HPC clusters',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--offlinedata',
        metavar='DIR',
        help='Load data from directory instead of querying Slurm (for testing/debugging)'
    )

    args = parser.parse_args()

    main_screen = SC(offline_data_dir=args.offlinedata)
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

