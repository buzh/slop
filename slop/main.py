import signal
import sys
import os
import argparse
import tempfile
import shutil
import urwid as u
from slop.app import Slop
from slop import anonymize

def main():
    parser = argparse.ArgumentParser(
        description='slop - A top-like TUI monitor for Slurm HPC clusters',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        '--export',
        action='store_true',
        help='Collect Slurm data, anonymize it, and write '
             'slop-debug-bundle-YYYY-MM-DD-HH-MM.tar.gz to the current directory',
    )
    mode.add_argument(
        '--import',
        dest='import_path',
        metavar='FILE',
        help='Load data from a previously exported .tar.gz bundle',
    )

    args = parser.parse_args()

    if args.export:
        path, anon = anonymize.export_bundle()
        print(f'Wrote {path}')
        print(f'  users replaced:    {len(anon.user_map)}')
        print(f'  accounts replaced: {len(anon.account_map)}')
        print(f'  text tokens:       {len(anon.text_token_map)}')
        return

    if args.import_path:
        tmp = tempfile.mkdtemp(prefix='slop-import-')
        try:
            data_dir = anonymize.extract_bundle(args.import_path, dest_dir=tmp)
            main_screen = Slop(offline_data_dir=data_dir)
            main_screen.startloop()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        return

    main_screen = Slop()
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
