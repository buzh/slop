

https://github.com/user-attachments/assets/e1522f06-295f-4b20-b5cd-76cb24a0c9f1

# slop
A `top`-like utility for the Slurm HPC batch job scheduler

# April 17th, 2026: v1.1.5 is released

Good news, everyone! Slop 1.1.5 is here with loads of optimizations and bug fixes, and
we also provide RPM files for RHEL/Rocky/Alma 9 for your convenience.

# April 12th, 2026: Version 1.1 is released!

This is a major overhaul which introduces several new features and improvements, including:

- Views for accounts, partitions and job states
- A brand new cluster resource monitor
- Search functionality; just hit `/` and type in a job id, user or node name
- Ability to inspect older jobs
- A ton of optimizations and performance improvements

Enjoy Slop!
It's highly addictive!

# dependencies and requirements

Uses [Urwid](https://urwid.org) to build the TUI.
Runs on any host with `scontrol` set up.

# install/run locally

```
python -m venv slop_venv
source slop_venv/bin/activate
git clone https://github.com/buzh/slop
cd slop
pip install -r slop/requirements.txt
python -m slop.main
```

# how to install globally

You can build a standalone binary with a tool such as `pyinstaller`.
Clone the repo, create venv and install deps as above, then:

```
pip install pyinstaller
pyinstaller --onefile slop/main.py -n slop
cp dist/slop /somewhere/in/path  # "slop" is the resulting binary
```
