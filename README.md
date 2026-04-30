

https://github.com/user-attachments/assets/e1522f06-295f-4b20-b5cd-76cb24a0c9f1

# slop
A `top`-like utility for the Slurm HPC batch job scheduler

# April 30th, 2026: v1.1.8 is released!

Adds a new dashboard as a landing screen, and includes numerous minor improvements

# April 29th, 2026: v1.1.7a is released!

Fixes loop when a user has no job history

# April 29th, 2026: v1.1.7 is released!

Numerous minor bug fixes, as well as a code overhaul that should improve TUI performance by handing off more of the legwork to Urwid itself.

# April 21st, 2026: v1.1.6 is released!

This version introduces a brand new "job flow" screen which gives a live view of how jobs are starting and ending.
It will show an ETA for the highest priority pending jobs, which jobs have just started, which are about to end, and the most recent jobs that did end.

There is also a new screen that displays statistics from the scheduler and backfiller, along with a view of the pending jobs per partition. These two are still work in progress, so consider this a preview.

Comments or feedback is highly appreciated, especially bug or crash reports. Even a "this works on my cluster" is helpful, since I only have access to so many systems - and there are many ways to configure slurm.

# dependencies and requirements

Slurm >= 25.x with json output is explicitly supported. Older versions should work fine (as long as it supports json), but I'll only fix bugs from 25 and up.

Python >= 3.9

[Urwid](https://urwid.org) >= 4.0.0 

Any distro or architecture should work just fine, as long as the above is supported.

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
pyinstaller --collect-all=urwid --onefile slop/main.py -n slop
cp dist/slop /somewhere/in/path  # "slop" is the resulting binary
```

# RPM

You can grab pre-built x86_64 RPMs from the Releases page. Other architectures might be added in the future

# DEB

I currently don't build .deb, but if popular demand arises I'll look into it.


Enjoy slop!
It's highly addictive!
