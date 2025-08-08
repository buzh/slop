# slop
A `top`-like utility for the Slurm HPC batch job scheduler

![slop screenshot](img/slop_screenshot1.png)

# how to install globally

You can build a binary with a tool such as `pyinstaller`:

```
python -m venv slop_install
source slop_install/bin/activate
git clone https://github.com/buzh/slop
pip install -r slop/slop/requirements.txt
pyinstaller slop/slop/main.py
cp dist/main /somewhere/in/path
```
