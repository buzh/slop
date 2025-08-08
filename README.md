# slop
A `top`-like utility for the Slurm HPC batch job scheduler

![slop screenshot](img/slop_screenshot1.png)

# install/run locally

```
python -m venv slop
source slop/bin/activate
git clone https://github.com/buzh/slop
cd slop
pip install -r slop/requirements.txt
python -m slop.main
```

# how to install globally

You can build a standalone binary with a tool such as `pyinstaller`.
Clone the repo and install deps like above, then:

```
pyinstaller slop/main.py
cp dist/main /somewhere/in/path
```
