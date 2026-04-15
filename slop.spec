Name:           slop
Version:        1.1.2
Release:        1%{?dist}
Summary:        A TUI monitor for Slurm HPC clusters

License:        GPLv3
URL:            https://github.com/buzh/slop
Source0:        slop-%{version}.tar.gz

# Disable debug package since this is a PyInstaller binary
%global debug_package %{nil}

BuildRequires:  python3-devel
BuildRequires:  python3-pip
BuildRequires:  python3-virtualenv

%description
A top-like TUI application for monitoring Slurm HPC batch job scheduler.
Built with Python and Urwid, it provides real-time views of job queues,
user activity, and job details similar to how 'top' monitors system processes.

%prep
%setup -q

%build
python3 -m venv build_venv
source build_venv/bin/activate
pip install --upgrade pip
pip install -r slop/requirements.txt
pip install pyinstaller
pyinstaller --onefile \
  --hidden-import=urwid.display.html_fragment \
  --hidden-import=urwid.display.raw_display \
  --hidden-import=urwid.display.curses_display \
  --hidden-import=urwid.display.lcd_display \
  --hidden-import=urwid.display.web_display \
  --hidden-import=urwid.display.lcd \
  --hidden-import=urwid.display.web \
  slop/main.py -n slop

%install
mkdir -p %{buildroot}%{_bindir}
install -m 755 dist/slop %{buildroot}%{_bindir}/slop

%files
%{_bindir}/slop
%doc README.md
%license LICENSE

%changelog
* Sun Apr 13 2026 Andreas Skau <andreas.skau@gmail.com> - 1.1.2-1
- Version 1.1.2 release
- Fix separator widths not updating on window resize
- Fix column alignment in history view
- Unify visual style across all views

* Sun Apr 06 2026 Andreas Skau <andreas.skau@gmail.com> - 1.1.1-1
- Version 1.1.1 release
- Add footer with keyboard shortcuts
- Improve responsive design for different terminal widths

* Sun Mar 30 2026 Andreas Skau <andreas.skau@gmail.com> - 1.1.0-1
- Initial RPM release
