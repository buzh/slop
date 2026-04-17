Name:           slop
Version:        1.1.4
Release:        1%{?dist}
Summary:        A top-like TUI monitor for Slurm HPC clusters

License:        GPL-3.0-or-later
URL:            https://github.com/buzh/slop
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  python3
BuildRequires:  python3-pip

# We ship a single self-contained pyinstaller bundle. There is no useful
# debuginfo to extract from it, so suppress the auto-generated subpackage.
%define debug_package %{nil}

%description
slop is a top-like terminal UI for monitoring Slurm HPC batch jobs.
It provides real-time views of job queues, user activity, accounts,
partitions, job states, and cluster resources, similar to how `top`
monitors processes.

The package ships a single self-contained executable built with
PyInstaller; the bundled Python interpreter and urwid library are
included, so no Python runtime dependency is imposed on the host.

%prep
%autosetup -n %{name}-%{version}

%build
python3 -m venv build_venv
source build_venv/bin/activate
pip install --upgrade pip
pip install -r slop/requirements.txt
pip install pyinstaller
pyinstaller --onefile --name slop --clean --noconfirm slop/main.py

%install
install -D -m 0755 dist/slop %{buildroot}%{_bindir}/slop

%files
%license LICENSE
%doc README.md
%{_bindir}/slop

%changelog
* Fri Apr 17 2026 slop maintainers <noreply@example.com> - 1.1.4-1
- Package built from upstream sources via PyInstaller bundle.
