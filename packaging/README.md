# Packaging

Build artifacts for distributing slop. Kept here so they don't clutter the
top-level repo.

## RPM (RHEL / AlmaLinux / Rocky 9)

CI builds RPMs automatically on every release via
`.github/workflows/build-rpm.yml`. To build one locally:

```bash
# Prereqs (one-time)
sudo dnf install -y rpm-build rpmdevtools python3 python3-pip
rpmdev-setuptree

# Match the version in slop/__init__.py
VERSION=$(grep __version__ slop/__init__.py | cut -d'"' -f2)

# Stage source tarball
mkdir -p slop-${VERSION}
cp -r slop slop-${VERSION}/
cp README.md LICENSE slop-${VERSION}/
cp packaging/rpm/slop.spec slop-${VERSION}/
tar --exclude='__pycache__' \
    -czf ~/rpmbuild/SOURCES/slop-${VERSION}.tar.gz slop-${VERSION}
rm -rf slop-${VERSION}

# Build
sed -i "s/^Version:.*/Version:        ${VERSION}/" packaging/rpm/slop.spec
cp packaging/rpm/slop.spec ~/rpmbuild/SPECS/
rpmbuild -ba ~/rpmbuild/SPECS/slop.spec
```

Output:
- `~/rpmbuild/RPMS/x86_64/slop-${VERSION}-1.<dist>.x86_64.rpm`
- `~/rpmbuild/SRPMS/slop-${VERSION}-1.<dist>.src.rpm`

The binary is a PyInstaller bundle — Python and urwid are baked in, so the
installed RPM has no runtime Python dependency.
