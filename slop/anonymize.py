"""Debug-bundle export/import with anonymization.

Public entry points:
    export_bundle(source_dir=None, output_path=None) -> (path, Anonymizer)
    extract_bundle(tar_path, dest_dir=None) -> str

`source_dir=None` collects fresh from the local Slurm install. Otherwise it
reads an existing collect_offlinedata.sh-style directory.

Bundle format mirrors collect_offlinedata.sh, plus an `ANONYMIZED.txt`
summary file. No inverse mapping is written: anonymization is one-way.
"""

import datetime
import gzip
import hashlib
import io
import json
import os
import re
import socket
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path


# ---- Field map for JSON anonymizer ------------------------------------------
#
# Recursive walker; for each dict key matching a name below, the value is
# rewritten through the named transform. Keys not listed are left alone.
# The 'name' key is special — see walk_json for the job-context guard.

JSON_FIELD_KINDS = {
    # Usernames
    'user_name': 'user',
    'user': 'user',
    'kill_request_user': 'user',
    'reason_set_by_user': 'user',
    'mail_user': 'user',
    'owner': 'user',
    # Accounts
    'account': 'account',
    # Posix groups, wckeys, free-form reasons — opaque length-preserving tokens
    # (unconditional, unlike 'name' which is gated to job context)
    'group': 'token',
    'group_name': 'token',
    'wckey': 'token',
    'reason': 'token',
    # Comma-separated lists (reservations)
    'users': 'user_list',
    'accounts': 'account_list',
    # Path-like fields
    'standard_output': 'path',
    'standard_error': 'path',
    'current_working_directory': 'path',
    'working_directory': 'path',
    'stdout': 'path',
    'stderr': 'path',
    'stdout_expanded': 'path',
    'stderr_expanded': 'path',
    'stdin': 'path',
    'stdin_expanded': 'path',
    # Commands / scripts
    'command': 'command',
    'submit_line': 'command',
    'script': 'command',
    # Job names — only when in job context (sibling key 'job_id')
    'name': 'jobname',
}


class Anonymizer:
    """Stateful real -> synthetic mapping, populated as data is walked."""

    def __init__(self):
        self.user_map = {}
        self.account_map = {}
        self.host_map = {}
        self.text_token_map = {}
        self._salt = b'slop-anon-v1'

    # -- name allocation --------------------------------------------------

    def map_user(self, real):
        if not isinstance(real, str) or not real:
            return real
        if real not in self.user_map:
            self.user_map[real] = f'user{len(self.user_map) + 1:03d}'
        return self.user_map[real]

    def map_account(self, real):
        if not isinstance(real, str) or not real:
            return real
        if real not in self.account_map:
            self.account_map[real] = f'acct{len(self.account_map) + 1:03d}'
        return self.account_map[real]

    def add_host(self, real):
        """Register a hostname (or FQDN) for textual scrubbing.

        Each subdomain prefix is also registered separately so e.g.
        'login-1' alone gets replaced even when seen without the FQDN tail.
        """
        if not isinstance(real, str) or not real:
            return
        if real not in self.host_map:
            self.host_map[real] = f'host{len(self.host_map) + 1:03d}'
        # Also register the bare hostname (first label) so 'login-1.example.com'
        # AND 'login-1' both get scrubbed.
        bare = real.split('.', 1)[0]
        if bare and bare != real and bare not in self.host_map:
            self.host_map[bare] = f'host{len(self.host_map) + 1:03d}'

    def map_text_token(self, real, preserve_slashes=False):
        """Same-length deterministic substitute for paths/job names/commands."""
        if not isinstance(real, str) or not real:
            return real
        if real in self.text_token_map:
            return self.text_token_map[real]
        h = hashlib.blake2b(real.encode('utf-8'), key=self._salt).hexdigest()
        token = (h * ((len(real) // len(h)) + 1))[: len(real)]
        if preserve_slashes:
            chars = list(token)
            for idx, ch in enumerate(real):
                if ch == '/' and idx < len(chars):
                    chars[idx] = '/'
            token = ''.join(chars)
        self.text_token_map[real] = token
        return token

    def _map_user_list(self, value):
        if not isinstance(value, str) or not value:
            return value
        return ','.join(self.map_user(x.strip()) for x in value.split(','))

    def _map_account_list(self, value):
        if not isinstance(value, str) or not value:
            return value
        return ','.join(self.map_account(x.strip()) for x in value.split(','))

    def _transform(self, kind, value):
        if kind == 'user':         return self.map_user(value)
        if kind == 'account':      return self.map_account(value)
        if kind == 'user_list':    return self._map_user_list(value)
        if kind == 'account_list': return self._map_account_list(value)
        if kind == 'path':         return self.map_text_token(value, preserve_slashes=True)
        if kind == 'jobname':      return self.map_text_token(value)
        if kind == 'command':      return self.map_text_token(value)
        if kind == 'token':        return self.map_text_token(value)
        return value

    # -- walkers ----------------------------------------------------------

    def walk_json(self, obj, in_job_context=False):
        if isinstance(obj, dict):
            ctx = in_job_context or 'job_id' in obj
            for key, value in list(obj.items()):
                kind = JSON_FIELD_KINDS.get(key)
                if kind is None:
                    obj[key] = self.walk_json(value, ctx)
                elif kind == 'jobname' and not ctx:
                    obj[key] = self.walk_json(value, ctx)
                elif isinstance(value, (dict, list)):
                    obj[key] = self.walk_json(value, ctx)
                else:
                    obj[key] = self._transform(kind, value)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                obj[i] = self.walk_json(item, in_job_context)
        return obj

    def scrub_text(self, text):
        """Replace every known real name with its synthetic equivalent."""
        pairs = (
            list(self.user_map.items())
            + list(self.account_map.items())
            + list(self.host_map.items())
        )
        if not pairs:
            return text
        pairs.sort(key=lambda kv: len(kv[0]), reverse=True)
        repl = dict(pairs)
        pattern = '|'.join(re.escape(real) for real, _ in pairs)
        return re.sub(r'\b(' + pattern + r')\b', lambda m: repl[m.group(0)], text)


# ---- Per-format helpers for plain-text outputs -----------------------------

def _rewrite_pipe_columns(text, anon, account_col, user_col, expect_header=False):
    """Rewrite pipe-separated rows. account_col/user_col are 0-based or None."""
    out_lines = []
    in_data = not expect_header
    for line in text.splitlines():
        if expect_header and not in_data:
            out_lines.append(line)
            if '|' in line and 'Login' in line:
                in_data = True
            continue
        if '|' not in line:
            out_lines.append(line)
            continue
        # Preserve any leading whitespace (sshare uses it for tree depth)
        leading = len(line) - len(line.lstrip())
        prefix = line[:leading]
        cells = line[leading:].split('|')
        if account_col is not None and account_col < len(cells) and cells[account_col]:
            cells[account_col] = anon.map_account(cells[account_col])
        if user_col is not None and user_col < len(cells) and cells[user_col]:
            cells[user_col] = anon.map_user(cells[user_col])
        out_lines.append(prefix + '|'.join(cells))
    return '\n'.join(out_lines) + ('\n' if text.endswith('\n') else '')


def _rewrite_sshare(text, anon):
    # Cols: Account|User|RawShares|... (--noheader)
    return _rewrite_pipe_columns(text, anon, account_col=0, user_col=1)


def _rewrite_sreport(text, anon):
    # Header: Cluster|Account|Login|Proper Name|Used|Energy
    out_lines = []
    in_data = False
    for line in text.splitlines():
        if not in_data:
            out_lines.append(line)
            if '|' in line and 'Login' in line:
                in_data = True
            continue
        if '|' not in line:
            out_lines.append(line)
            continue
        cells = line.split('|')
        if len(cells) >= 4:
            if cells[1]:
                cells[1] = anon.map_account(cells[1])
            if cells[2]:
                cells[2] = anon.map_user(cells[2])
            if cells[3]:
                cells[3] = anon.map_text_token(cells[3])
        out_lines.append('|'.join(cells))
    return '\n'.join(out_lines) + ('\n' if text.endswith('\n') else '')


def _rewrite_sprio(text, anon):
    # Whitespace-separated, no header. Cols (from format string in
    # collect_offlinedata.sh): jobid user account reason ...
    out_lines = []
    for line in text.splitlines():
        if not line.strip():
            out_lines.append(line)
            continue
        # Use a regex to preserve column widths.
        m = re.match(r'^(\s*\S+\s+)(\S+)(\s+)(\S+)(\s+.*)?$', line)
        if not m:
            out_lines.append(line)
            continue
        head, user, gap, account, tail = m.groups()
        new_user = anon.map_user(user)
        new_account = anon.map_account(account)
        # Pad to original widths so column alignment survives.
        new_user = new_user.ljust(len(user))
        new_account = new_account.ljust(len(account))
        out_lines.append(f'{head}{new_user}{gap}{new_account}{tail or ""}')
    return '\n'.join(out_lines) + ('\n' if text.endswith('\n') else '')


# ---- Collector --------------------------------------------------------------

# (filename, argv) — argv may contain {USER} placeholders.
SLURM_COMMANDS = [
    ('jobs.json',                ['scontrol', '--json', 'show', 'jobs']),
    ('nodes.json',               ['scontrol', '--json', 'show', 'nodes']),
    ('partitions.json',          ['scontrol', '--json', 'show', 'partitions']),
    ('scontrol-res.json',        ['scontrol', '--json', 'show', 'reservations']),
    ('scontrol-licenses.json',   ['scontrol', '--json', 'show', 'licenses']),
    ('scontrol-show-config.out', ['scontrol', 'show', 'config']),
    ('sdiag.json',               ['sdiag', '--json']),
    ('sprio.out',                ['sprio', '--noheader',
                                  '--format=%10i %.16u %.12o %.12r %.8n %10Y %8A %8F %8J %8P %8Q %6N %20T %8B %8S']),
    ('sshare.out',               ['sshare', '--parsable2', '--noheader', '--all', '--long',
                                  '--format=Account,User,RawShares,NormShares,RawUsage,NormUsage,EffectvUsage,LevelFS,FairShare,GrpTRESMins,TRESRunMins']),
    ('sacctmgr-qos.out',         ['sacctmgr', '--parsable2', '--noheader', 'show', 'qos',
                                  'format=Name,Priority,Flags,UsageFactor,GraceTime,MaxWall,MaxTRES,MaxJobsPU,MaxSubmitJobsPU,GrpTRES']),
    ('sacct_user_30days.json',   ['sacct', '--json', '--user={USER}', '--starttime=now-30days']),
    ('sacct_user_1week.json',    ['sacct', '--json', '--user={USER}', '--starttime=now-7days']),
    ('sacct_user_1day.json',     ['sacct', '--json', '--user={USER}', '--starttime=now-1days']),
    ('sacct_user_12hours.json',  ['sacct', '--json', '--user={USER}', '--starttime=now-12hours']),
    ('sacct_user_6hours.json',   ['sacct', '--json', '--user={USER}', '--starttime=now-6hours']),
    ('sreport_user.txt',         ['sreport', '-P', '-t', 'Hours', 'cluster',
                                  'AccountUtilizationByUser', 'User={USER}', 'Start=1970-01-01']),
]


def _collect_live(target_user, timeout=60):
    """Run all Slurm commands; return {filename: bytes} plus metadata files."""
    files = {}
    timing = []
    stderr_chunks = []

    for filename, argv in SLURM_COMMANDS:
        argv = [a.replace('{USER}', target_user) for a in argv]
        label = ' '.join(argv[:3])
        sys.stderr.write(f'  {label} -> {filename}\n')
        sys.stderr.flush()
        start = datetime.datetime.now()
        try:
            r = subprocess.run(argv, capture_output=True, timeout=timeout)
            dur = (datetime.datetime.now() - start).total_seconds()
            files[filename] = r.stdout
            timing.append(f'{label:<45} {dur:>7.3f}s  exit={r.returncode}  -> {filename}')
            if r.returncode != 0 and r.stderr:
                stderr_chunks.append(f'--- {label} (exit {r.returncode}) ---')
                stderr_chunks.append(r.stderr.decode('utf-8', errors='replace'))
        except subprocess.TimeoutExpired:
            files[filename] = b''
            timing.append(f'{label:<45} {timeout:>7.3f}s  TIMEOUT  -> {filename}')
            stderr_chunks.append(f'--- {label} TIMEOUT after {timeout}s ---')
        except FileNotFoundError as e:
            files[filename] = b''
            timing.append(f'{label:<45}        -  NOT FOUND  -> {filename}')
            stderr_chunks.append(f'--- {label} command not found: {e} ---')

    files['timing.txt'] = (
        f'Slop Bundle Collection Timing\n'
        f'Generated: {datetime.datetime.now().isoformat()}\n'
        f'Target User: {target_user}\n\n'
        + '\n'.join(timing) + '\n'
    ).encode('utf-8')

    files['_collect.stderr'] = ('\n'.join(stderr_chunks) + '\n').encode('utf-8') if stderr_chunks else b''

    files['_env.txt'] = (
        f'hostname: {socket.gethostname()}\n'
        f'date: {datetime.datetime.now().isoformat()}\n'
        f'target_user: {target_user}\n'
        f'collector: {os.environ.get("USER", "unknown")}\n'
        f'uname: {" ".join(os.uname())}\n'
        f'anonymized: true\n'
    ).encode('utf-8')

    return files


def _collect_from_dir(source_dir):
    """Read an existing collect_offlinedata.sh-style directory."""
    out = {}
    for entry in sorted(os.listdir(source_dir)):
        path = os.path.join(source_dir, entry)
        if os.path.isfile(path):
            with open(path, 'rb') as f:
                out[entry] = f.read()
    return out


# ---- Whole-bundle anonymization --------------------------------------------

# Files handled by structured text rewriters (column-aware).
STRUCTURED_TEXT = {
    'sshare.out':       _rewrite_sshare,
    'sprio.out':        _rewrite_sprio,
    'sreport_user.txt': _rewrite_sreport,
}


def _register_hosts_from_env(env_blob, anon):
    """Pre-scan _env.txt for the collector's hostname so it gets scrubbed."""
    if not env_blob:
        return
    try:
        text = env_blob.decode('utf-8')
    except UnicodeDecodeError:
        return
    for line in text.splitlines():
        if line.startswith('hostname:'):
            anon.add_host(line.split(':', 1)[1].strip())


def _anonymize_files(files):
    """In-place anonymize {filename: bytes}. Returns (files, Anonymizer)."""
    anon = Anonymizer()

    # Pre-pass: register the collector's hostname so the textual scrub catches it.
    _register_hosts_from_env(files.get('_env.txt'), anon)

    # Pass 1: any file that parses as JSON gets the structured walker.
    # This catches the standard files (jobs.json, sacct_user_*.json, etc.)
    # plus any extra .json files the source dir happens to carry.
    for name in sorted(files):
        blob = files[name]
        if not blob or not name.endswith('.json'):
            continue
        try:
            doc = json.loads(blob)
        except (json.JSONDecodeError, ValueError):
            continue
        anon.walk_json(doc)
        files[name] = (json.dumps(doc, indent=2, sort_keys=True) + '\n').encode('utf-8')

    # Pass 2: column-aware rewrite for known text formats. Populates the maps
    # with users/accounts that don't show up in JSON (e.g. sshare's full
    # fairshare tree).
    for name, fn in STRUCTURED_TEXT.items():
        blob = files.get(name)
        if not blob:
            continue
        try:
            text = blob.decode('utf-8')
        except UnicodeDecodeError:
            continue
        files[name] = fn(text, anon).encode('utf-8')

    # Pass 3: textual scrub of EVERY file (including JSON) using the
    # now-populated user/account/host maps. Catches:
    #   - hostnames in JSON fields not on our key whitelist (e.g. allocating_node)
    #   - real-name substrings inside argv lists like meta.command=[..., --user=X]
    #   - free-form leaks in README.txt, _env.txt, sacctmgr-qos.out, etc.
    # The scrub uses word boundaries and only replaces full token matches,
    # so JSON syntax stays intact.
    for name in sorted(files):
        blob = files[name]
        if not blob:
            continue
        try:
            text = blob.decode('utf-8')
        except UnicodeDecodeError:
            continue
        files[name] = anon.scrub_text(text).encode('utf-8')

    files['ANONYMIZED.txt'] = (
        f'Slop Anonymized Debug Bundle\n'
        f'Users replaced: {len(anon.user_map)}\n'
        f'Accounts replaced: {len(anon.account_map)}\n'
        f'Hostnames replaced: {len(anon.host_map)}\n'
        f'Path/jobname/command/group tokens: {len(anon.text_token_map)}\n'
        f'\n'
        f'No inverse mapping is included. The mapping is one-way.\n'
    ).encode('utf-8')

    return files, anon


# ---- Tarball read/write -----------------------------------------------------

BUNDLE_ROOT = 'slop-debug-bundle'


def _write_bundle(files, output_path):
    """Write {filename: bytes} as a deterministic .tar.gz at output_path.

    Uses a fixed in-tar root directory so the bundle bytes do not depend on
    the chosen output filename — two exports of identical input produce a
    byte-identical tarball.
    """
    bundle_root = BUNDLE_ROOT
    # Deterministic gzip header (mtime=0) + per-entry mtime=0 → byte-identical
    # tarballs from identical input.
    with open(output_path, 'wb') as raw:
        # filename='' suppresses the FNAME header so identical input produces
        # byte-identical tarballs regardless of the chosen output filename.
        with gzip.GzipFile(filename='', fileobj=raw, mode='wb', mtime=0, compresslevel=9) as gz:
            with tarfile.open(fileobj=gz, mode='w|') as tar:
                for name in sorted(files):
                    data = files[name]
                    info = tarfile.TarInfo(name=f'{bundle_root}/{name}')
                    info.size = len(data)
                    info.mtime = 0
                    info.mode = 0o644
                    info.uid = 0
                    info.gid = 0
                    info.uname = ''
                    info.gname = ''
                    tar.addfile(info, io.BytesIO(data))
    return output_path


_SAFE_TAR_TYPES = {tarfile.REGTYPE, tarfile.AREGTYPE, tarfile.DIRTYPE}


def extract_bundle(tar_path, dest_dir=None):
    """Extract a .tar.gz into dest_dir (a fresh tempdir if None).

    Rejects path-traversal and symlink/special members. Returns the path of
    the extracted top-level directory (the bundle root).
    """
    if dest_dir is None:
        dest_dir = tempfile.mkdtemp(prefix='slop-import-')
    dest = Path(dest_dir).resolve()
    with tarfile.open(tar_path, 'r:gz') as tar:
        members = tar.getmembers()
        for m in members:
            if m.type not in _SAFE_TAR_TYPES:
                raise ValueError(f'unsafe tarball entry type: {m.name!r}')
            target = (dest / m.name).resolve()
            if target != dest and not str(target).startswith(str(dest) + os.sep):
                raise ValueError(f'unsafe tarball entry path: {m.name!r}')
        # Python 3.12+ added a `filter` argument to harden extraction;
        # older versions emit a DeprecationWarning if it's missing. Use it
        # when available.
        extract_kwargs = {}
        if 'filter' in tarfile.TarFile.extract.__code__.co_varnames:
            extract_kwargs['filter'] = 'data'
        for m in members:
            tar.extract(m, dest, **extract_kwargs)
    entries = [p for p in dest.iterdir() if not p.name.startswith('.')]
    if len(entries) == 1 and entries[0].is_dir():
        return str(entries[0])
    return str(dest)


# ---- Public export entry point ---------------------------------------------

def export_bundle(source_dir=None, output_path=None):
    """Collect (or load) data, anonymize, and write a .tar.gz.

    source_dir: if given, anonymize this existing collect_offlinedata.sh-style
                directory; otherwise run the slurm commands locally.
    output_path: target file. Defaults to slop-debug-bundle-<ts>.tar.gz in CWD.

    Returns (output_path, Anonymizer).
    """
    if output_path is None:
        ts = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')
        output_path = f'slop-debug-bundle-{ts}.tar.gz'
    target_user = os.environ.get('USER', 'unknown')
    if source_dir:
        files = _collect_from_dir(source_dir)
    else:
        files = _collect_live(target_user)
    files, anon = _anonymize_files(files)
    _write_bundle(files, output_path)
    return output_path, anon


__all__ = ['Anonymizer', 'export_bundle', 'extract_bundle']
