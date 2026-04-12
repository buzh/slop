"""Anonymization for demo/recording purposes."""

import hashlib
import os


def _is_demo_mode():
    """Check if demo mode is enabled (checks env var each time)."""
    return os.getenv('SLOP_DEMO_MODE', '').lower() in ('1', 'true', 'yes')


class Anonymizer:
    """Anonymize sensitive data for demos/screenshots."""

    # Common demo usernames (sorted alphabetically)
    DEMO_USERS = [
        'alice', 'bob', 'charlie', 'diana', 'eve', 'frank', 'grace', 'henry',
        'iris', 'jack', 'karen', 'liam', 'maya', 'noah', 'olivia', 'paul',
        'quinn', 'rachel', 'sam', 'tina', 'uma', 'victor', 'wendy', 'xander',
        'yuki', 'zoe', 'alex', 'blake', 'casey', 'drew'
    ]

    # Common demo project/account names
    DEMO_ACCOUNTS = [
        'physics', 'chemistry', 'biology', 'astronomy', 'geology',
        'ml-research', 'ai-lab', 'data-science', 'hpc-team', 'climate',
        'genomics', 'neuroscience', 'materials', 'energy', 'quantum',
        'particle-phys', 'computational', 'bioinformatics', 'simulation',
        'research-group'
    ]

    def __init__(self):
        self.user_map = {}
        self.account_map = {}
        self.node_map = {}
        self.user_counter = 0
        self.account_counter = 0
        self.node_counter = 0
        # Reverse maps for demo mode search
        self.reverse_user_map = {}
        self.reverse_account_map = {}

    def _hash_to_int(self, text, modulo):
        """Generate consistent integer from text hash."""
        h = hashlib.md5(text.encode()).hexdigest()
        return int(h[:8], 16) % modulo

    def anonymize_user(self, username):
        """Replace username with anonymous version."""
        if not _is_demo_mode() or not username:
            return username

        if username not in self.user_map:
            # Map to a demo username using consistent hashing
            user_idx = self._hash_to_int(username, len(self.DEMO_USERS))
            demo_name = self.DEMO_USERS[user_idx]
            self.user_map[username] = demo_name
            # Also store reverse mapping for search
            self.reverse_user_map[demo_name] = username

        return self.user_map[username]

    def anonymize_account(self, account):
        """Replace account name with anonymous version."""
        if not _is_demo_mode() or not account:
            return account

        if account not in self.account_map:
            # Map to a demo account using consistent hashing
            acc_idx = self._hash_to_int(account, len(self.DEMO_ACCOUNTS))
            demo_name = self.DEMO_ACCOUNTS[acc_idx]
            self.account_map[account] = demo_name
            # Also store reverse mapping for search
            self.reverse_account_map[demo_name] = account

        return self.account_map[account]

    def anonymize_node(self, nodename):
        """Replace node name with anonymous version."""
        if not _is_demo_mode() or not nodename:
            return nodename

        # Skip N/A and empty
        if nodename.lower() in ('n/a', 'none', ''):
            return nodename

        if nodename not in self.node_map:
            # Try to preserve node type prefix (gpu, node, compute, etc.)
            if 'gpu' in nodename.lower():
                prefix = 'gpu'
            elif 'compute' in nodename.lower():
                prefix = 'compute'
            else:
                prefix = 'node'

            node_id = self._hash_to_int(nodename, 100)
            self.node_map[nodename] = f"{prefix}{node_id:03d}"

        return self.node_map[nodename]

    def anonymize_job_name(self, jobname):
        """Replace job name with generic version."""
        if not _is_demo_mode() or not jobname:
            return jobname

        # Keep common patterns but anonymize
        if 'bash' in jobname.lower():
            return 'bash'
        elif 'python' in jobname.lower():
            return 'python_job'
        elif 'interactive' in jobname.lower():
            return 'interactive'
        elif jobname.startswith('job_'):
            return jobname  # Already generic
        else:
            # Hash-based generic name
            job_id = self._hash_to_int(jobname, 1000)
            return f"job_{job_id:04d}"

    def anonymize_path(self, path):
        """Replace file paths with generic versions.

        Simple approach: Replace any occurrence of known real usernames
        in the path with their anonymized versions.
        """
        if not _is_demo_mode() or not path:
            return path

        # Replace all known real usernames with their anonymized versions
        anonymized_path = path
        for real_username, demo_username in self.user_map.items():
            # Replace username as a path component (surrounded by /)
            anonymized_path = anonymized_path.replace(f'/{real_username}/', f'/{demo_username}/')
            # Replace username at end of path
            if anonymized_path.endswith(f'/{real_username}'):
                anonymized_path = anonymized_path[:-len(real_username)] + demo_username

        # Also replace known account names
        for real_account, demo_account in self.account_map.items():
            anonymized_path = anonymized_path.replace(f'/{real_account}/', f'/{demo_account}/')
            if anonymized_path.endswith(f'/{real_account}'):
                anonymized_path = anonymized_path[:-len(real_account)] + demo_account

        return anonymized_path


# Global instance
_anonymizer = Anonymizer()


def anonymize_user(username):
    """Anonymize username if demo mode is enabled."""
    return _anonymizer.anonymize_user(username)


def anonymize_account(account):
    """Anonymize account name if demo mode is enabled."""
    return _anonymizer.anonymize_account(account)


def anonymize_node(nodename):
    """Anonymize node name if demo mode is enabled."""
    return _anonymizer.anonymize_node(nodename)


def anonymize_job_name(jobname):
    """Anonymize job name if demo mode is enabled."""
    return _anonymizer.anonymize_job_name(jobname)


def anonymize_path(path):
    """Anonymize file path if demo mode is enabled."""
    return _anonymizer.anonymize_path(path)


def is_demo_mode():
    """Check if demo mode is enabled."""
    return _is_demo_mode()


def deanonymize_user(demo_name):
    """Get real username from demo name (for search functionality).

    Returns the real username if demo_name is a known demo name,
    otherwise returns demo_name unchanged.
    """
    if not _is_demo_mode():
        return demo_name
    return _anonymizer.reverse_user_map.get(demo_name, demo_name)


def deanonymize_account(demo_name):
    """Get real account name from demo name (for search functionality).

    Returns the real account if demo_name is a known demo name,
    otherwise returns demo_name unchanged.
    """
    if not _is_demo_mode():
        return demo_name
    return _anonymizer.reverse_account_map.get(demo_name, demo_name)
