"""Fetch cluster utilization statistics using sreport command."""

import subprocess
from datetime import timedelta

__all__ = ["SreportFetcher"]


class SreportFetcher:
    """Fetch user and account utilization data using sreport command."""

    def __init__(self):
        self.timeout = 30
        self.last_fetch_duration = timedelta(0)

    def fetch_user_utilization(self, username, start_date='1970-01-01', end_date='now'):
        """Fetch account utilization by user.

        Args:
            username: Username to query
            start_date: Start date (YYYY-MM-DD or 'now-Xdays')
            end_date: End date (YYYY-MM-DD or 'now')

        Returns:
            list: List of dicts with keys: login, account, used (hours)
                  Returns None on error
        """
        cmd = [
            "sreport", "cluster", "AccountUtilizationByUser",
            "-T", "billing",
            f"user={username}",
            "-t", "Hours",
            f"start={start_date}",
            f"end={end_date}",
            "format=Login%20,Accounts,Used",
            "--parsable"
        ]

        return self._run_sreport(cmd)

    def fetch_account_utilization(self, account, start_date='1970-01-01', end_date='now'):
        """Fetch user utilization by account.

        Args:
            account: Account name to query
            start_date: Start date (YYYY-MM-DD or 'now-Xdays')
            end_date: End date (YYYY-MM-DD or 'now')

        Returns:
            list: List of dicts with keys: login, account, used (hours)
                  Returns None on error
        """
        cmd = [
            "sreport", "cluster", "AccountUtilizationByUser",
            "-T", "billing",
            f"account={account}",
            "-t", "Hours",
            f"start={start_date}",
            f"end={end_date}",
            "format=Login%20,Accounts,Used",
            "--parsable"
        ]

        return self._run_sreport(cmd)

    def _run_sreport(self, cmd):
        """Run sreport command and parse output.

        Args:
            cmd: Command list to execute

        Returns:
            list: List of dicts with parsed data, or None on error
        """
        import time
        start_time = time.time()

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=self.timeout,
                text=True
            )

            self.last_fetch_duration = timedelta(seconds=time.time() - start_time)

            if result.returncode != 0:
                return None

            # Parse the output
            lines = result.stdout.strip().split('\n')
            data = []

            # Find the header line (contains '|')
            header_idx = -1
            for idx, line in enumerate(lines):
                if '|' in line and 'Login' in line:
                    header_idx = idx
                    break

            if header_idx == -1:
                return None

            # Parse header
            header = [field.strip().lower() for field in lines[header_idx].split('|') if field.strip()]

            # Parse data rows
            for line in lines[header_idx + 1:]:
                if not line.strip() or '---' in line:
                    continue

                fields = [field.strip() for field in line.split('|') if field.strip()]
                if len(fields) != len(header):
                    continue

                row = {}
                for key, value in zip(header, fields):
                    # Convert numeric fields
                    if key == 'used':
                        try:
                            row[key] = int(value)
                        except ValueError:
                            row[key] = 0
                    else:
                        row[key] = value

                data.append(row)

            return data

        except subprocess.TimeoutExpired:
            self.last_fetch_duration = timedelta(seconds=self.timeout)
            return None
        except Exception:
            return None
