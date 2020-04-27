"""Set of Proxy Log Classes for various log types."""
import re
import os
from distro_info import UbuntuDistroInfo


class ISO:
    """Generic squid proxy log entry.

    - - [10/Mar/2019:08:06:30 +0000]
    "HEAD /ubuntu/releases/bionic/release/ubuntu-18.04.2-server-amd64.iso
    HTTP/1.1" 200 258 "-" "Packer/1.3.5 (+https://www.packer.io/; go1.12;
    linux/amd64)"
    """

    def __init__(self, entry):
        """Collect standard data in proxy log."""
        self.status = None
        self.valid = False

        self._parse_entry(entry)
        if self.valid:
            file = os.path.basename(self.target)
            codename = file.split('-')[0]
            all_dists = UbuntuDistroInfo().get_all()
            if codename in all_dists:
                self.valid = False
                return

            self.release = '.'.join(file.split('-')[1].split('.')[0:2])
            self.flavor = file.split('-')[2]
            self.arch = file.split('-')[3].split('.')[0]

            # special handling of 'live-server'
            if self.flavor == 'live':
                self.flavor = '%s-%s' % (
                    file.split('-')[2], file.split('-')[3]
                )
                self.arch = file.split('-')[4].split('.')[0]

    def __str__(self):
        """Print out object similar to proxy log would."""
        return ('[%s] %s' % (self.status, self.target))

    def _parse_entry(self, entry):
        """Parse a log entry into its parts."""
        self.status = int(re.findall(r'\s(\d{1,3})\s', entry)[0])

        try:
            self.target = re.findall(r'\"(.+?)\"', entry)[0].split(' ')[1]
        except IndexError:
            self.target = ''

        if (self.target.endswith('iso') or self.target.endswith ('img.xz') \
            or self.target.endswith('img.gz')) and self.status == 200:
            self.valid = True

    def csv(self):
        """Return entry as CSV."""
        return '%s %s' % (self.status, self.target)

    def json(self):
        """Return entry as dictionary for JSON."""
        return {
            "status": self.status,
            "target": self.target,
        }
