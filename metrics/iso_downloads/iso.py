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
        if not self.valid:
            return

        file = os.path.basename(self.target)
        codename = file.split('-')[0]
        all_dists = UbuntuDistroInfo().get_all()
        if codename in all_dists:
            self.valid = False
            return

        re_match = re.match(r'^(\S*)-([\d.]*)-([^.]*)\.', file)
        if not re_match:
            # a few oddball images we don't care about, like
            # 14.09-factory-preinstalled-system-armel+manta.img
            self.valid = False
            return
        self.product = re_match[1]
        self.release = re_match[2]
        try:
            self.flavor,self.arch = re_match[3].rsplit('-', maxsplit=1)
        except:
            self.flavor = ''
            self.arch = re_match[3]

        # extract a subarch if any
        try:
            self.arch, self.subarch = self.arch.split('+')
        except:
            self.subarch = ''

        ua_match = re.match(r'.*"([^#]*)"$', entry)
        if ua_match[1] == 'Mozilla/5.0' and self.subarch.startswith('raspi'):
            self.ua = 'raspi-imager'
        else:
            self.ua = ''

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

        # if there is a query string thanks to download.u.c, strip it off.
        self.target = self.target.split('?')[0]

        if (self.target.endswith('.iso') or self.target.endswith('.img.xz') \
            or self.target.endswith('.img.gz') or self.target.endswith('.img')) \
           and self.status == 200:
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
