#!c:\migration\python\python.exe
# EASY-INSTALL-ENTRY-SCRIPT: 'dateutils==0.6.6','console_scripts','datediff'
__requires__ = 'dateutils==0.6.6'
import re
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(
        load_entry_point('dateutils==0.6.6', 'console_scripts', 'datediff')()
    )
