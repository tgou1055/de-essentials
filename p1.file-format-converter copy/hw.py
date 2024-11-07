"""
Environment Variables
"""

import sys
import os
import json
args = sys.argv
#arg = args[1]
#prog_file = args[0]


json.loads(args[1])
print(args[1])

#print(f'{arg} from {prog_file}')


host = os.environ.get('HOST')

print(f'Connecting to {host}')
