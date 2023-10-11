import os
import subprocess
import dotenv

dotenv.load_dotenv()

KEYS = [
    'ADMIN_PANEL_PASSWORD',
    'HANDLE',
    'PASSWORD',
    'PERSONAL_HANDLE',
    'PERSONAL_PASSWORD',
    'HOSTNAME'
]

args = [f'{i}={os.environ[i]}' for i in KEYS]
subprocess.run(['heroku', 'config:set', *args])
