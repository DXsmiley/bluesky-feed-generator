import server.monkeypatch

import os
from server.app import create_and_run_webapp

if __name__ == '__main__':
    # FOR DEBUG PURPOSE ONLY
    port = int(os.environ.get('PORT', 8000))
    create_and_run_webapp(port=port)
