import threading
from flask import Flask
import os

class HealthServer:

    def __init__(self):
        self.t = threading.Thread(target=self.run)
        self.t.setDaemon(True)
        self.t.start()

    def run(self):
        app = Flask(__name__)

        @app.route('/health', methods=['GET'])
        def healthcheck():
            return ('', 204)

        app.run(debug=True, use_reloader=False, host='0.0.0.0', port=os.environ['HEALTHCHECK_PORT'])

    def stop(self):
        exit(0)
