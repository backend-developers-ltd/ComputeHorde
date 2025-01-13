import os
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer


class StreamingJobHandler(BaseHTTPRequestHandler):
    def _set_response(self, code: int, msg: bytes = b""):
        self.send_response(code)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(msg)

    def do_GET(self):
        if self.path == "/execute-job":
            self._set_response(200, b"OK")
        elif self.path == "/health":
            self._set_response(200, b"OK")
        elif self.path == "/terminate":
            self._set_response(200, b"OK")
            time.sleep(2)
            os._exit(0)
        else:
            self._set_response(404, b"Not Found")


class AutoStartStreamingJobHandler(BaseHTTPRequestHandler):
    def _set_response(self, code: int, msg: bytes = b""):
        self.send_response(code)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(msg)

    def do_GET(self):
        if self.path == "/health":
            self._set_response(200, b"OK")
            # Mock job finishing right after docker ready
            time.sleep(2)
            os._exit(0)
        else:
            self._set_response(404, b"Not Found")


if __name__ == "__main__":
    autostart = True if len(sys.argv) > 1 and sys.argv[1] == "autostart" else False
    httpd = HTTPServer(
        ("0.0.0.0", 8000), AutoStartStreamingJobHandler if autostart else StreamingJobHandler
    )
    httpd.serve_forever()
