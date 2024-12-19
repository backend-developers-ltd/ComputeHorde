import os
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer


class StreamingJobHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/execute-job":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            # Mock job finish after endpoint was hit
            time.sleep(2)
            os._exit(0)
        elif self.path == "/health":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Not Found")


class AutoStartStreamingJobHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            # Mock job finishing right after docker ready
            time.sleep(2)
            os._exit(0)
        else:
            self.send_response(404)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Not Found")


if __name__ == "__main__":
    autostart = True if len(sys.argv) > 1 and sys.argv[1] == "autostart" else False
    httpd = HTTPServer(
        ("0.0.0.0", 8000), AutoStartStreamingJobHandler if autostart else StreamingJobHandler
    )
    httpd.serve_forever()
