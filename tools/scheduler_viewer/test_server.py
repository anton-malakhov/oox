import http.client
import threading
import unittest

from run import ASSETS, ROOT, ThreadingHTTPServer, ViewerHandler


class ServerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), ViewerHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=3)

    def request(self, path, method="GET"):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=3)
        try:
            connection.request(method, path)
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    def test_assets(self):
        for path, (filename, mime) in ASSETS.items():
            with self.subTest(path=path):
                status, headers, body = self.request(path)
                self.assertEqual(status, 200)
                self.assertEqual(headers["Content-Type"], mime)
                self.assertEqual(body, (ROOT / filename).read_bytes())
                self.assertEqual(headers["Cache-Control"], "no-store")
                self.assertIn("connect-src 'none'", headers["Content-Security-Policy"])

    def test_head_and_query(self):
        status, headers, body = self.request("/app.js?version=1", "HEAD")
        self.assertEqual(status, 200)
        self.assertEqual(body, b"")
        self.assertEqual(int(headers["Content-Length"]), (ROOT / "app.js").stat().st_size)

    def test_repository_and_traversal_not_served(self):
        for path in ["/../CMakeLists.txt", "/%2e%2e/CMakeLists.txt", "/.git/config", "/run.py", "/README.md", "/missing", "/index.html/extra"]:
            with self.subTest(path=path):
                self.assertEqual(self.request(path)[0], 404)

    def test_embedded_trace_is_not_executable_html(self):
        self.server.trace_records = [{"mode": "</script><script>alert(1)</script>"}]
        try:
            status, _, body = self.request("/trace.html")
            self.assertEqual(status, 200)
            self.assertNotIn(b"<script>alert(1)</script>", body)
            self.assertIn(b"\\u003c/script\\u003e", body)
        finally:
            self.server.trace_records = []


if __name__ == "__main__":
    unittest.main()
