import json
import threading
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import server


class StubUpstreamHandler(BaseHTTPRequestHandler):
    last_request = None

    def do_POST(self):
        content_length = int(self.headers["Content-Length"])
        body = self.rfile.read(content_length).decode("utf-8")
        StubUpstreamHandler.last_request = {
            "path": self.path,
            "headers": dict(self.headers),
            "body": json.loads(body),
        }

        if StubUpstreamHandler.last_request["body"].get("stream") is True:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            events = [
                {
                    "type": "response.created",
                    "response": {
                        "id": "resp_stream",
                        "created_at": 1710000001,
                        "model": "gpt-test",
                    },
                },
                {"type": "response.output_text.delta", "delta": "proxy "},
                {"type": "response.output_text.delta", "delta": "stream"},
                {
                    "type": "response.completed",
                    "response": {
                        "id": "resp_stream",
                        "created_at": 1710000001,
                        "model": "gpt-test",
                        "status": "completed",
                        "output": [
                            {
                                "type": "message",
                                "role": "assistant",
                                "content": [{"type": "output_text", "text": "proxy stream"}],
                            }
                        ],
                        "usage": {"input_tokens": 9, "output_tokens": 2, "total_tokens": 11},
                    },
                },
            ]
            for event in events:
                self.wfile.write(f"data: {json.dumps(event)}\n\n".encode("utf-8"))
            self.wfile.flush()
            return

        response = {
            "id": "resp_test",
            "object": "response",
            "created_at": 1710000000,
            "model": "gpt-test",
            "status": "completed",
            "output": [
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "proxy works"}],
                }
            ],
            "usage": {"input_tokens": 12, "output_tokens": 5, "total_tokens": 17},
        }
        encoded = json.dumps(response).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format, *args):
        return


def serve_in_thread(handler_cls):
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd, thread


class ProxyTests(unittest.TestCase):
    def test_responses_stream_events_to_chat_chunks(self):
        events = [
            (
                None,
                json.dumps(
                    {
                        "type": "response.created",
                        "response": {"id": "resp_1", "created_at": 1710000002, "model": "gpt-4.1"},
                    }
                ),
            ),
            (None, json.dumps({"type": "response.output_text.delta", "delta": "hello"})),
            (
                None,
                json.dumps(
                    {
                        "type": "response.completed",
                        "response": {
                            "id": "resp_1",
                            "created_at": 1710000002,
                            "model": "gpt-4.1",
                            "status": "completed",
                            "output": [
                                {
                                    "type": "message",
                                    "role": "assistant",
                                    "content": [{"type": "output_text", "text": "hello"}],
                                }
                            ],
                            "usage": {"input_tokens": 2, "output_tokens": 1, "total_tokens": 3},
                        },
                    }
                ),
            ),
        ]

        chunks = list(server.responses_stream_events_to_chat_chunks(events, include_usage=True))
        self.assertEqual(chunks[0]["choices"][0]["delta"]["role"], "assistant")
        self.assertEqual(chunks[1]["choices"][0]["delta"]["content"], "hello")
        self.assertEqual(chunks[2]["choices"][0]["finish_reason"], "stop")
        self.assertEqual(chunks[3]["usage"]["total_tokens"], 3)

    def test_chat_request_to_responses_payload(self):
        payload = server.chat_request_to_responses_payload(
            {
                "model": "gpt-4.1",
                "messages": [
                    {"role": "system", "content": "You are helpful."},
                    {
                        "role": "user",
                        "content": [{"type": "text", "text": "hello"}],
                    },
                ],
                "max_tokens": 123,
                "temperature": 0.2,
            }
        )
        self.assertEqual(payload["model"], "gpt-4.1")
        self.assertEqual(payload["max_output_tokens"], 123)
        self.assertEqual(payload["temperature"], 0.2)
        self.assertEqual(payload["input"][0]["content"], "You are helpful.")
        self.assertEqual(payload["input"][0]["role"], "developer")
        self.assertEqual(payload["input"][1]["content"][0]["type"], "input_text")

    def test_responses_to_chat_completion(self):
        chat_response = server.responses_to_chat_completion(
            {
                "id": "resp_123",
                "created_at": 1710000000,
                "model": "gpt-4.1",
                "status": "completed",
                "output": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [
                            {"type": "output_text", "text": "hello"},
                            {"type": "output_text", "text": " world"},
                        ],
                    }
                ],
                "usage": {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12},
            }
        )
        self.assertEqual(chat_response["object"], "chat.completion")
        self.assertEqual(chat_response["choices"][0]["message"]["content"], "hello world")
        self.assertEqual(chat_response["usage"]["prompt_tokens"], 10)

    def test_end_to_end_proxy_route(self):
        upstream_server, _ = serve_in_thread(StubUpstreamHandler)
        proxy_server, _ = serve_in_thread(server.ChatForwardHandler)
        original_provider = server.PROVIDERS["codex-for-me"].copy()
        server.PROVIDERS["codex-for-me"]["base_url"] = f"http://127.0.0.1:{upstream_server.server_address[1]}"

        try:
            request_body = {
                "model": "gpt-4.1",
                "messages": [{"role": "user", "content": "Say hi"}],
            }
            request = urllib.request.Request(
                f"http://127.0.0.1:{proxy_server.server_address[1]}/codex-for-me/v1/chat/completions",
                data=json.dumps(request_body).encode("utf-8"),
                headers={"Content-Type": "application/json", "Authorization": "Bearer sk-test"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                body = json.loads(response.read().decode("utf-8"))

            self.assertEqual(body["choices"][0]["message"]["content"], "proxy stream")
            self.assertEqual(StubUpstreamHandler.last_request["path"], "/responses")
            self.assertEqual(
                StubUpstreamHandler.last_request["body"]["input"][0]["content"],
                "Say hi",
            )
            self.assertTrue(StubUpstreamHandler.last_request["body"]["stream"])
            self.assertEqual(
                StubUpstreamHandler.last_request["headers"]["Authorization"],
                "Bearer sk-test",
            )
        finally:
            server.PROVIDERS["codex-for-me"] = original_provider
            proxy_server.shutdown()
            upstream_server.shutdown()
            proxy_server.server_close()
            upstream_server.server_close()

    def test_missing_authorization_returns_401(self):
        proxy_server, _ = serve_in_thread(server.ChatForwardHandler)

        try:
            request_body = {
                "model": "gpt-4.1",
                "messages": [{"role": "user", "content": "Say hi"}],
            }
            request = urllib.request.Request(
                f"http://127.0.0.1:{proxy_server.server_address[1]}/codex-for-me/v1/chat/completions",
                data=json.dumps(request_body).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                urllib.request.urlopen(request, timeout=5)

            body = json.loads(ctx.exception.read().decode("utf-8"))
            self.assertEqual(ctx.exception.code, 401)
            self.assertIn("Missing upstream API key", body["error"]["message"])
        finally:
            proxy_server.shutdown()
            proxy_server.server_close()

    def test_end_to_end_stream_proxy_route(self):
        upstream_server, _ = serve_in_thread(StubUpstreamHandler)
        proxy_server, _ = serve_in_thread(server.ChatForwardHandler)
        original_provider = server.PROVIDERS["codex-for-me"].copy()
        server.PROVIDERS["codex-for-me"]["base_url"] = f"http://127.0.0.1:{upstream_server.server_address[1]}"

        try:
            request_body = {
                "model": "gpt-4.1",
                "messages": [{"role": "user", "content": "Say hi"}],
                "stream": True,
                "stream_options": {"include_usage": True},
            }
            request = urllib.request.Request(
                f"http://127.0.0.1:{proxy_server.server_address[1]}/codex-for-me/v1/chat/completions",
                data=json.dumps(request_body).encode("utf-8"),
                headers={"Content-Type": "application/json", "Authorization": "Bearer sk-test"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                body = response.read().decode("utf-8")

            frames = [
                line[len("data: ") :]
                for line in body.splitlines()
                if line.startswith("data: ")
            ]
            chunks = [json.loads(frame) for frame in frames[:-1]]

            self.assertEqual(frames[-1], "[DONE]")
            self.assertEqual(chunks[0]["choices"][0]["delta"]["role"], "assistant")
            self.assertEqual(chunks[1]["choices"][0]["delta"]["content"], "proxy ")
            self.assertEqual(chunks[2]["choices"][0]["delta"]["content"], "stream")
            self.assertEqual(chunks[3]["choices"][0]["finish_reason"], "stop")
            self.assertEqual(chunks[4]["usage"]["total_tokens"], 11)
            self.assertTrue(StubUpstreamHandler.last_request["body"]["stream"])
        finally:
            server.PROVIDERS["codex-for-me"] = original_provider
            proxy_server.shutdown()
            upstream_server.shutdown()
            proxy_server.server_close()
            upstream_server.server_close()


if __name__ == "__main__":
    unittest.main()
