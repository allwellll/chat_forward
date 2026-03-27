#!/usr/bin/env python3
import json
import os
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from itertools import count
from typing import Any
from urllib.parse import urlparse

import requests

PROVIDERS = {
    "codex-for-me": {
        "name": "codex-for-me",
        "base_url": "https://api-mobile.codex-for.me/v1",
        "wire_api": "responses",
        "requires_openai_auth": True,
    },
    "right": {
        "name": "right",
        "base_url": "https://right.codes/codex/v1",
        "wire_api": "responses",
        "requires_openai_auth": True,
    },
    "fox": {
        "name": "fox",
        "base_url": "https://code.newcli.com/codex/v1",
        "wire_api": "responses",
        "requires_openai_auth": True,
    },
}

FORWARDED_FIELDS = (
    "temperature",
    "top_p",
    "tools",
    "tool_choice",
    "parallel_tool_calls",
    "metadata",
    "store",
    "reasoning",
    "text",
    "truncation",
)
UNSET = object()


class RequestError(Exception):
    def __init__(self, status: int, message: str, error_type: str = "invalid_request_error"):
        super().__init__(message)
        self.status = status
        self.message = message
        self.error_type = error_type


class ChatForwardHTTPServer(ThreadingHTTPServer):
    request_queue_size = 20


def extract_bearer_token(headers: dict[str, str]) -> str | None:
    auth = headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()

    api_key = headers.get("X-API-Key") or headers.get("x-api-key")
    if api_key:
        return api_key.strip()
    return None


def normalize_chat_message(message: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(message, dict):
        raise RequestError(400, "Each message must be an object.")

    role = message.get("role")
    if role not in {"system", "developer", "user", "assistant"}:
        raise RequestError(400, f"Unsupported message role: {role!r}")

    # Some upstream Responses-compatible providers reject `system` messages
    # but accept `developer` with equivalent intent.
    if role == "system":
        role = "developer"

    if "tool_calls" in message:
        raise RequestError(400, "tool_calls input is not supported by this proxy.")

    content = message.get("content")
    return {
        "type": "message",
        "role": role,
        "content": normalize_message_content(content),
    }


def normalize_message_content(content: Any) -> str | list[dict[str, Any]]:
    if content is None:
        return ""

    if isinstance(content, str):
        return content

    if not isinstance(content, list):
        raise RequestError(400, "Message content must be a string or an array.")

    normalized = []
    for part in content:
        if not isinstance(part, dict):
            raise RequestError(400, "Each content part must be an object.")

        part_type = part.get("type")
        if part_type == "text":
            normalized.append({"type": "input_text", "text": part.get("text", "")})
            continue

        if part_type == "image_url":
            image = part.get("image_url")
            if isinstance(image, dict):
                image_url = image.get("url")
                detail = image.get("detail", "auto")
            else:
                image_url = image
                detail = "auto"

            if not image_url:
                raise RequestError(400, "image_url content parts must include a URL.")

            normalized.append(
                {
                    "type": "input_image",
                    "image_url": image_url,
                    "detail": detail,
                }
            )
            continue

        raise RequestError(400, f"Unsupported content part type: {part_type!r}")

    return normalized


def chat_request_to_responses_payload(chat_request: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(chat_request, dict):
        raise RequestError(400, "Request body must be a JSON object.")

    messages = chat_request.get("messages")
    if not isinstance(messages, list) or not messages:
        raise RequestError(400, "`messages` must be a non-empty array.")

    model = chat_request.get("model")
    if not isinstance(model, str) or not model.strip():
        raise RequestError(400, "`model` must be a non-empty string.")

    if "n" in chat_request and chat_request["n"] not in (None, 1):
        raise RequestError(400, "This proxy only supports `n = 1`.")

    payload: dict[str, Any] = {
        "model": model,
        "input": [normalize_chat_message(message) for message in messages],
    }

    max_tokens = chat_request.get("max_completion_tokens", chat_request.get("max_tokens"))
    if max_tokens is not None:
        payload["max_output_tokens"] = max_tokens

    for field in FORWARDED_FIELDS:
        if field in chat_request:
            payload[field] = chat_request[field]

    return payload


def extract_assistant_message(response_payload: dict[str, Any]) -> tuple[dict[str, Any], str]:
    text_parts: list[str] = []
    refusals: list[str] = []
    tool_calls: list[dict[str, Any]] = []

    for item in response_payload.get("output", []):
        item_type = item.get("type")

        if item_type == "message" and item.get("role") == "assistant":
            for content in item.get("content", []):
                content_type = content.get("type")
                if content_type == "output_text":
                    text_parts.append(content.get("text", ""))
                elif content_type == "refusal":
                    refusals.append(content.get("refusal", ""))
            continue

        if item_type == "function_call":
            arguments = item.get("arguments", "")
            if not isinstance(arguments, str):
                arguments = json.dumps(arguments, ensure_ascii=False)
            tool_calls.append(
                {
                    "id": item.get("call_id") or item.get("id") or f"call_{len(tool_calls) + 1}",
                    "type": "function",
                    "function": {
                        "name": item.get("name", ""),
                        "arguments": arguments,
                    },
                }
            )

    message: dict[str, Any] = {"role": "assistant"}
    if tool_calls:
        message["content"] = None
        message["tool_calls"] = tool_calls
        finish_reason = "tool_calls"
    else:
        message["content"] = "".join(text_parts)
        finish_reason = "stop"

    if refusals:
        message["refusal"] = "\n".join(refusals)
        if not message.get("content"):
            message["content"] = None

    if response_payload.get("status") == "incomplete":
        finish_reason = "length"

    return message, finish_reason


def responses_to_chat_completion(response_payload: dict[str, Any]) -> dict[str, Any]:
    message, finish_reason = extract_assistant_message(response_payload)
    usage = response_payload.get("usage") or {}

    return {
        "id": response_payload.get("id", f"chatcmpl-proxy-{int(time.time())}"),
        "object": "chat.completion",
        "created": response_payload.get("created_at", int(time.time())),
        "model": response_payload.get("model"),
        "system_fingerprint": "",
        "choices": [
            {
                "index": 0,
                "message": message,
                "finish_reason": finish_reason,
            }
        ],
        "usage": {
            "prompt_tokens": usage.get("input_tokens", 0),
            "completion_tokens": usage.get("output_tokens", 0),
            "total_tokens": usage.get("total_tokens", 0),
        },
    }


def error_body(message: str, error_type: str = "invalid_request_error") -> dict[str, Any]:
    return {"error": {"message": message, "type": error_type}}


def sse_frame(data: str) -> bytes:
    lines = data.splitlines() or [""]
    payload = "".join(f"data: {line}\n" for line in lines) + "\n"
    return payload.encode("utf-8")


def iter_sse_events(response: Any):
    event_name = None
    data_lines: list[str] = []

    while True:
        raw_line = response.readline()
        if not raw_line:
            break

        line = raw_line.decode("utf-8", errors="replace")
        stripped = line.rstrip("\r\n")

        if stripped == "":
            if data_lines:
                yield event_name, "\n".join(data_lines)
            event_name = None
            data_lines = []
            continue

        if stripped.startswith(":"):
            continue

        if stripped.startswith("event:"):
            event_name = stripped[6:].strip()
            continue

        if stripped.startswith("data:"):
            data_lines.append(stripped[5:].lstrip())

    if data_lines:
        yield event_name, "\n".join(data_lines)


def iter_sse_events_from_lines(lines):
    event_name = None
    data_lines: list[str] = []

    for line in lines:
        if line is None:
            continue

        if isinstance(line, bytes):
            line = line.decode("utf-8", errors="replace")

        stripped = line.rstrip("\r\n")
        if stripped == "":
            if data_lines:
                yield event_name, "\n".join(data_lines)
            event_name = None
            data_lines = []
            continue

        if stripped.startswith(":"):
            continue

        if stripped.startswith("event:"):
            event_name = stripped[6:].strip()
            continue

        if stripped.startswith("data:"):
            data_lines.append(stripped[5:].lstrip())

    if data_lines:
        yield event_name, "\n".join(data_lines)


def collect_completed_response(events) -> dict[str, Any]:
    final_response: dict[str, Any] | None = None

    for _event_name, data in events:
        if data == "[DONE]":
            break

        payload = json.loads(data)
        event_type = payload.get("type")
        if event_type == "response.completed":
            final_response = payload.get("response") or {}
        elif event_type == "response.failed" or event_type == "error":
            message = payload.get("message")
            if not message:
                message = ((payload.get("error") or {}).get("message")) or "Upstream stream failed."
            raise RequestError(502, message, "upstream_error")

    if final_response is None:
        raise RequestError(502, "Upstream stream did not produce a completed response.", "upstream_error")
    return final_response


def chat_chunk_payload(
    response_id: str,
    created: int,
    model: str,
    delta: dict[str, Any],
    finish_reason: str | None = None,
    usage: Any = UNSET,
) -> dict[str, Any]:
    payload = {
        "id": response_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "system_fingerprint": "",
        "choices": [
            {
                "index": 0,
                "delta": delta,
                "finish_reason": finish_reason,
            }
        ],
    }
    if usage is not UNSET:
        payload["usage"] = usage
    return payload


def usage_to_chat_usage(usage: dict[str, Any] | None) -> dict[str, int]:
    usage = usage or {}
    return {
        "prompt_tokens": usage.get("input_tokens", 0),
        "completion_tokens": usage.get("output_tokens", 0),
        "total_tokens": usage.get("total_tokens", 0),
    }


def responses_stream_events_to_chat_chunks(
    events,
    include_usage: bool = False,
):
    response_id = f"chatcmpl-proxy-{int(time.time())}"
    created = int(time.time())
    model = ""
    role_sent = False
    tool_call_indexes: dict[str, int] = {}
    tool_call_counter = count()
    final_response: dict[str, Any] | None = None

    def make_usage() -> Any:
        if include_usage:
            return None
        return UNSET

    def get_tool_index(call_id: str) -> int:
        if call_id not in tool_call_indexes:
            tool_call_indexes[call_id] = next(tool_call_counter)
        return tool_call_indexes[call_id]

    def emit(delta: dict[str, Any], finish_reason: str | None = None):
        return chat_chunk_payload(
            response_id=response_id,
            created=created,
            model=model,
            delta=delta,
            finish_reason=finish_reason,
            usage=make_usage(),
        )

    for _event_name, data in events:
        if data == "[DONE]":
            break

        payload = json.loads(data)
        event_type = payload.get("type")

        if event_type == "response.created":
            response = payload.get("response") or {}
            response_id = response.get("id", response_id)
            created = response.get("created_at", created)
            model = response.get("model", model)
            continue

        if event_type == "response.output_item.added":
            item = payload.get("item") or {}
            if item.get("type") == "function_call":
                call_id = item.get("call_id") or item.get("id") or f"call_{len(tool_call_indexes)}"
                tool_index = get_tool_index(call_id)
                if not role_sent:
                    role_sent = True
                    yield emit({"role": "assistant"})
                yield emit(
                    {
                        "tool_calls": [
                            {
                                "index": tool_index,
                                "id": call_id,
                                "type": "function",
                                "function": {
                                    "name": item.get("name", ""),
                                    "arguments": "",
                                },
                            }
                        ]
                    }
                )
            continue

        if event_type == "response.output_text.delta":
            if not role_sent:
                role_sent = True
                yield emit({"role": "assistant"})
            yield emit({"content": payload.get("delta", "")})
            continue

        if event_type == "response.refusal.delta":
            if not role_sent:
                role_sent = True
                yield emit({"role": "assistant"})
            yield emit({"refusal": payload.get("delta", "")})
            continue

        if event_type == "response.function_call_arguments.delta":
            call_id = payload.get("call_id") or payload.get("item_id") or f"call_{len(tool_call_indexes)}"
            tool_index = get_tool_index(call_id)
            if not role_sent:
                role_sent = True
                yield emit({"role": "assistant"})
            yield emit(
                {
                    "tool_calls": [
                        {
                            "index": tool_index,
                            "function": {
                                "arguments": payload.get("delta", ""),
                            },
                        }
                    ]
                }
            )
            continue

        if event_type == "response.completed":
            final_response = payload.get("response") or {}
            response_id = final_response.get("id", response_id)
            created = final_response.get("created_at", created)
            model = final_response.get("model", model)
            continue

        if event_type == "response.failed" or event_type == "error":
            message = payload.get("message")
            if not message:
                message = ((payload.get("error") or {}).get("message")) or "Upstream stream failed."
            raise RequestError(502, message, "upstream_error")

    finish_reason = "stop"
    usage: dict[str, int] | None = None
    if final_response is not None:
        _message, finish_reason = extract_assistant_message(final_response)
        usage = usage_to_chat_usage(final_response.get("usage"))

    yield chat_chunk_payload(
        response_id=response_id,
        created=created,
        model=model,
        delta={},
        finish_reason=finish_reason,
        usage=make_usage(),
    )

    if include_usage:
        yield {
            "id": response_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [],
            "usage": usage or {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        }


class ChatForwardHandler(BaseHTTPRequestHandler):
    server_version = "ChatForward/0.1"
    protocol_version = "HTTP/1.1"

    def log_request_arrival(self) -> None:
        now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        client_ip, client_port = self.client_address
        print(f"[{now}] {self.command} {self.path} from {client_ip}:{client_port}", flush=True)

    def do_OPTIONS(self) -> None:
        self.log_request_arrival()
        self.send_response(204)
        self.send_cors_headers()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:
        self.log_request_arrival()
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            self.send_json(200, {"status": "ok", "providers": sorted(PROVIDERS)})
            return

        self.send_json(404, error_body("Not found.", "not_found_error"))

    def do_POST(self) -> None:
        self.log_request_arrival()
        try:
            parsed = urlparse(self.path)
            server_name = self.extract_server_name(parsed.path)
            provider = PROVIDERS[server_name]

            body = self.read_json_body()
            upstream_payload = chat_request_to_responses_payload(body)
            if body.get("stream") is True:
                self.stream_request(provider, upstream_payload, body.get("stream_options"))
                return

            upstream_response = self.forward_request(provider, upstream_payload)
            chat_response = responses_to_chat_completion(upstream_response)
            self.send_json(200, chat_response)
        except RequestError as exc:
            self.send_json(exc.status, error_body(exc.message, exc.error_type))
        except requests.HTTPError as exc:
            response = exc.response
            status = response.status_code if response is not None else 502
            raw = response.text if response is not None else ""
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = error_body(raw or "Upstream request failed.", "upstream_error")
            self.send_json(status, payload)
        except requests.RequestException as exc:
            self.send_json(502, error_body(f"Upstream connection failed: {exc}", "upstream_error"))
        except Exception:
            traceback.print_exc()
            self.send_json(500, error_body("Internal server error.", "server_error"))

    def extract_server_name(self, path: str) -> str:
        parts = [part for part in path.split("/") if part]
        if len(parts) != 4 or parts[1:] != ["v1", "chat", "completions"]:
            raise RequestError(404, "Route not found.", "not_found_error")

        server_name = parts[0]
        if server_name not in PROVIDERS:
            raise RequestError(404, f"Unknown server_name: {server_name}", "not_found_error")
        return server_name

    def read_json_body(self) -> dict[str, Any]:
        content_length = self.headers.get("Content-Length")
        if not content_length:
            raise RequestError(400, "Missing Content-Length header.")

        try:
            length = int(content_length)
        except ValueError as exc:
            raise RequestError(400, "Invalid Content-Length header.") from exc

        raw = self.rfile.read(length)
        try:
            body = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RequestError(400, f"Invalid JSON body: {exc.msg}") from exc

        if not isinstance(body, dict):
            raise RequestError(400, "Request body must be a JSON object.")
        return body

    def forward_request(self, provider: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        return self.forward_request_via_stream(provider, payload)

    def forward_request_via_stream(self, provider: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        url, headers = self.build_upstream_request(provider, extra_headers={"Accept": "text/event-stream"})
        stream_payload = dict(payload)
        stream_payload["stream"] = True
        timeout = float(os.environ.get("UPSTREAM_TIMEOUT_SECONDS", "120"))
        with requests.post(url, headers=headers, json=stream_payload, timeout=timeout, stream=True) as response:
            response.raise_for_status()
            return collect_completed_response(iter_sse_events_from_lines(response.iter_lines(decode_unicode=False)))

    def stream_request(
        self,
        provider: dict[str, Any],
        payload: dict[str, Any],
        stream_options: dict[str, Any] | None,
    ) -> None:
        stream_payload = dict(payload)
        stream_payload["stream"] = True
        url, headers = self.build_upstream_request(provider, extra_headers={"Accept": "text/event-stream"})

        include_usage = bool((stream_options or {}).get("include_usage"))
        timeout = float(os.environ.get("UPSTREAM_TIMEOUT_SECONDS", "120"))
        with requests.post(url, headers=headers, json=stream_payload, timeout=timeout, stream=True) as response:
            response.raise_for_status()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.send_cors_headers()
            self.end_headers()
            self.close_connection = True
            events = iter_sse_events(response.raw)
            for chunk in responses_stream_events_to_chat_chunks(events, include_usage=include_usage):
                self.wfile.write(sse_frame(json.dumps(chunk, ensure_ascii=False)))
                self.wfile.flush()

        self.wfile.write(sse_frame("[DONE]"))
        self.wfile.flush()

    def build_upstream_request(
        self,
        provider: dict[str, Any],
        extra_headers: dict[str, str] | None = None,
    ) -> tuple[str, dict[str, str]]:
        upstream_url = provider["base_url"].rstrip("/") + "/responses"
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "curl/8.5.0",
        }
        if extra_headers:
            headers.update(extra_headers)

        token = extract_bearer_token(dict(self.headers))

        if provider.get("requires_openai_auth"):
            if not token:
                raise RequestError(401, "Missing upstream API key in Authorization bearer token.")
            headers["Authorization"] = f"Bearer {token}"

        return upstream_url, headers

    def send_json(self, status: int, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_cors_headers()
        self.end_headers()
        self.wfile.write(encoded)

    def send_cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Expose-Headers", "Content-Length,Content-Type")
        self.send_header("Access-Control-Allow-Credentials", "true")
        self.send_header("Access-Control-Max-Age", "43200")

    def log_message(self, format: str, *args: Any) -> None:
        return


def run_server() -> None:
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "80"))
    server = ChatForwardHTTPServer((host, port), ChatForwardHandler)
    print(f"Listening on {host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run_server()
