#!/usr/bin/env python3
import json
import os
import socket
import threading
import time
import traceback
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from itertools import count
from typing import Any
from urllib.parse import quote, urlparse

import requests

PROVIDERS = {
    "codex-for-me": {
        "name": "codex-for-me",
        "base_url": "https://api-mobile.codex-for.me/v1",
        "wire_api": "responses",
        "auth_header": "Authorization",
        "auth_prefix": "Bearer ",
    },
    "right": {
        "name": "right",
        "base_url": "https://right.codes/codex/v1",
        "wire_api": "responses",
        "auth_header": "Authorization",
        "auth_prefix": "Bearer ",
    },
    "fox": {
        "name": "fox",
        "base_url": "https://code.newcli.com/codex/v1",
        "wire_api": "responses",
        "auth_header": "Authorization",
        "auth_prefix": "Bearer ",
    },
    "cch": {
        "name": "cch",
        "base_url": "http://8.141.2.179:23000/v1",
        "wire_api": "responses",
        "auth_header": "Authorization",
        "auth_prefix": "Bearer ",
        "user_agent": "codex_cli_rs/0.0.0",
        "default_tools": [{"type": "web_search"}],
    },
    "glm": {
        "name": "glm",
        "base_url": "https://open.bigmodel.cn/api/paas/v4",
        "wire_api": "openai_chat_completions",
        "auth_header": "Authorization",
        "auth_prefix": "Bearer ",
        "user_agent": "chat-forward/1.0",
        "default_tools": [
            {
                "type": "web_search",
                "web_search": {
                    "enable": True,
                    "search_engine": "Search-Pro-Quark",
                    "search_result": True,
                    "count": 10,
                },
            }
        ],
        "default_thinking": {"type": "disabled"},
    },
    "fox-gemini": {
        "name": "fox-gemini",
        "base_url": "https://code.newcli.com/gemini/v1beta",
        "wire_api": "gemini_generate_content",
        "auth_header": "x-goog-api-key",
        "auth_prefix": "",
    },
    "siliconflow": {
        "name": "siliconflow",
        "base_url": "https://api.siliconflow.cn/v1",
        "wire_api": "openai_chat_completions",
        "auth_header": "Authorization",
        "auth_prefix": "Bearer ",
    },
    "input": {
        "name": "input",
        "base_url": "https://ai.input.im/v1",
        "wire_api": "openai_chat_completions",
        "auth_header": "Authorization",
        "auth_prefix": "Bearer ",
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
POLL_STREAM_DONE_MARKER = "[DONE]"
POLL_STREAM_TASK_TTL_SECONDS = 600
POLL_STREAM_TASKS: dict[str, "PollingStreamTask"] = {}
POLL_STREAM_TASKS_LOCK = threading.Lock()
CLIENT_POLL_STREAM_TASK_LIMIT = 20
CLIENT_POLL_STREAM_TASKS: dict[tuple[str, str], "PollingStreamTask"] = {}
CLIENT_POLL_STREAM_TASKS_LOCK = threading.Lock()


class RequestError(Exception):
    def __init__(
        self,
        status: int,
        message: str,
        error_type: str = "invalid_request_error",
        payload: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.status = status
        self.message = message
        self.error_type = error_type
        self.payload = payload


class ChatForwardHTTPServer(ThreadingHTTPServer):
    request_queue_size = 20


class PollingStreamTask:
    def __init__(self, provider_name: str, requested_model: str, request_id: str | None = None):
        self.request_id = request_id or f"poll_{uuid.uuid4().hex}"
        self.provider_name = provider_name
        self.requested_model = requested_model
        self.upstream_response_id = ""
        self.created = int(time.time())
        self.status = "queued"
        self.pending_text = ""
        self.accumulated_text = ""
        self.finish_reason: str | None = None
        self.usage: dict[str, int] | None = None
        self.error: dict[str, Any] | None = None
        self.updated_at = time.time()
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    def touch(self) -> None:
        self.updated_at = time.time()

    def mark_running(self) -> None:
        with self._condition:
            self.status = "running"
            self.touch()
            self._condition.notify_all()

    def set_response_info(self, response_id: str | None = None, model: str | None = None) -> None:
        with self._condition:
            if response_id:
                self.upstream_response_id = response_id
            if model:
                self.requested_model = model
            self.touch()

    def append_text(self, delta: str) -> None:
        if not delta:
            return
        with self._condition:
            self.pending_text += delta
            self.accumulated_text += delta
            self.touch()
            self._condition.notify_all()

    def append_missing_suffix(self, full_text: str) -> None:
        if not full_text:
            return
        with self._condition:
            if full_text.startswith(self.accumulated_text):
                suffix = full_text[len(self.accumulated_text) :]
            else:
                suffix = ""
            if suffix:
                self.pending_text += suffix
                self.accumulated_text += suffix
                self._condition.notify_all()
            self.touch()

    def mark_completed(self, finish_reason: str | None, usage: dict[str, int] | None) -> None:
        with self._condition:
            self.status = "completed"
            self.finish_reason = finish_reason or "stop"
            self.usage = usage
            self.touch()
            self._condition.notify_all()

    def mark_error(self, message: str, error_type: str = "upstream_error") -> None:
        with self._condition:
            self.status = "error"
            self.error = error_body(message, error_type)["error"]
            self.touch()
            self._condition.notify_all()

    def poll(self) -> dict[str, Any]:
        with self._lock:
            delta = self.pending_text
            self.pending_text = ""
            self.touch()
            payload: dict[str, Any] = {
                "id": self.request_id,
                "object": "chat.completion.poll",
                "status": self.status,
                "response_id": self.upstream_response_id or self.request_id,
                "created": self.created,
                "model": self.requested_model,
                "delta": delta,
                "accumulated_text": self.accumulated_text,
                "done": self.status in {"completed", "error"},
                "done_marker": POLL_STREAM_DONE_MARKER if self.status in {"completed", "error"} else None,
            }
            if self.finish_reason is not None:
                payload["finish_reason"] = self.finish_reason
            if self.usage is not None:
                payload["usage"] = self.usage
            if self.error is not None:
                payload["error"] = self.error
            return payload

    def poll_client(self, wait_for_delta: bool = False) -> dict[str, Any]:
        with self._condition:
            while wait_for_delta and not self.pending_text and self.status not in {"completed", "error"}:
                self._condition.wait()

            delta = self.pending_text
            self.pending_text = ""
            self.touch()
            payload: dict[str, Any] = {
                "request_id": self.request_id,
                "status": self.status,
                "full_text": self.accumulated_text,
                "delta": delta,
                "done": self.status in {"completed", "error"},
                "done_marker": POLL_STREAM_DONE_MARKER if self.status in {"completed", "error"} else None,
            }
            if self.finish_reason is not None:
                payload["finish_reason"] = self.finish_reason
            if self.usage is not None:
                payload["usage"] = self.usage
            if self.error is not None:
                payload["error"] = self.error
            return payload


def prune_polling_stream_tasks() -> None:
    now = time.time()
    expired_ids: list[str] = []
    with POLL_STREAM_TASKS_LOCK:
        for request_id, task in POLL_STREAM_TASKS.items():
            if now - task.updated_at > POLL_STREAM_TASK_TTL_SECONDS:
                expired_ids.append(request_id)
        for request_id in expired_ids:
            POLL_STREAM_TASKS.pop(request_id, None)


def register_polling_stream_task(task: PollingStreamTask) -> None:
    prune_polling_stream_tasks()
    with POLL_STREAM_TASKS_LOCK:
        POLL_STREAM_TASKS[task.request_id] = task


def get_polling_stream_task(request_id: str) -> PollingStreamTask | None:
    prune_polling_stream_tasks()
    with POLL_STREAM_TASKS_LOCK:
        return POLL_STREAM_TASKS.get(request_id)


def prune_client_polling_stream_tasks() -> None:
    now = time.time()
    expired_keys: list[tuple[str, str]] = []
    with CLIENT_POLL_STREAM_TASKS_LOCK:
        for key, task in CLIENT_POLL_STREAM_TASKS.items():
            if now - task.updated_at > POLL_STREAM_TASK_TTL_SECONDS:
                expired_keys.append(key)
        for key in expired_keys:
            CLIENT_POLL_STREAM_TASKS.pop(key, None)


def get_client_polling_stream_task(provider_name: str, request_id: str) -> PollingStreamTask | None:
    prune_client_polling_stream_tasks()
    with CLIENT_POLL_STREAM_TASKS_LOCK:
        return CLIENT_POLL_STREAM_TASKS.get((provider_name, request_id))


def register_client_polling_stream_task(task: PollingStreamTask) -> None:
    prune_client_polling_stream_tasks()
    key = (task.provider_name, task.request_id)
    with CLIENT_POLL_STREAM_TASKS_LOCK:
        if key in CLIENT_POLL_STREAM_TASKS:
            return
        while len(CLIENT_POLL_STREAM_TASKS) >= CLIENT_POLL_STREAM_TASK_LIMIT:
            oldest_key = next(iter(CLIENT_POLL_STREAM_TASKS))
            CLIENT_POLL_STREAM_TASKS.pop(oldest_key, None)
        CLIENT_POLL_STREAM_TASKS[key] = task


def build_upstream_request_from_headers(
    provider: dict[str, Any],
    requested_model: str,
    request_headers: dict[str, str],
    stream: bool,
    extra_headers: dict[str, str] | None = None,
) -> tuple[str, dict[str, str]]:
    wire_api = provider.get("wire_api")
    if wire_api == "responses":
        upstream_url = provider["base_url"].rstrip("/") + "/responses"
    elif wire_api == "gemini_generate_content":
        action = "streamGenerateContent?alt=sse" if stream else "generateContent"
        upstream_url = (
            provider["base_url"].rstrip("/")
            + f"/models/{quote(str(requested_model), safe='')}:{action}"
        )
    elif wire_api == "openai_chat_completions":
        upstream_url = provider["base_url"].rstrip("/") + "/chat/completions"
    else:
        raise RequestError(500, f"Unsupported upstream wire_api: {wire_api}", "server_error")

    headers = {
        "Content-Type": "application/json",
        "User-Agent": provider.get("user_agent", "curl/8.5.0"),
    }
    if extra_headers:
        headers.update(extra_headers)

    token = extract_bearer_token(request_headers)
    auth_header = provider.get("auth_header")
    if auth_header:
        if not token:
            raise RequestError(401, "Missing upstream API key in Authorization bearer token.")
        headers[auth_header] = f"{provider.get('auth_prefix', '')}{token}"

    return upstream_url, headers


def run_polling_stream_task(
    task: PollingStreamTask,
    provider: dict[str, Any],
    request_headers: dict[str, str],
    requested_model: str,
    payload: dict[str, Any],
) -> None:
    task.mark_running()

    try:
        if provider.get("wire_api") == "openai_chat_completions":
            run_openai_chat_polling_stream_task(task, provider, request_headers, requested_model, payload)
            return

        url, headers = build_upstream_request_from_headers(
            provider,
            requested_model,
            request_headers,
            stream=True,
            extra_headers={"Accept": "text/event-stream"},
        )
        stream_payload = dict(payload)
        stream_payload["stream"] = True
        timeout = float(os.environ.get("UPSTREAM_TIMEOUT_SECONDS", "120"))

        with requests.post(url, headers=headers, json=stream_payload, timeout=timeout, stream=True) as response:
            raise_for_upstream_status(response)
            final_response: dict[str, Any] | None = None
            events = iter_sse_events(response.raw)
            for _event_name, data in events:
                if data == "[DONE]":
                    break

                event = json.loads(data)
                event_type = event.get("type")

                if event_type == "response.created":
                    upstream_response = event.get("response") or {}
                    task.set_response_info(
                        response_id=upstream_response.get("id"),
                        model=upstream_response.get("model"),
                    )
                    continue

                if event_type == "response.output_text.delta":
                    task.append_text(str(event.get("delta", "")))
                    continue

                if event_type == "response.refusal.delta":
                    task.append_text(str(event.get("delta", "")))
                    continue

                if event_type == "response.completed":
                    final_response = event.get("response") or {}
                    task.set_response_info(
                        response_id=final_response.get("id"),
                        model=final_response.get("model"),
                    )
                    continue

                if event_type == "response.failed" or event_type == "error":
                    message = event.get("message")
                    if not message:
                        message = ((event.get("error") or {}).get("message")) or "Upstream stream failed."
                    raise RequestError(502, message, "upstream_error")

            if final_response is None:
                raise RequestError(502, "Upstream stream did not produce a completed response.", "upstream_error")

            message, finish_reason = extract_assistant_message(final_response)
            if isinstance(message.get("content"), str):
                task.append_missing_suffix(message["content"])
            task.mark_completed(
                finish_reason=finish_reason,
                usage=usage_to_chat_usage(final_response.get("usage")),
            )
    except RequestError as exc:
        task.mark_error(exc.message, exc.error_type)
    except requests.RequestException as exc:
        task.mark_error(f"Upstream connection failed: {exc}")
    except Exception:
        traceback.print_exc()
        task.mark_error("Internal server error.", "server_error")


def run_openai_chat_polling_stream_task(
    task: PollingStreamTask,
    provider: dict[str, Any],
    request_headers: dict[str, str],
    requested_model: str,
    payload: dict[str, Any],
) -> None:
    url, headers = build_upstream_request_from_headers(
        provider,
        requested_model,
        request_headers,
        stream=True,
        extra_headers={"Accept": "text/event-stream"},
    )
    stream_payload = dict(payload)
    stream_payload["stream"] = True
    timeout = float(os.environ.get("UPSTREAM_TIMEOUT_SECONDS", "120"))

    with requests.post(url, headers=headers, json=stream_payload, timeout=timeout, stream=True) as response:
        raise_for_upstream_status(response)
        task.set_response_info(model=requested_model)
        for _event_name, data in iter_sse_events(response.raw):
            if data == "[DONE]":
                break

            chunk = json.loads(data)
            task.set_response_info(
                response_id=chunk.get("id"),
                model=chunk.get("model"),
            )

            choices = chunk.get("choices") or []
            if choices:
                delta = (choices[0].get("delta") or {}).get("content")
                if isinstance(delta, str) and delta:
                    task.append_text(delta)

                finish_reason = choices[0].get("finish_reason")
                if finish_reason:
                    task.mark_completed(
                        finish_reason=finish_reason,
                        usage=openai_chat_usage_to_chat_usage(chunk.get("usage")),
                    )
                    return

            if chunk.get("usage") is not None:
                task.usage = openai_chat_usage_to_chat_usage(chunk.get("usage"))

        task.mark_completed(
            finish_reason=task.finish_reason or "stop",
            usage=task.usage,
        )


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


def validate_chat_request(chat_request: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
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

    return model.strip(), messages


def normalize_model_name_for_provider(provider: dict[str, Any], model: str) -> str:
    normalized = model.strip()
    if provider.get("wire_api") != "gemini_generate_content":
        return normalized

    # Some OpenAI-compatible clients save Gemini models with provider/path prefixes
    # like `models/gemini-3-flash` or `google/gemini-3-flash`.
    while "/" in normalized:
        normalized = normalized.split("/", 1)[1].strip()
    return normalized


def merge_default_tools(
    chat_request: dict[str, Any],
    default_tools: list[dict[str, Any]] | None,
) -> list[dict[str, Any]] | None:
    if not default_tools:
        if "tools" in chat_request:
            return chat_request["tools"]
        return None

    if "tools" not in chat_request:
        return [dict(tool) for tool in default_tools]

    tools = chat_request["tools"]
    if not isinstance(tools, list):
        return tools

    requested_tool_types = {tool.get("type") for tool in tools if isinstance(tool, dict)}
    merged = [
        dict(tool)
        for tool in default_tools
        if tool.get("type") not in requested_tool_types
    ]
    for tool in tools:
        merged.append(tool)
    return merged


def chat_request_to_responses_payload(
    chat_request: dict[str, Any],
    provider: dict[str, Any] | None = None,
) -> dict[str, Any]:
    model, messages = validate_chat_request(chat_request)

    payload: dict[str, Any] = {
        "model": model,
        "instructions": "",
        "input": [normalize_chat_message(message) for message in messages],
    }

    max_tokens = chat_request.get("max_completion_tokens", chat_request.get("max_tokens"))
    if max_tokens is not None:
        payload["max_output_tokens"] = max_tokens

    for field in FORWARDED_FIELDS:
        if field in chat_request:
            payload[field] = chat_request[field]

    merged_tools = merge_default_tools(chat_request, (provider or {}).get("default_tools"))
    if merged_tools is not None:
        payload["tools"] = merged_tools

    return payload


def normalize_gemini_message_parts(content: Any) -> list[dict[str, Any]]:
    if content is None:
        return [{"text": ""}]

    if isinstance(content, str):
        return [{"text": content}]

    if not isinstance(content, list):
        raise RequestError(400, "Message content must be a string or an array.")

    parts: list[dict[str, Any]] = []
    for part in content:
        if not isinstance(part, dict):
            raise RequestError(400, "Each content part must be an object.")

        if part.get("type") != "text":
            raise RequestError(
                400,
                "Gemini upstream currently only supports text content parts through this proxy.",
            )

        parts.append({"text": part.get("text", "")})

    return parts or [{"text": ""}]


def append_gemini_content(contents: list[dict[str, Any]], role: str, parts: list[dict[str, Any]]) -> None:
    if contents and contents[-1].get("role") == role:
        contents[-1]["parts"].extend(parts)
        return

    contents.append({"role": role, "parts": parts})


def chat_request_to_gemini_payload(chat_request: dict[str, Any]) -> dict[str, Any]:
    model, messages = validate_chat_request(chat_request)

    unsupported_fields = [
        field
        for field in ("tools", "tool_choice", "parallel_tool_calls", "reasoning", "text", "truncation")
        if field in chat_request
    ]
    if unsupported_fields:
        raise RequestError(
            400,
            f"Gemini upstream does not support these fields through this proxy: {', '.join(unsupported_fields)}.",
        )

    system_parts: list[dict[str, Any]] = []
    contents: list[dict[str, Any]] = []

    for message in messages:
        if not isinstance(message, dict):
            raise RequestError(400, "Each message must be an object.")

        role = message.get("role")
        if role not in {"system", "developer", "user", "assistant"}:
            raise RequestError(400, f"Unsupported message role: {role!r}")

        parts = normalize_gemini_message_parts(message.get("content"))
        if role in {"system", "developer"}:
            system_parts.extend(parts)
            continue

        append_gemini_content(contents, "model" if role == "assistant" else "user", parts)

    if not contents:
        raise RequestError(400, "Gemini upstream requires at least one non-system chat message.")

    generation_config: dict[str, Any] = {}
    max_tokens = chat_request.get("max_completion_tokens", chat_request.get("max_tokens"))
    if max_tokens is not None:
        generation_config["maxOutputTokens"] = max_tokens
    if "temperature" in chat_request:
        generation_config["temperature"] = chat_request["temperature"]
    if "top_p" in chat_request:
        generation_config["topP"] = chat_request["top_p"]

    stop = chat_request.get("stop")
    if isinstance(stop, str):
        generation_config["stopSequences"] = [stop]
    elif isinstance(stop, list) and all(isinstance(item, str) for item in stop):
        generation_config["stopSequences"] = stop

    payload: dict[str, Any] = {"contents": contents}
    if system_parts:
        payload["systemInstruction"] = {"parts": system_parts}
    if generation_config:
        payload["generationConfig"] = generation_config

    return payload


def chat_request_to_openai_chat_payload(chat_request: dict[str, Any]) -> dict[str, Any]:
    model, messages = validate_chat_request(chat_request)

    payload = dict(chat_request)
    payload["model"] = model
    payload["messages"] = messages
    payload["enable_thinking"] = False
    return payload


def merge_default_dict_value(current_value: Any, default_value: Any) -> Any:
    if current_value is not None:
        return current_value
    return default_value


def chat_request_to_upstream_payload(provider: dict[str, Any], chat_request: dict[str, Any]) -> dict[str, Any]:
    wire_api = provider.get("wire_api")
    if wire_api == "responses":
        return chat_request_to_responses_payload(chat_request, provider)
    if wire_api == "gemini_generate_content":
        return chat_request_to_gemini_payload(chat_request)
    if wire_api == "openai_chat_completions":
        payload = chat_request_to_openai_chat_payload(chat_request)
        if provider.get("default_thinking") is not None and "thinking" not in payload:
            payload["thinking"] = provider["default_thinking"]
        merged_tools = merge_default_tools(chat_request, provider.get("default_tools"))
        if merged_tools is not None:
            payload["tools"] = merged_tools
        return payload
    raise RequestError(500, f"Unsupported upstream wire_api: {wire_api}", "server_error")


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


def parse_unix_timestamp(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value:
        try:
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
        except ValueError:
            return int(time.time())
    return int(time.time())


def gemini_finish_reason_to_chat(finish_reason: str | None) -> str:
    if finish_reason == "MAX_TOKENS":
        return "length"
    if finish_reason in {"SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII"}:
        return "content_filter"
    return "stop"


def gemini_usage_to_chat_usage(usage: dict[str, Any] | None) -> dict[str, int]:
    usage = usage or {}
    return {
        "prompt_tokens": usage.get("promptTokenCount", 0),
        "completion_tokens": usage.get("candidatesTokenCount", 0),
        "total_tokens": usage.get("totalTokenCount", 0),
    }


def extract_gemini_text(payload: dict[str, Any]) -> str:
    text_parts: list[str] = []
    candidates = payload.get("candidates") or []
    if not candidates:
        return ""

    content = (candidates[0].get("content") or {}).get("parts") or []
    for part in content:
        if isinstance(part, dict) and isinstance(part.get("text"), str):
            text_parts.append(part["text"])
    return "".join(text_parts)


def gemini_to_chat_completion(response_payload: dict[str, Any], requested_model: str) -> dict[str, Any]:
    finish_reason = gemini_finish_reason_to_chat(((response_payload.get("candidates") or [{}])[0]).get("finishReason"))
    text = extract_gemini_text(response_payload)

    return {
        "id": response_payload.get("responseId", f"chatcmpl-proxy-{int(time.time())}"),
        "object": "chat.completion",
        "created": parse_unix_timestamp(response_payload.get("createTime")),
        "model": response_payload.get("modelVersion", requested_model),
        "system_fingerprint": "",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": finish_reason,
            }
        ],
        "usage": gemini_usage_to_chat_usage(response_payload.get("usageMetadata")),
    }


def upstream_error_payload(response: requests.Response | None) -> dict[str, Any]:
    status = response.status_code if response is not None else 502
    raw = response.text if response is not None else ""
    request_id = ""
    reason = ""
    if response is not None:
        request_id = response.headers.get("x-oneapi-request-id", "").strip()
        reason = (response.reason or "").strip()

    if raw:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = None

        if isinstance(payload, dict):
            existing_error = payload.get("error")
            if isinstance(existing_error, dict) and existing_error.get("message"):
                return payload

            message = payload.get("message") or payload.get("statusMessage") or payload.get("detail")
            if message:
                if request_id:
                    message = f"{message} (upstream request id: {request_id})"
                return error_body(message, "upstream_error")

        message = raw.strip()
        if request_id:
            message = f"{message} (upstream request id: {request_id})"
        return error_body(message, "upstream_error")

    message = reason or f"Upstream request failed with status {status}."
    if request_id:
        message = f"{message} (upstream request id: {request_id})"
    return error_body(message, "upstream_error")


def error_body(message: str, error_type: str = "invalid_request_error") -> dict[str, Any]:
    return {"error": {"message": message, "type": error_type}}


def raise_for_upstream_status(response: requests.Response) -> None:
    if response.ok:
        return

    payload = upstream_error_payload(response)
    error = payload.get("error") or {}
    raise RequestError(
        response.status_code,
        str(error.get("message") or f"Upstream request failed with status {response.status_code}."),
        str(error.get("type") or "upstream_error"),
        payload=payload,
    )


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
    include_system_fingerprint: bool = True,
) -> dict[str, Any]:
    payload = {
        "id": response_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [
            {
                "index": 0,
                "delta": delta,
                "finish_reason": finish_reason,
            }
        ],
    }
    if include_system_fingerprint:
        payload["system_fingerprint"] = ""
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


def openai_chat_usage_to_chat_usage(usage: dict[str, Any] | None) -> dict[str, int]:
    usage = usage or {}
    return {
        "prompt_tokens": usage.get("prompt_tokens", 0),
        "completion_tokens": usage.get("completion_tokens", 0),
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


def gemini_stream_events_to_chat_chunks(
    events,
    requested_model: str,
    include_usage: bool = False,
):
    response_id = f"chatcmpl-proxy-{int(time.time())}"
    created = int(time.time())
    model = requested_model
    finish_reason = "stop"
    usage: dict[str, int] | None = None
    stream_finished = False

    for _event_name, data in events:
        if data == "[DONE]":
            break

        payload = json.loads(data)
        if payload.get("error"):
            raise RequestError(502, payload.get("message") or "Upstream stream failed.", "upstream_error")

        response_id = payload.get("responseId", response_id)
        created = parse_unix_timestamp(payload.get("createTime"))
        model = payload.get("modelVersion", model)
        usage = gemini_usage_to_chat_usage(payload.get("usageMetadata"))

        candidates = payload.get("candidates") or []
        if candidates:
            finish_reason = gemini_finish_reason_to_chat(candidates[0].get("finishReason"))
            if candidates[0].get("finishReason"):
                stream_finished = True

        text = extract_gemini_text(payload)
        if text:
            yield chat_chunk_payload(
                response_id=response_id,
                created=created,
                model=model,
                delta={"content": text},
                include_system_fingerprint=False,
            )

        if stream_finished:
            break

    yield chat_chunk_payload(
        response_id=response_id,
        created=created,
        model=model,
        delta={},
        finish_reason=finish_reason,
        include_system_fingerprint=False,
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

        try:
            server_name, request_id = self.extract_poll_stream_route(parsed.path)
            if request_id is None:
                raise RequestError(404, "Route not found.", "not_found_error")
            self.handle_poll_stream_get(server_name, request_id)
            return
        except RequestError as exc:
            if exc.status != 404:
                self.send_json(exc.status, exc.payload or error_body(exc.message, exc.error_type))
                return

        self.send_json(404, error_body("Not found.", "not_found_error"))

    def do_POST(self) -> None:
        self.log_request_arrival()
        try:
            parsed = urlparse(self.path)
            poll_route = self.try_extract_poll_stream_route(parsed.path)
            if poll_route is not None:
                server_name, request_id = poll_route
                if request_id is not None:
                    raise RequestError(404, "Route not found.", "not_found_error")
                body = self.read_json_body()
                if "request_id" in body:
                    self.handle_client_poll_stream_create(server_name, body)
                else:
                    self.handle_poll_stream_create(server_name, body)
                return

            server_name = self.extract_server_name(parsed.path)
            provider = PROVIDERS[server_name]

            body = self.read_json_body()
            requested_model = normalize_model_name_for_provider(provider, str(body.get("model", "")))
            upstream_payload = chat_request_to_upstream_payload(provider, body)
            if body.get("stream") is True:
                self.stream_request(provider, requested_model, upstream_payload, body.get("stream_options"))
                return

            upstream_response = self.forward_request(provider, requested_model, upstream_payload)
            chat_response = self.upstream_to_chat_completion(provider, requested_model, upstream_response)
            self.send_json(200, chat_response)
        except RequestError as exc:
            self.send_json(exc.status, exc.payload or error_body(exc.message, exc.error_type))
        except requests.HTTPError as exc:
            response = exc.response
            status = response.status_code if response is not None else 502
            self.send_json(status, upstream_error_payload(response))
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

    def try_extract_poll_stream_route(self, path: str) -> tuple[str, str | None] | None:
        parts = [part for part in path.split("/") if part]
        if len(parts) == 4 and parts[1:] == ["v1", "chat", "poll-completions"]:
            server_name = parts[0]
            if server_name not in PROVIDERS:
                raise RequestError(404, f"Unknown server_name: {server_name}", "not_found_error")
            return server_name, None

        if len(parts) == 5 and parts[1:4] == ["v1", "chat", "poll-completions"]:
            server_name = parts[0]
            if server_name not in PROVIDERS:
                raise RequestError(404, f"Unknown server_name: {server_name}", "not_found_error")
            return server_name, parts[4]

        return None

    def extract_poll_stream_route(self, path: str) -> tuple[str, str | None]:
        route = self.try_extract_poll_stream_route(path)
        if route is None:
            raise RequestError(404, "Route not found.", "not_found_error")
        return route

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

    def handle_poll_stream_create(self, server_name: str, body: dict[str, Any] | None = None) -> None:
        if server_name != "fox":
            raise RequestError(404, "Route not found.", "not_found_error")

        provider = PROVIDERS[server_name]
        if provider.get("wire_api") != "responses":
            raise RequestError(400, "Polling stream mode currently only supports Responses-backed routes.")

        if body is None:
            body = self.read_json_body()
        requested_model = normalize_model_name_for_provider(provider, str(body.get("model", "")))
        upstream_payload = chat_request_to_upstream_payload(provider, body)
        build_upstream_request_from_headers(
            provider,
            requested_model,
            dict(self.headers),
            stream=True,
            extra_headers={"Accept": "text/event-stream"},
        )

        task = PollingStreamTask(provider_name=server_name, requested_model=requested_model)
        register_polling_stream_task(task)
        worker = threading.Thread(
            target=run_polling_stream_task,
            args=(task, provider.copy(), dict(self.headers), requested_model, upstream_payload),
            daemon=True,
        )
        worker.start()

        self.send_json(
            202,
            {
                "id": task.request_id,
                "object": "chat.completion.poll",
                "status": "queued",
                "done": False,
                "done_marker": POLL_STREAM_DONE_MARKER,
                "poll_url": f"/{server_name}/v1/chat/poll-completions/{task.request_id}",
            },
        )

    def handle_client_poll_stream_create(self, server_name: str, body: dict[str, Any]) -> None:
        provider = PROVIDERS[server_name]
        if provider.get("wire_api") not in {"responses", "openai_chat_completions"}:
            raise RequestError(
                400,
                "Client poll mode currently only supports Responses-backed and OpenAI chat-backed routes.",
            )

        request_id = str(body.get("request_id", "")).strip()
        if not request_id:
            raise RequestError(400, "`request_id` must be a non-empty string.")

        task = get_client_polling_stream_task(server_name, request_id)
        if task is None:
            requested_model = normalize_model_name_for_provider(provider, str(body.get("model", "")))
            upstream_payload = chat_request_to_upstream_payload(provider, body)
            task = PollingStreamTask(provider_name=server_name, requested_model=requested_model, request_id=request_id)
            register_client_polling_stream_task(task)
            worker = threading.Thread(
                target=run_polling_stream_task,
                args=(task, provider.copy(), dict(self.headers), requested_model, upstream_payload),
                daemon=True,
            )
            worker.start()

        self.send_json(200, task.poll_client(wait_for_delta=True))

    def handle_poll_stream_get(self, server_name: str, request_id: str) -> None:
        if server_name != "fox":
            raise RequestError(404, "Route not found.", "not_found_error")

        task = get_polling_stream_task(request_id)
        if task is None or task.provider_name != server_name:
            raise RequestError(404, f"Unknown request_id: {request_id}", "not_found_error")
        self.send_json(200, task.poll())

    def forward_request(self, provider: dict[str, Any], requested_model: str, payload: dict[str, Any]) -> dict[str, Any]:
        if provider.get("wire_api") == "responses":
            return self.forward_request_via_stream(provider, requested_model, payload)
        if provider.get("wire_api") in {"gemini_generate_content", "openai_chat_completions"}:
            return self.forward_request_json(provider, requested_model, payload)
        raise RequestError(500, f"Unsupported upstream wire_api: {provider.get('wire_api')}", "server_error")

    def forward_request_json(self, provider: dict[str, Any], requested_model: str, payload: dict[str, Any]) -> dict[str, Any]:
        url, headers = self.build_upstream_request(provider, requested_model, stream=False)
        timeout = float(os.environ.get("UPSTREAM_TIMEOUT_SECONDS", "120"))
        response = requests.post(url, headers=headers, json=payload, timeout=timeout)
        raise_for_upstream_status(response)
        return response.json()

    def forward_request_via_stream(
        self,
        provider: dict[str, Any],
        requested_model: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        url, headers = self.build_upstream_request(
            provider,
            requested_model,
            stream=True,
            extra_headers={"Accept": "text/event-stream"},
        )
        stream_payload = dict(payload)
        stream_payload["stream"] = True
        timeout = float(os.environ.get("UPSTREAM_TIMEOUT_SECONDS", "120"))
        with requests.post(url, headers=headers, json=stream_payload, timeout=timeout, stream=True) as response:
            raise_for_upstream_status(response)
            return collect_completed_response(iter_sse_events_from_lines(response.iter_lines(decode_unicode=False)))

    def stream_request(
        self,
        provider: dict[str, Any],
        requested_model: str,
        payload: dict[str, Any],
        stream_options: dict[str, Any] | None,
    ) -> None:
        if provider.get("wire_api") == "responses":
            self.stream_request_responses(provider, requested_model, payload, stream_options)
            return
        if provider.get("wire_api") == "gemini_generate_content":
            self.stream_request_gemini(provider, requested_model, payload, stream_options)
            return
        if provider.get("wire_api") == "openai_chat_completions":
            self.stream_request_openai_chat(provider, requested_model, payload)
            return
        raise RequestError(500, f"Unsupported upstream wire_api: {provider.get('wire_api')}", "server_error")

    def stream_request_responses(
        self,
        provider: dict[str, Any],
        requested_model: str,
        payload: dict[str, Any],
        stream_options: dict[str, Any] | None,
    ) -> None:
        stream_payload = dict(payload)
        stream_payload["stream"] = True
        url, headers = self.build_upstream_request(
            provider,
            requested_model,
            stream=True,
            extra_headers={"Accept": "text/event-stream"},
        )

        include_usage = bool((stream_options or {}).get("include_usage"))
        timeout = float(os.environ.get("UPSTREAM_TIMEOUT_SECONDS", "120"))
        with requests.post(url, headers=headers, json=stream_payload, timeout=timeout, stream=True) as response:
            raise_for_upstream_status(response)
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache, no-transform")
            self.send_header("Connection", "close")
            self.send_header("X-Accel-Buffering", "no")
            self.send_cors_headers()
            self.end_headers()
            self.close_connection = True
            events = iter_sse_events(response.raw)
            for chunk in responses_stream_events_to_chat_chunks(events, include_usage=include_usage):
                self.wfile.write(sse_frame(json.dumps(chunk, ensure_ascii=False)))
                self.wfile.flush()

        self.finish_sse_response()

    def stream_request_gemini(
        self,
        provider: dict[str, Any],
        requested_model: str,
        payload: dict[str, Any],
        stream_options: dict[str, Any] | None,
    ) -> None:
        url, headers = self.build_upstream_request(
            provider,
            requested_model,
            stream=True,
            extra_headers={"Accept": "text/event-stream"},
        )
        include_usage = bool((stream_options or {}).get("include_usage"))
        timeout = float(os.environ.get("UPSTREAM_TIMEOUT_SECONDS", "120"))
        with requests.post(url, headers=headers, json=payload, timeout=timeout, stream=True) as response:
            raise_for_upstream_status(response)
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache, no-transform")
            self.send_header("Connection", "close")
            self.send_header("X-Accel-Buffering", "no")
            self.send_cors_headers()
            self.end_headers()
            self.close_connection = True
            events = iter_sse_events(response.raw)
            for chunk in gemini_stream_events_to_chat_chunks(
                events,
                requested_model=requested_model,
                include_usage=include_usage,
            ):
                self.wfile.write(sse_frame(json.dumps(chunk, ensure_ascii=False)))
                self.wfile.flush()

        self.finish_sse_response()

    def stream_request_openai_chat(
        self,
        provider: dict[str, Any],
        requested_model: str,
        payload: dict[str, Any],
    ) -> None:
        url, headers = self.build_upstream_request(
            provider,
            requested_model,
            stream=True,
            extra_headers={"Accept": "text/event-stream"},
        )
        timeout = float(os.environ.get("UPSTREAM_TIMEOUT_SECONDS", "120"))
        with requests.post(url, headers=headers, json=payload, timeout=timeout, stream=True) as response:
            raise_for_upstream_status(response)
            self.send_response(200)
            self.send_header(
                "Content-Type",
                response.headers.get("Content-Type", "text/event-stream; charset=utf-8"),
            )
            self.send_header("Cache-Control", "no-cache, no-transform")
            self.send_header("Connection", "close")
            self.send_header("X-Accel-Buffering", "no")
            self.send_cors_headers()
            self.end_headers()
            self.close_connection = True
            for chunk in response.iter_content(chunk_size=8192):
                if not chunk:
                    continue
                self.wfile.write(chunk)
                self.wfile.flush()

        self.close_streaming_response()

    def finish_sse_response(self) -> None:
        try:
            self.wfile.write(sse_frame("[DONE]"))
            self.wfile.flush()
        finally:
            self.close_streaming_response()

    def close_streaming_response(self) -> None:
        self.close_connection = True
        try:
            self.connection.shutdown(socket.SHUT_WR)
        except OSError:
            pass

    def upstream_to_chat_completion(
        self,
        provider: dict[str, Any],
        requested_model: str,
        response_payload: dict[str, Any],
    ) -> dict[str, Any]:
        if provider.get("wire_api") == "responses":
            return responses_to_chat_completion(response_payload)
        if provider.get("wire_api") == "gemini_generate_content":
            return gemini_to_chat_completion(response_payload, requested_model)
        if provider.get("wire_api") == "openai_chat_completions":
            return response_payload
        raise RequestError(500, f"Unsupported upstream wire_api: {provider.get('wire_api')}", "server_error")

    def build_upstream_request(
        self,
        provider: dict[str, Any],
        requested_model: str,
        stream: bool,
        extra_headers: dict[str, str] | None = None,
    ) -> tuple[str, dict[str, str]]:
        return build_upstream_request_from_headers(
            provider,
            requested_model,
            dict(self.headers),
            stream=stream,
            extra_headers=extra_headers,
        )

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
    port = int(os.environ.get("PORT", "8000"))
    server = ChatForwardHTTPServer((host, port), ChatForwardHandler)
    print(f"Listening on {host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run_server()
