# Chat Forward Proxy

把 OpenAI `responses` 协议的上游，包装成普通的 `chat/completions` 接口，同时支持 `stream=true` 和 `stream=false`。

支持的路由：

- `/codex-for-me/v1/chat/completions`
- `/right/v1/chat/completions`
- `/fox/v1/chat/completions`
- `/cch/v1/chat/completions`
- `/glm/v1/chat/completions`
- `/fox/v1/chat/poll-completions`
- `/fox/v1/chat/poll-completions/{request_id}`
- `/{provider}/v1/chat/poll-completions`，带 `request_id` 时启用快捷指令轮询模式
- `/fox-gemini/v1/chat/completions`
- `/siliconflow/v1/chat/completions`
- `/input/v1/chat/completions`

## 启动

```bash
python3 server.py
```

默认监听 `0.0.0.0:8000`，可通过环境变量修改：

```bash
HOST=0.0.0.0 PORT=8000 python3 server.py
```

可选环境变量：

- `UPSTREAM_TIMEOUT_SECONDS`: 上游超时，默认 `120`

## 调用示例

```bash
curl http://127.0.0.1:8000/codex-for-me/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer sk-your-key' \
  -d '{
    "model": "gpt-4.1",
    "messages": [
      {"role": "system", "content": "You are helpful."},
      {"role": "user", "content": "hello"}
    ]
  }'
```

必须传 `Authorization: Bearer <key>`；服务只转发请求里带来的 key，不做任何回退。

`cch` 路由同样走 OpenAI `responses` 协议，上游指向 `http://8.141.2.179:23000/v1`，并会默认启用 Responses API 的内置搜索工具：

```json
{"tools": [{"type": "web_search"}]}
```

`glm` 路由走 OpenAI `chat/completions` 协议，上游指向 `https://open.bigmodel.cn/api/paas/v4`，服务端会默认补上：

```json
{
  "thinking": {"type": "disabled"},
  "tools": [
    {
      "type": "web_search",
      "web_search": {
        "enable": true,
        "search_engine": "Search-Pro-Quark",
        "search_result": true,
        "count": 10
      }
    }
  ]
}
```

Gemini 路由同样对外暴露成 `chat/completions`，但服务端会自动转成 Gemini 原生接口：

- 上游路径：`/v1beta/models/{model}:generateContent`
- 流式路径：`/v1beta/models/{model}:streamGenerateContent?alt=sse`
- 上游鉴权头：`x-goog-api-key`

也就是说，客户端仍然只需要对代理传 `Authorization: Bearer <key>`，不要把 key 写进代码。

SiliconFlow 路由本身就是 OpenAI `chat/completions` 协议，代理会直接透传请求和响应，但会强制设置：

- `enable_thinking: false`

也就是说，客户端仍然只需要传模型名和 key，例如 `Qwen/Qwen3.5-35B-A3B`，不需要自己处理关闭思考模式。

`input` 路由同样是 OpenAI `chat/completions` 协议，默认上游指向 `https://ai.input.im/v1`，直接复用同样的 Bearer key 即可。

流式调用示例：

```bash
curl http://127.0.0.1:8000/codex-for-me/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer sk-your-key' \
  -N \
  -d '{
    "model": "gpt-4.1",
    "stream": true,
    "stream_options": {"include_usage": true},
    "messages": [
      {"role": "user", "content": "hello"}
    ]
  }'
```

## Fox 轮询接口

为了兼容不支持 SSE 的客户端，额外提供了一组只针对 `fox` 的轮询接口；原来的 `/fox/v1/chat/completions` 保持不变。

1. 创建任务：

```bash
curl http://127.0.0.1:8000/fox/v1/chat/poll-completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer sk-your-key' \
  -d '{
    "model": "gpt-4.1",
    "messages": [
      {"role": "user", "content": "hello"}
    ]
  }'
```

返回示例：

```json
{
  "id": "poll_xxx",
  "object": "chat.completion.poll",
  "status": "queued",
  "done": false,
  "done_marker": "[DONE]",
  "poll_url": "/fox/v1/chat/poll-completions/poll_xxx"
}
```

2. 轮询增量：

```bash
curl http://127.0.0.1:8000/fox/v1/chat/poll-completions/poll_xxx
```

返回字段说明：

- `delta`: 自上次轮询后新增的文本片段
- `accumulated_text`: 到当前为止的完整累计文本
- `status`: `queued`、`running`、`completed`、`error`
- `done`: 是否结束
- `done_marker`: 结束时固定返回 `[DONE]`
- `error`: 仅在 `status=error` 时返回

## 快捷指令轮询接口

为了兼容不方便处理 SSE 的客户端，可以对 Responses-backed 路由使用重复 POST 长轮询。首次看到新的 `request_id` 时，服务会在后台启动一次上游流式请求；后续用同一个 `request_id` 重复 POST，会返回到目前为止的完整文本和本次新增文本。

示例：

```bash
curl http://127.0.0.1:8000/cch/v1/chat/poll-completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer sk-your-key' \
  -d '{
    "request_id": "shortcut-20260613-001",
    "model": "gpt-5.4",
    "messages": [
      {"role": "user", "content": "今天 NBA 总决赛打到第几场了？下一场什么时候？"}
    ]
  }'
```

`glm` 也支持同样的快捷指令轮询形式：

```bash
curl http://127.0.0.1:8000/glm/v1/chat/poll-completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer sk-your-key' \
  -d '{
    "request_id": "shortcut-glm-001",
    "model": "glm-5",
    "messages": [
      {"role": "user", "content": "2026年NBA比赛总决赛比分情况"}
    ]
  }'
```

返回示例：

```json
{
  "request_id": "shortcut-20260613-001",
  "status": "running",
  "full_text": "到目前为止的完整回复",
  "delta": "本次新增文本",
  "done": false,
  "done_marker": null
}
```

如果当前没有新增文本且任务还在运行，请求会挂住等待，直到有新的 `delta` 或任务结束/报错才返回。也就是说，正常情况下客户端不会收到空 `delta` 后立刻空转重试。当 `done` 为 `true` 时停止轮询；结束时 `done_marker` 固定为 `[DONE]`。服务最多保留 20 个客户端 `request_id` 任务，超过后会删除最早的任务，防止内存持续增长。

## 当前限制

- 只支持 `n=1`
- 输入暂不支持 `tool_calls` 历史消息
- 代理主要面向文本聊天；`text` 和 `image_url` 内容段已做兼容

## 测试

```bash
python3 -m unittest discover -s tests -v
```
