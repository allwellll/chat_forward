# Chat Forward Proxy

把 OpenAI `responses` 协议的上游，包装成普通的 `chat/completions` 接口，同时支持 `stream=true` 和 `stream=false`。

支持的路由：

- `/codex-for-me/v1/chat/completions`
- `/right/v1/chat/completions`
- `/fox/v1/chat/completions`

## 启动

```bash
python3 server.py
```

默认监听 `0.0.0.0:80`，可通过环境变量修改：

```bash
HOST=0.0.0.0 PORT=8080 python3 server.py
```

可选环境变量：

- `UPSTREAM_TIMEOUT_SECONDS`: 上游超时，默认 `120`

## 调用示例

```bash
curl http://127.0.0.1:8080/codex-for-me/v1/chat/completions \
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

流式调用示例：

```bash
curl http://127.0.0.1:8080/codex-for-me/v1/chat/completions \
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

## 当前限制

- 只支持 `n=1`
- 输入暂不支持 `tool_calls` 历史消息
- 代理主要面向文本聊天；`text` 和 `image_url` 内容段已做兼容

## 测试

```bash
python3 -m unittest discover -s tests -v
```
