# sdk-runtime — DeepSeek Harness SDK runtime cho Windows

`deepseek-harness-runtime-bin` (PyPI) không có bản wheel cho Windows
(chỉ `manylinux` và `macosx_arm64`). Python SDK `deepseek-harness-sdk`
vì vậy không tự tìm được runtime và báo:

```
Unable to locate the bundled DeepSeek Harness SDK runtime.
Install deepseek-harness-runtime-bin or set HarnessConfig.runtime_bin.
```

Thư mục này là npm closure của runtime JSON-RPC (stdio) mà SDK spawn,
theo đúng "dev-only node carrier" trong tài liệu SDK:

- `package.json` — manifest plugin (bản `0.1.1-rc.2`, khớp closure
  `@deepseek-ai/dsh` đang cài global; wire protocol `dsh-sdk-protocol`
  giống hệt rc.1 nên tương thích SDK Python `0.1.1rc1`).
- `cordis.yml` — composition: JSON-RPC server, agent spine, adapter
  DeepSeek, persistence JSONL + checkpoint policy, bash/subprocess,
  filesystem local + tool-fs, token meter, compaction.
- `node_modules/` — được `npm install` sinh ra, không commit.

## Cài lại (khi clone máy khác hoặc cập nhật)

```sh
cd sdk-runtime
npm install
```

`telegram_bot.py` tự tìm runtime tại
`sdk-runtime/node_modules/@deepseek-ai/dsh-sdk-jsonrpc-demo/lib/packaged-bin.js`
và truyền qua `launch_args_override` + `cordis`. Nếu thiếu, bot báo
"Thiếu DeepSeek Harness SDK runtime (sdk-runtime/ chưa cài)".

Yêu cầu: Node >= 22.19 (máy này dùng Node 24). Git Bash
(`C:\Program Files\Git\bin`) được thêm vào PATH của runtime để tool bash
chạy được các wrapper `.sh`.
