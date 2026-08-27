# Telegram gateway

Gateway này biến Telegram thành **cầu nối chat trực tiếp tới DeepSeek Harness**:
mọi tin nhắn bạn gửi cho bot đều được chuyển thẳng cho agent DeepSeek Harness
(giữ session theo từng chat, dùng tool trong workspace để đọc file / chạy
wrapper), và câu trả lời của agent được gửi về Telegram.

Không còn wizard cấu hình hay lệnh `/run tri|age|mockup` — bạn chỉ cần nhắn
bình thường như đang chat trực tiếp với Harness. Bot dùng Telegram Bot API
polling, chỉ dùng Python standard library cho phần Telegram (agent dùng
`deepseek-harness-sdk`).

## Cài đặt lần đầu

1. Mở Telegram, nhắn `/newbot` cho [@BotFather](https://t.me/BotFather), tạo bot và copy token.
2. Tạo file cấu hình local:

   ```bash
   cp .env.example .env
   chmod 600 .env
   ```

   Điền `TELEGRAM_BOT_TOKEN`. Chưa cần điền chat ID ở bước này.

3. Nhắn một tin bất kỳ cho bot, sau đó lấy chat ID:

   ```bash
   ./run-telegram.sh --show-chat-ids
   ```

   Điền ID vào `TELEGRAM_ALLOWED_CHAT_IDS`, rồi chạy lại bot.

4. Cài DeepSeek Harness SDK:

   ```bash
   python3 -m venv .venv
   .venv/bin/pip install -r requirements.txt
   ```

   Điền `DEEPSEEK_API_KEY` trong `.env`. Có thể đổi model bằng `DSH_MODEL`.

## Chạy

```bash
./run-telegram.sh            # macOS
run-telegram.bat             # Windows
```

Chạy nền trên macOS:

```bash
nohup ./run-telegram.sh >> telegram-bot.log 2>&1 &
```

> **Quan trọng:** chạy bot từ chính **Terminal.app**. Khi agent gọi wrapper
> Photoshop (`run-*.sh` → AppleScript), macOS cần Terminal được cấp quyền tại
> **System Settings → Privacy & Security → Automation → bật Terminal → Adobe
> Photoshop**. Nếu chưa cấp, job sẽ lỗi `A privilege violation occurred (-10004)`.

## Dùng như thế nào

- **Nhắn bình thường** — tin nhắn được chuyển cho DeepSeek Harness agent. Agent
  sẽ hỏi đủ thông tin cấu hình, tóm tắt và chờ bạn xác nhận trước khi chạy
  pipeline, rồi báo kết quả (exit code, trạng thái done, log, đường dẫn output)
  ngay trong chat.
- **`/reset`** — xóa lịch sử hội thoại AI của chat (tạo session mới).

Không đưa `.env`, token, hoặc credential NAS vào git.
