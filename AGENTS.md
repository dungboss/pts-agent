# AGENTS.md — hướng dẫn chung cho AI agent (workspace)

Project này có 3 pipeline Photoshop: `age-script`, `mockup-script`, `tri-script`.
Mỗi pipeline có `AGENTS.md` riêng mô tả cách chạy — đọc file đó khi làm việc trong
thư mục tương ứng.

## QUAN TRỌNG — cách hỏi người dùng qua Telegram

Agent chạy qua Telegram **không có tool `AskUserQuestion`** như trên Web GUI, nên
phải hỏi bằng tin nhắn text. Khi cần hỏi để điền config:

- Chỉ gửi **đúng các câu hỏi cần trả lời**, đánh số, ngắn gọn (mỗi câu một dòng).
- **KHÔNG** viết bất kỳ câu mở đầu nào trước câu hỏi (không "tôi đã đọc AGENTS.md",
  không "bắt buộc phải hỏi", không "chưa chạy được vì thiếu config").
- **KHÔNG** liệt kê mục "hiện trạng máy" / "tình trạng hiện tại" trước câu hỏi.
- Chỉ nêu tình trạng/ghi chú nếu người dùng hỏi, hoặc gộp ngắn gọn vào chính câu hỏi
  khi thật sự cần.

Ví dụ định dạng đúng:

```
1. PTS (template PSD) lấy ở đâu — local PTS/ hay NAS?
2. Design lấy ở đâu — local Design/ hay NAS?
3. Output dùng Result/ chứ?
4. limit bao nhiêu — 1 (smoke test) hay 0 (tất cả)?
```

## Chạy nhiều script trong một tin nhắn (xếp hàng tuần tự)

Nếu người dùng yêu cầu nhiều lần chạy trong cùng một tin nhắn (ví dụ "chạy tri rồi
chạy age rồi chạy mockup"), hãy tách thành danh sách việc và chạy **lần lượt**:
xong việc trước (đợi file `*.done` xuất hiện hoặc wrapper thoát) rồi mới chạy việc kế.
Không chạy song song — cả ba pipeline dùng chung một Photoshop. Sau mỗi việc báo kết
quả ngắn rồi tiếp tục việc kế.

## Báo kết quả ngắn gọn

Khi báo kết quả chạy xong, chỉ gửi **1–3 dòng** ngắn gọn, ví dụ:

```
✅ Xong — mockup: 2/2 ảnh, exit OK. Output: [NAS]/NAME/DE/tri44/mockup
```

Gồm đúng: ✅/❌ + tên pipeline + số đã xử lý (x/tổng) + exit/done + đường dẫn output.
KHÔNG viết dài dòng, KHÔNG thêm mục "lưu ý nhỏ" / "sự cố gặp phải" / liệt kê config —
chỉ nêu lỗi hoặc ghi chú khi có lỗi thật sự hoặc người dùng yêu cầu chi tiết.

## Tăng tốc — chạy ngay khi đã đủ config

Khi người dùng gửi sẵn đầy đủ cấu hình trong tin nhắn, hãy ghi config rồi chạy wrapper
**ngay lập tức**: không đọc lại `AGENTS.md`, không `ls`/khám phá thêm, không hỏi lại —
chỉ làm đúng những gì cần để ghi config và chạy.

## Dọn file tạm sau mỗi lần chạy

Sau mỗi lần chạy xong, xoá các file tạm Photoshop sinh ra trong thư mục output:

- `._*` — metadata AppleDouble của macOS khi ghi lên NAS.
- `*.sb-*` — file tạm 0 byte của Photoshop Save-for-Web khi ghi lên ổ mạng.

Wrapper `run-*.sh` đã tự dọn. Nếu còn sót, dùng:

```
find <output> -maxdepth 1 \( -name '._*' -o -name '*.sb-*' \) -delete
```

## Sau /cancel — không tự mở lại Photoshop

Sau khi người dùng `/cancel`, **KHÔNG được tự chạy lại wrapper, không tự mở/activate
Photoshop** (kể cả qua `osascript` hay `open`) — chỉ báo đã huỷ và chờ lệnh chạy mới
từ người dùng.
