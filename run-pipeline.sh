#!/bin/bash
# Gộp nhiều bước chạy pipeline vào 1 lệnh — cho agent gọi bằng ĐÚNG 1 tool (tiết kiệm token).
#
#   ./run-pipeline.sh "<toàn bộ tin nhắn lệnh>"
#
# Thực chất chỉ gọi fast_run.py: parse lệnh → sinh config (rules/font/NAS) → chạy wrapper
# → chờ done → in kết quả. Nếu lệnh không khớp mẫu fast-path thì in lỗi (agent tự xử lý tiếp).

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PYTHON_BIN="python3"
if [[ -x "$SCRIPT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$SCRIPT_DIR/.venv/bin/python"
fi

exec "$PYTHON_BIN" "$SCRIPT_DIR/fast_run.py" "$@"
