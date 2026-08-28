#!/usr/bin/env python3
"""Telegram gateway — cầu nối chat trực tiếp tới DeepSeek Harness.

Bot Telegram dùng long polling, chỉ chuyển tiếp tin nhắn từ các chat được phép
tới DeepSeek Harness agent (deepseek-harness-sdk). Agent giữ session theo từng
chat và dùng tool trong workspace (đọc file, chạy wrapper, ...) y hệt khi chat
trực tiếp với Harness. Khi agent trả lời, câu trả lời được gửi về Telegram.

Không còn wizard /run tri|age|mockup hay các lệnh pipeline — mọi tin nhắn đều
đi thẳng tới agent. Chỉ giữ /reset để xóa lịch sử hội thoại AI của chat.

Phần Telegram chỉ dùng Python standard library; cần deepseek-harness-sdk trong
.venv (xem requirements.txt).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shlex
import shutil
import signal
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

try:
    from deepseek_harness import DeepSeekHarness
except ImportError:
    DeepSeekHarness = None  # type: ignore[assignment]

try:
    import fast_run
except Exception:  # noqa: BLE001 — fast-path là tùy chọn, không làm sập bot
    fast_run = None  # type: ignore[assignment]


ROOT = Path(__file__).resolve().parent
ENV_FILE = ROOT / ".env"
OFFSET_FILE = ROOT / "telegram-offset.json"
CANCEL_FLAG = ROOT / ".cancel-flag"
MAX_MESSAGE_LENGTH = 3900
# Some installs/OS trust stores lack the GoDaddy root that api.telegram.org uses.
# When present, trust only this one extra root (kept out of git) — no other
# sites' TLS verification changes.
# Verified 2026-08-27 against https://certs.godaddy.com/repository/gdroot-g2.crt
CA_FILE = ROOT / "telegram-ca.pem"
GODADDY_ROOT_G2_FP = "45:14:0B:32:47:EB:9C:C8:C5:B4:F0:D7:B5:30:91:F7:32:92:08:9E:6E:5A:63:E2:74:9D:D3:AC:A9:19:8E:DA"


def env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(int(os.environ.get(name, str(default))), minimum)
    except ValueError:
        return default


def load_env_file(path: Path) -> None:
    """Load simple KEY=VALUE lines without requiring python-dotenv."""
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ.setdefault(key, value)


class TelegramAPI:
    def __init__(self, token: str, request_timeout: int = 40) -> None:
        self.base_url = "https://api.telegram.org/bot" + token + "/"
        self.request_timeout = request_timeout
        self.ssl_context = self._build_ssl_context()

    @staticmethod
    def _build_ssl_context() -> Optional[ssl.SSLContext]:
        """System trust, plus the pinned GoDaddy root when telegram-ca.pem is present."""
        ctx = ssl.create_default_context()
        if not CA_FILE.is_file():
            return ctx
        try:
            data = CA_FILE.read_bytes()
        except OSError:
            logging.getLogger(__name__).warn("Cannot read %s; using system trust only", CA_FILE)
            return ctx
        try:
            cert = ssl.PEM_cert_to_DER_cert(data.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            logging.getLogger(__name__).warn("Invalid CA file %s (%s); using system trust only", CA_FILE, exc)
            return ctx
        import hashlib
        fp = ":".join("%02X" % b for b in hashlib.sha256(cert).digest())
        if fp != GODADDY_ROOT_G2_FP:
            logging.getLogger(__name__).warn("CA file %s does not match pinned GoDaddy root; ignoring", CA_FILE)
            return ctx
        try:
            ctx.load_verify_locations(cadata=data.decode("utf-8"))
            return ctx
        except ssl.SSLError as exc:
            logging.getLogger(__name__).warn("Cannot use %s (%s); using system trust only", CA_FILE, exc)
            return ctx

    def call(self, method: str, payload: Optional[Dict[str, Any]] = None) -> Any:
        data = json.dumps(payload or {}).encode("utf-8")
        request = urllib.request.Request(
            self.base_url + method,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.request_timeout, context=self.ssl_context) as response:
                result = json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise RuntimeError("Telegram API %s failed: %s" % (method, exc)) from exc
        if not result.get("ok"):
            raise RuntimeError("Telegram API %s failed: %s" % (method, result.get("description", result)))
        return result.get("result")

    def get_updates(self, offset: Optional[int], timeout: int) -> List[Dict[str, Any]]:
        return self.call(
            "getUpdates",
            {"offset": offset, "timeout": timeout, "allowed_updates": ["message"]},
        ) or []

    def send_message(self, chat_id: int, text: str) -> None:
        self.call("sendMessage", {"chat_id": chat_id, "text": text})


def split_message(text: str, limit: int = MAX_MESSAGE_LENGTH) -> Iterable[str]:
    if not text:
        yield "(không có output)"
        return
    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut < limit // 2:
            cut = limit
        yield text[:cut]
        text = text[cut:].lstrip("\n")
    yield text


def parse_allowed_chat_ids(raw: str) -> Set[int]:
    result: Set[int] = set()
    for item in raw.split(","):
        item = item.strip()
        if item:
            result.add(int(item))
    return result


def parse_command(text: str) -> Tuple[str, List[str]]:
    try:
        parts = shlex.split(text)
    except ValueError:
        parts = text.split()
    if not parts:
        return "", []
    command = parts[0].lstrip("/").split("@", 1)[0].lower()
    return command, parts[1:]


PROJECTS = {
    "tri": {"label": "TRI", "dir": "tri-script", "log": "tri-run.log", "done": "tri-run.done", "config": "tri-config.json"},
    "age": {"label": "AGE", "dir": "age-script", "log": "age-run.log", "done": "age-run.done", "config": "age-config.json"},
    "mockup": {"label": "MOCKUP", "dir": "mockup-script", "log": "mockup-run.log", "done": "mockup-run.done", "config": "mockup-config.json"},
}


def _project_dir(project: Dict[str, str]) -> Path:
    return ROOT / project["dir"]


STALE_LOG_SECONDS = 600  # log không được ghi trong 10 phút → coi như script đã dừng


def _detect_running_project() -> Optional[str]:
    """Trả về key pipeline đang chạy (log tồn tại, done chưa có, log còn mới) — hoặc None."""
    best_key: Optional[str] = None
    best_mtime = -1.0
    now = time.time()
    for key, project in PROJECTS.items():
        log_path = _project_dir(project) / project["log"]
        done_path = _project_dir(project) / project["done"]
        if not log_path.is_file() or done_path.is_file():
            continue
        try:
            mtime = log_path.stat().st_mtime
        except OSError:
            continue
        if now - mtime > STALE_LOG_SECONDS:
            continue  # log cũ — script đã bị giết giữa chừng, thiếu done
        if mtime > best_mtime:
            best_mtime = mtime
            best_key = key
    if best_key is not None:
        return best_key
    # Fallback: wrapper process đang chạy nhưng log chưa có / log cũ (vd Photoshop
    # mới khởi động, hoặc wrapper kẹt poll). Vẫn báo là script đang chạy.
    for line in _running_wrapper_cmdlines():
        for key in PROJECTS:
            if re.search(r"run-%s" % re.escape(key), line):
                return key
    return None


def _progress_message() -> Optional[str]:
    """Đọc dòng 'PROGRESS x/y' gần nhất trong log pipeline đang chạy."""
    key = _detect_running_project()
    if key is None:
        return None
    project = PROJECTS[key]
    log_path = _project_dir(project) / project["log"]
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return "%s đang chạy." % project["label"]
    matches = re.findall(r"PROGRESS (\d+)/(\d+)", text)
    if not matches:
        return "%s đang chạy." % project["label"]
    done = int(matches[-1][0])
    total = int(matches[-1][1])
    pct = (done * 100 // total) if total else 0
    return "%s: đã chạy %d/%d (%d%%)." % (project["label"], done, total, pct)


WRAPPER_PATTERNS = ["run-tri.sh", "run-age.sh", "run-mockup.sh"]
OSASCRIPT_PATTERNS = ["tri-script.jsx", "age-script.jsx", "mockup-script.jsx"]
PHOTOSHOP_PATTERN = "Adobe Photoshop 2025.app/Contents/MacOS/Adobe Photoshop 2025"


def _pgrep(pattern: str) -> List[int]:
    try:
        out = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True, timeout=10).stdout
    except (OSError, subprocess.SubprocessError):
        return []
    return [int(p) for p in out.split() if p.strip().isdigit()]


def _running_wrapper_cmdlines() -> List[str]:
    """Command lines của tiến trình wrapper đang chạy (run-*.sh / run-*.bat).

    Dùng để phát hiện script đang chạy kể cả khi log pipeline chưa có (vd Photoshop
    mới khởi động) hoặc log bị giết giữa chừng. Chỉ soi cmd.exe/bash.exe để tránh
    nhầm với chính lệnh kiểm tra của người dùng/agent.
    """
    if sys.platform == "win32":
        try:
            out = subprocess.run(
                [
                    "powershell", "-NoProfile", "-NonInteractive", "-Command",
                    "Get-CimInstance Win32_Process -Filter \"Name='cmd.exe' or Name='bash.exe'\" | "
                    "Where-Object { $_.CommandLine -match 'run-(tri|age|mockup)' } | "
                    "ForEach-Object { $_.CommandLine }",
                ],
                capture_output=True, text=True, timeout=15,
            ).stdout
        except (OSError, subprocess.SubprocessError):
            return []
        return [line for line in out.splitlines() if line.strip()]
    try:
        out = subprocess.run(["pgrep", "-af", "run-(tri|age|mockup)"], capture_output=True, text=True, timeout=10).stdout
    except (OSError, subprocess.SubprocessError):
        return []
    return [line for line in out.splitlines() if line.strip()]


def _windows_pids_matching(regex: str, name_only: bool = False) -> List[int]:
    """Windows: PID của process khớp regex (Name hoặc CommandLine).

    name_only=True → chỉ soi tên process (dùng cho Photoshop.exe, tránh nhầm với
    lệnh kiểm tra của người dùng có chứa chữ 'Photoshop' trong command line).
    """
    safe = regex.replace("'", "''")
    if name_only:
        cond = "$_.Name -match '%s'" % safe
    else:
        cond = "$_.Name -match '%s' -or $_.CommandLine -match '%s'" % (safe, safe)
    script = (
        "Get-CimInstance Win32_Process | Where-Object { %s } | "
        "ForEach-Object { $_.ProcessId }" % cond
    )
    try:
        out = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
            capture_output=True, text=True, timeout=15,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return []
    return [int(p) for p in out.split() if p.strip().isdigit()]


def _kill_windows_pids(pids: List[int]) -> None:
    """Windows: kill process kèm cây con (taskkill /T /F) — /T để diệt cả script con."""
    for pid in pids:
        try:
            subprocess.run(["taskkill", "/T", "/F", "/PID", str(pid)], capture_output=True, timeout=15)
        except (OSError, subprocess.SubprocessError):
            pass


def _kill_pids(pids: List[int], sig: int) -> None:
    for pid in pids:
        try:
            os.kill(pid, sig)
        except (ProcessLookupError, PermissionError, OSError):
            pass


def cancel_running_script() -> List[str]:
    """Dừng wrapper, osascript và Photoshop (JSX chạy bên trong Photoshop)."""
    stopped = []
    if sys.platform == "win32":
        # Wrapper (cmd/bash chạy run-*.bat) + JSX chạy BÊN TRONG Photoshop
        # → phải dừng Photoshop mới dừng được script thật sự.
        for key in ("tri", "age", "mockup"):
            pids = _windows_pids_matching(r"run-%s" % key)
            if pids:
                _kill_windows_pids(pids)
                stopped.append(key)
        ps_pids = _windows_pids_matching(r"^Photoshop\.exe$", name_only=True)
        if ps_pids:
            _kill_windows_pids(ps_pids)
            time.sleep(2)
            still = _windows_pids_matching(r"^Photoshop\.exe$", name_only=True)
            if still:
                _kill_windows_pids(still)
            stopped.append("Photoshop")
        return stopped
    for pattern in WRAPPER_PATTERNS + OSASCRIPT_PATTERNS:
        pids = _pgrep(pattern)
        if not pids:
            continue
        _kill_pids(pids, signal.SIGTERM)
        label = pattern.replace(".jsx", "").replace("run-", "").replace(".sh", "")
        if label not in stopped:
            stopped.append(label)
    # JSX chạy BÊN TRONG Photoshop → phải dừng Photoshop mới dừng được script thật sự.
    ps_pids = _pgrep(PHOTOSHOP_PATTERN)
    if ps_pids:
        _kill_pids(ps_pids, signal.SIGTERM)
        time.sleep(2)
        still = _pgrep(PHOTOSHOP_PATTERN)
        if still:
            _kill_pids(still, signal.SIGKILL)
        if "Photoshop" not in stopped:
            stopped.append("Photoshop")
    return stopped


def _photoshop_watchdog() -> None:
    """Sau /cancel, giữ Photoshop tắt trong 60s — trừ khi người dùng gửi lệnh chạy mới (xoá cờ)."""
    deadline = time.time() + 60
    while time.time() < deadline:
        if not CANCEL_FLAG.exists():
            return
        if sys.platform == "win32":
            ps_pids = _windows_pids_matching(r"^Photoshop\.exe$", name_only=True)
            if ps_pids:
                _kill_windows_pids(ps_pids)
        else:
            ps_pids = _pgrep(PHOTOSHOP_PATTERN)
            if ps_pids:
                _kill_pids(ps_pids, signal.SIGKILL)
        time.sleep(3)


OWNER_FILE = ROOT / ".telegram-owner.json"


def _load_last_owner() -> Optional[int]:
    """Chat nhận update gần nhất (để sau khi restart bot, script đang chạy vẫn có người nhận update)."""
    try:
        return int(json.loads(OWNER_FILE.read_text(encoding="utf-8")).get("chat_id"))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def _save_last_owner(chat_id: int) -> None:
    try:
        OWNER_FILE.write_text(json.dumps({"chat_id": chat_id}), encoding="utf-8")
    except OSError:
        pass


def _cleanup_pipeline_temp(key: str) -> None:
    """Xoá file tạm Photoshop ở top-level thư mục output sau khi pipeline chạy xong.

    Khớp với hướng dẫn 'Dọn file tạm' trong AGENTS.md: xoá `._*` (AppleDouble của
    macOS khi ghi lên NAS) và `*.sb-*` (file tạm 0 byte của Save-for-Web).
    Output lấy từ `. <key>-config-resolved.json` (đường dẫn thật, [NAS] đã thay);
    không resolve được thì bỏ qua, không đoán đường dẫn.
    """
    project = PROJECTS[key]
    pdir = _project_dir(project)
    output: Any = None
    try:
        resolved = pdir / (".%s-config-resolved.json" % key)
        if resolved.is_file():
            output = json.loads(resolved.read_text(encoding="utf-8")).get("outputFolder")
    except (OSError, ValueError):
        pass
    if not output:
        try:
            base = pdir / ("%s-config.json" % key)
            if base.is_file():
                output = json.loads(base.read_text(encoding="utf-8")).get("outputFolder")
        except (OSError, ValueError):
            pass
    if not output or "[NAS]" in str(output):
        return
    out = Path(str(output))
    if not out.is_absolute():
        out = pdir / out
    if not out.is_dir():
        return
    removed = 0
    try:
        for entry in out.iterdir():
            if not entry.is_file():
                continue
            if entry.name.startswith("._") or ".sb-" in entry.name:
                try:
                    entry.unlink()
                    removed += 1
                except OSError:
                    pass
    except OSError:
        return
    if removed:
        logging.info("Đã dọn %d file tạm Photoshop trong %s", removed, out)


def _start_progress_monitor(ai_agent: "DeepSeekAgent") -> None:
    """Thread nền: khi có script pipeline đang chạy, mỗi TELEGRAM_PROGRESS_INTERVAL_SEC
    gửi 1 tin cập nhật trạng thái (📊 ...) tới chat chủ.

    Hoạt động độc lập với lượt agent — kể cả khi lượt đã kết thúc mà script vẫn
    chạy nền (runtime restart / bash tool trả về sớm), update vẫn được gửi.
    """
    interval = env_int("TELEGRAM_PROGRESS_INTERVAL_SEC", 300, 10)
    check_every = max(10, min(interval, 60))
    state: Dict[str, Any] = {"project": None, "owner": None, "started": 0.0, "last_sent": 0.0}

    def _pick_owner() -> Optional[int]:
        busy = list(ai_agent.busy_chats)
        if len(busy) == 1:
            return busy[0]
        chats = list(ai_agent.session_ids)
        if len(chats) == 1:
            return chats[0]
        return _load_last_owner()

    def _loop() -> None:
        cleaned_done: Dict[str, float] = {}
        while True:
            try:
                now = time.time()
                key = _detect_running_project()
                # 1) Dọn file tạm: pipeline nào vừa ghi *.done (run xong) → dọn output
                #    một lần cho từng run (theo mtime của done). Chạy thread riêng để
                #    không block vòng lặp khi output là NAS chậm/không với tới.
                for pkey, project in PROJECTS.items():
                    done_path = _project_dir(project) / project["done"]
                    try:
                        mtime = done_path.stat().st_mtime
                    except OSError:
                        continue
                    if cleaned_done.get(pkey) != mtime:
                        cleaned_done[pkey] = mtime
                        threading.Thread(target=_cleanup_pipeline_temp, args=(pkey,), daemon=True).start()
                # 2) Update trạng thái mỗi 5 phút khi có script chạy
                if key is None:
                    state["project"] = None
                    state["owner"] = None
                    state["last_sent"] = 0.0
                else:
                    if state["project"] != key:
                        # Script mới bắt đầu (hoặc đổi pipeline) — ghi nhớ chat chủ.
                        state["project"] = key
                        state["started"] = now
                        state["last_sent"] = now
                        state["owner"] = _pick_owner()
                    owner = state["owner"]
                    if owner is not None:
                        elapsed = now - state["started"]
                        if elapsed >= interval and now - state["last_sent"] >= interval:
                            message = _progress_message()
                            if message:
                                ai_agent.telegram_api.send_message(
                                    owner, "📊 %s (đã %d phút)" % (message, int(elapsed) // 60)
                                )
                                state["last_sent"] = now
                                _save_last_owner(owner)
            except Exception:
                logging.exception("Progress monitor error")
            time.sleep(check_every)

    threading.Thread(target=_loop, daemon=True, name="progress-monitor").start()


def status_message(ai_agent: Optional["DeepSeekAgent"], chat_id: int) -> str:
    """Tổng hợp trạng thái: agent, script đang chạy, và kết quả cuối mỗi pipeline."""
    lines = []
    if ai_agent is not None:
        lines.append("Agent: " + ("đang xử lý yêu cầu" if ai_agent.is_busy(chat_id) else "sẵn sàng"))
        pending = ai_agent.pending_count(chat_id)
        lines.append("Hàng đợi: " + ("%d việc đang chờ." % pending if pending else "trống."))
    running = _detect_running_project()
    if running is not None:
        lines.append("Script: " + (_progress_message() or "%s đang chạy." % PROJECTS[running]["label"]))
    else:
        lines.append("Script: không có script nào đang chạy.")
    for key, project in PROJECTS.items():
        done_path = _project_dir(project) / project["done"]
        if done_path.is_file():
            try:
                status = done_path.read_text(encoding="utf-8").strip() or "?"
            except OSError:
                status = "?"
        else:
            status = "chưa chạy"
        lines.append("%s: trạng thái cuối=%s" % (project["label"], status))
    return "\n".join(lines)


AGENT_INSTRUCTIONS = """Bạn là agent DeepSeek Harness điều khiển project Photoshop qua Telegram. Trả lời bằng tiếng Việt.

Workspace của bạn là project hiện tại. Ba pipeline là age-script, mockup-script và tri-script. Khi người dùng muốn chạy, hãy hỏi đủ thông tin cấu hình rồi tóm tắt và chờ người dùng xác nhận rõ ràng trước khi sửa config hoặc chạy. Không tự đoán đường dẫn, năm, tháng, nguồn dữ liệu hay giới hạn chạy.

Cách hỏi: đưa thẳng các câu hỏi cần trả lời, ngắn gọn, đánh số. KHÔNG mở đầu bằng lời giải thích dài dòng kiểu "Chưa chạy được vì thiếu config...", "theo hướng dẫn AGENTS.md tôi bắt buộc phải hỏi", và KHÔNG liệt kê mục "Tình trạng hiện tại" trước câu hỏi.

Bạn có thể dùng các tool local của Harness trong workspace để đọc file, xem log và chạy wrapper. Chỉ chạy run-age.sh/run-mockup.sh/run-tri.sh sau khi người dùng xác nhận; không chạy shell tùy ý ngoài phạm vi project. Nếu người dùng gửi sẵn đầy đủ cấu hình (template, nguồn, output, limit…) trong tin nhắn, hãy ghi config rồi chạy wrapper NGAY — không đọc lại AGENTS.md, không ls/khám phá thêm, không hỏi lại. Nếu người dùng chỉ hỏi chuyện thông thường, trả lời tự nhiên. Khi job hoàn tất, chỉ báo NGẮN GỌN (1–3 dòng): ✅/❌ + tên pipeline + số ảnh đã xử lý (x/tổng) + exit code/done + đường dẫn output. KHÔNG viết dài dòng, KHÔNG thêm mục "lưu ý nhỏ" / "sự cố gặp phải" — chỉ nêu lỗi/ghi chú khi có lỗi thật sự hoặc người dùng yêu cầu chi tiết. Sau mỗi lần chạy xong, xoá các file tạm (._* và *.sb-*) trong thư mục output. Sau khi người dùng /cancel, KHÔNG được tự chạy lại wrapper, không tự mở/activate Photoshop (kể cả qua osascript hay open) — chỉ báo đã huỷ và chờ lệnh mới. Nếu nguồn (template/design) nằm trên NAS, hãy tải về local trước rồi chạy local, sau đó upload kết quả lên NAS — không để Photoshop đọc/ghi trực tiếp qua WebDAV. Với lệnh chạy tri đã đủ config (template/output/formula/limit), ưu tiên gọi ./run-pipeline.sh "<toàn bộ lệnh>" để chạy gộp trong 1 bước thay vì gọi nhiều tool.
"""


def _is_collision_error(result: Any) -> bool:
    """True khi turn kết thúc bằng lỗi id collision (runtime mới không nhận session cũ)."""
    if getattr(result, "finish_reason", None) != "error":
        return False
    for event in reversed(getattr(result, "events", None) or []):
        if event.get("type") != "turn/end":
            continue
        data = event.get("data") or {}
        reason = data.get("reason") if isinstance(data, dict) else None
        error = reason.get("error") if isinstance(reason, dict) else None
        if isinstance(error, dict) and "id collision" in str(error.get("message", "")):
            return True
    return False


class DeepSeekAgent:
    def __init__(
        self,
        telegram_api: TelegramAPI,
        api_key: str,
        model: str,
        max_tokens: int,
        request_timeout: int,
    ) -> None:
        if DeepSeekHarness is None:
            raise RuntimeError("Thiếu deepseek-harness-sdk trong môi trường Python")
        self.telegram_api = telegram_api
        self._api_key = api_key
        self._model = model
        self._max_tokens = max_tokens
        self._request_timeout = request_timeout
        self.session_ids: Dict[int, str] = {}
        self.queues: Dict[int, List[str]] = {}
        self.instance_id = uuid.uuid4().hex[:12]
        self.state_lock = threading.RLock()
        self.run_lock = threading.Lock()
        self.busy_chats: Set[int] = set()
        self._harness_dead = False
        self._session_generation = 0
        self._make_harness()

    def _next_session_id(self, chat_id: int) -> str:
        return "telegram-%s-%s-g%d" % (chat_id, self.instance_id, self._session_generation)

    def _make_harness(self) -> None:
        session_root = ROOT / ".deepseek-sessions"
        session_root.mkdir(parents=True, exist_ok=True)
        # deepseek-harness-runtime-bin không có bản Windows nên phải chỉ rõ runtime
        # Node (npm closure trong sdk-runtime/) qua launch_args_override + cordis.
        runtime_dir = ROOT / "sdk-runtime"
        packaged_bin = (
            runtime_dir
            / "node_modules"
            / "@deepseek-ai"
            / "dsh-sdk-jsonrpc-demo"
            / "lib"
            / "packaged-bin.js"
        )
        node = shutil.which("node")
        if node is None or not packaged_bin.is_file():
            raise RuntimeError(
                "Thiếu DeepSeek Harness SDK runtime (sdk-runtime/ chưa cài). "
                "Chạy: cd sdk-runtime && npm install"
            )
        env = {"DSH_SYSTEM_PROMPT": AGENT_INSTRUCTIONS}
        # Git Bash cho tool bash (wrapper .sh); không có thì runtime vẫn chạy,
        # nhưng agent không chạy được wrapper.
        git_bin = r"C:\Program Files\Git\bin"
        if os.path.isdir(git_bin) and git_bin not in os.environ.get("PATH", "").split(os.pathsep):
            env["PATH"] = os.environ.get("PATH", "") + os.pathsep + git_bin
        self.harness = DeepSeekHarness(
            provider=os.environ.get("DSH_PROVIDER", "deepseek-official"),
            model=self._model,
            max_tokens=self._max_tokens,
            cwd=str(ROOT),
            runtime_cwd=str(ROOT),
            session_root=str(session_root),
            api_key=self._api_key,
            base_url=os.environ.get("DEEPSEEK_BASE_URL") or None,
            launch_args_override=(node, str(packaged_bin)),
            cordis=str(runtime_dir / "cordis.yml"),
            env=env,
            request_timeout_seconds=self._request_timeout,
        )
        # Runtime mới không resume được session cũ đã lưu trên đĩa (id collision) —
        # mỗi lần tạo harness mới phải dùng session id mới cho mọi chat.
        self._session_generation += 1
        with self.state_lock:
            for chat_id in list(self.session_ids):
                self.session_ids[chat_id] = self._next_session_id(chat_id)

    def _ensure_harness(self) -> None:
        if self._harness_dead:
            self._make_harness()
            self._harness_dead = False

    def hard_cancel(self, chat_id: int) -> None:
        """Dừng ngay lượt agent hiện tại bằng cách đóng harness runtime (dùng cho /cancel)."""
        self._harness_dead = True
        try:
            self.harness.close()
        except Exception:
            pass
        # Đóng runtime giữa lượt chạy có thể làm log session bị ngắt giữa chừng (torn).
        # Bỏ id session cũ khi chat đang bận để lệnh kế tiếp tạo session mới, tránh
        # lỗi "id collision" khi resume log hỏng.
        with self.state_lock:
            if chat_id in self.busy_chats:
                self.session_ids[chat_id] = "telegram-%s-%s" % (chat_id, int(time.time()))

    def submit(self, chat_id: int, text: str) -> None:
        # Tin nhắn mới từ người dùng → bỏ cờ huỷ cũ (lần chạy hợp lệ kế tiếp sẽ chạy bình thường).
        try:
            if CANCEL_FLAG.exists():
                CANCEL_FLAG.unlink()
        except OSError:
            pass
        with self.state_lock:
            queue = self.queues.setdefault(chat_id, [])
            queue.append(text)
            if chat_id in self.busy_chats:
                self.telegram_api.send_message(chat_id, "📥 Đã xếp hàng (vị trí %d)." % len(queue))
                return
            self.busy_chats.add(chat_id)
        threading.Thread(target=self._drain_queue, args=(chat_id,), daemon=True).start()

    def _drain_queue(self, chat_id: int) -> None:
        while True:
            with self.state_lock:
                queue = self.queues.get(chat_id)
                if not queue:
                    self.queues.pop(chat_id, None)
                    self.busy_chats.discard(chat_id)
                    return
                text = queue.pop(0)
            self._answer(chat_id, text)

    def pending_count(self, chat_id: int) -> int:
        with self.state_lock:
            return len(self.queues.get(chat_id, []))

    def clear_queue(self, chat_id: int) -> int:
        with self.state_lock:
            n = len(self.queues.get(chat_id, []))
            self.queues[chat_id] = []
            return n

    def reset(self, chat_id: int) -> None:
        with self.state_lock:
            self.session_ids[chat_id] = "telegram-%s-%s" % (chat_id, int(time.time()))

    def is_busy(self, chat_id: int) -> bool:
        with self.state_lock:
            return chat_id in self.busy_chats

    def close(self) -> None:
        with self.state_lock:
            self.harness.close()

    def _answer(self, chat_id: int, text: str) -> None:
        try:
            self.telegram_api.send_message(chat_id, "DeepSeek Harness đang xử lý...")
            with self.state_lock:
                session_id = self.session_ids.setdefault(chat_id, self._next_session_id(chat_id))
            # Update trạng thái script mỗi 5 phút do thread progress-monitor đảm nhiệm
            # (độc lập lượt agent) — không còn vòng lặp trong lượt.
            self._ensure_harness()
            with self.run_lock:
                result = self.harness.run(text, session_id=session_id)
            if _is_collision_error(result):
                # Runtime đã khởi động lại (ví dụ sau /cancel): session id cũ đã có
                # log trên đĩa mà runtime mới không nhận. Tạo harness + session mới
                # rồi chạy lại một lần.
                logging.warning("Session id collision (chat %s); tạo session mới và chạy lại", chat_id)
                self._harness_dead = True
                self._make_harness()
                with self.state_lock:
                    session_id = self.session_ids.setdefault(chat_id, self._next_session_id(chat_id))
                with self.run_lock:
                    result = self.harness.run(text, session_id=session_id)
            answer = (result.final_response or "").strip()
            if not answer:
                detail = "finish_reason=%s" % (result.finish_reason or "unknown")
                for event in reversed(result.events):
                    if event.get("type") != "turn/end":
                        continue
                    data = event.get("data") or {}
                    reason = data.get("reason") if isinstance(data, dict) else None
                    error = reason.get("error") if isinstance(reason, dict) else None
                    if isinstance(error, dict) and error.get("message"):
                        detail += "; " + str(error["message"])
                    break
                answer = "DeepSeek Harness chưa tạo được câu trả lời (%s)." % detail
            for chunk in split_message(answer):
                self.telegram_api.send_message(chat_id, chunk)
        except Exception as exc:
            if self._harness_dead:
                self.telegram_api.send_message(chat_id, "Đã huỷ.")
            else:
                logging.exception("DeepSeek Harness agent error")
                self.telegram_api.send_message(chat_id, "DeepSeek Harness gặp lỗi: %s" % exc)


FASTPATH_LOCK = threading.Lock()


def _run_fastpath(api: TelegramAPI, chat_id: int, job: Dict[str, Any]) -> None:
    """Chạy job pipeline theo fast-path (không qua LLM) trong thread nền."""
    if not FASTPATH_LOCK.acquire(blocking=False):
        api.send_message(chat_id, "⏳ Đang có job pipeline khác chạy — hãy chờ xong rồi gửi lại.")
        return
    stop_progress = threading.Event()
    interval = env_int("TELEGRAM_PROGRESS_INTERVAL_SEC", 300, 10)
    started = time.time()

    def _progress_loop() -> None:
        while not stop_progress.wait(interval):
            message = _progress_message()
            if message is None:
                continue
            elapsed = int(time.time() - started) // 60
            try:
                api.send_message(chat_id, "📊 %s (đã %d phút)" % (message, elapsed))
            except Exception:
                logging.exception("Fast-path progress send failed")

    progress_thread = threading.Thread(target=_progress_loop, daemon=True)
    progress_thread.start()
    try:
        def send(msg: str) -> None:
            try:
                for chunk in split_message(msg):
                    api.send_message(chat_id, chunk)
            except Exception:
                logging.exception("Fast-path gửi Telegram lỗi")

        report = fast_run.run_job(job, log=send)
        for chunk in split_message(report):
            api.send_message(chat_id, chunk)
    except fast_run.FastPathError as exc:
        api.send_message(chat_id, "⚠️ Fast-path không chạy được: %s\nBạn có thể gửi lại để agent xử lý." % exc)
    finally:
        stop_progress.set()
        FASTPATH_LOCK.release()


def save_offset(offset: int) -> None:
    OFFSET_FILE.write_text(json.dumps({"offset": offset}), encoding="utf-8")


def load_offset() -> Optional[int]:
    try:
        return int(json.loads(OFFSET_FILE.read_text(encoding="utf-8")).get("offset"))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def discover_chat_ids(api: TelegramAPI) -> int:
    updates = api.get_updates(None, 0)
    ids = sorted({int(u["message"]["chat"]["id"]) for u in updates if u.get("message", {}).get("chat", {}).get("id") is not None})
    if ids:
        print("Telegram chat IDs:", ", ".join(str(i) for i in ids))
    else:
        print("Chưa có update. Hãy nhắn một tin cho bot rồi chạy lại lệnh này.")
    return 0


def handle_update(
    api: TelegramAPI,
    allowed_ids: Set[int],
    update: Dict[str, Any],
    ai_agent: Optional[DeepSeekAgent] = None,
) -> None:
    message = update.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    text = (message.get("text") or "").strip()
    if chat_id is None or not text:
        return
    chat_id = int(chat_id)
    if chat_id not in allowed_ids:
        logging.warning("Ignored message from unauthorized chat_id=%s", chat_id)
        return

    command, _args = parse_command(text)
    if command == "reset":
        if ai_agent is not None:
            ai_agent.reset(chat_id)
        api.send_message(chat_id, "Đã xóa lịch sử hội thoại AI của chat này.")
        return
    if command in {"status", "trangthai"}:
        api.send_message(chat_id, status_message(ai_agent, chat_id))
        return
    if command in {"cancel", "huy", "huỷ", "hủy"}:
        try:
            CANCEL_FLAG.touch()
        except OSError:
            pass
        if ai_agent is not None:
            ai_agent.hard_cancel(chat_id)
        stopped = cancel_running_script()
        threading.Thread(target=_photoshop_watchdog, daemon=True).start()
        cleared = ai_agent.clear_queue(chat_id) if ai_agent is not None else 0
        parts = []
        parts.append("Đã dừng: %s." % ", ".join(stopped) if stopped else "Không thấy script đang chạy.")
        if "Photoshop" in stopped:
            parts.append("Photoshop đã tắt — lần chạy sau sẽ tự mở lại khi cần.")
        if cleared:
            parts.append("Đã bỏ %d việc đang xếp hàng." % cleared)
        api.send_message(chat_id, " ".join(parts))
        return
    if fast_run is not None:
        job = fast_run.parse_job(text)
        if job is not None:
            api.send_message(chat_id, "🚀 Fast-path: chạy %s (không qua AI)." % job["pipeline"].upper())
            threading.Thread(target=_run_fastpath, args=(api, chat_id, job), daemon=True).start()
            return
    if ai_agent is None:
        api.send_message(chat_id, "Chưa cấu hình DEEPSEEK_API_KEY hoặc deepseek-harness-sdk nên agent chưa hoạt động.")
        return
    ai_agent.submit(chat_id, text)


def main() -> int:
    load_env_file(ENV_FILE)
    parser = argparse.ArgumentParser(description="Telegram gateway cho DeepSeek Harness (chat trực tiếp)")
    parser.add_argument("--show-chat-ids", action="store_true", help="in chat ID từ update đang chờ rồi thoát")
    args = parser.parse_args()

    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        print("Thiếu TELEGRAM_BOT_TOKEN. Tạo .env từ .env.example.", file=sys.stderr)
        return 2
    api = TelegramAPI(token)
    if args.show_chat_ids:
        return discover_chat_ids(api)

    raw_allowed = os.environ.get("TELEGRAM_ALLOWED_CHAT_IDS", "").strip()
    if not raw_allowed:
        print("Thiếu TELEGRAM_ALLOWED_CHAT_IDS; bot không khởi động để tránh nhận lệnh từ người lạ.", file=sys.stderr)
        print("Gửi tin nhắn cho bot, chạy: ./run-telegram.sh --show-chat-ids", file=sys.stderr)
        return 2
    try:
        allowed_ids = parse_allowed_chat_ids(raw_allowed)
    except ValueError:
        print("TELEGRAM_ALLOWED_CHAT_IDS phải là danh sách số, ví dụ: 123456789,-1001234567890", file=sys.stderr)
        return 2

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(ROOT / "telegram-bot.log", encoding="utf-8")],
    )
    try:
        # Polling và webhook cũ không thể dùng cùng lúc.
        api.call("deleteWebhook", {"drop_pending_updates": False})
        me = api.call("getMe")
        logging.info("Telegram gateway started as @%s", me.get("username", "unknown"))
    except RuntimeError as exc:
        logging.error("Không xác thực được bot token: %s", exc)
        return 2

    ai_agent: Optional[DeepSeekAgent] = None
    deepseek_key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
    if deepseek_key and DeepSeekHarness is not None:
        ai_agent = DeepSeekAgent(
            api,
            deepseek_key,
            os.environ.get("DSH_MODEL", "deepseek-v4-flash"),
            env_int("DSH_MAX_TOKENS", 32768),
            env_int("DSH_REQUEST_TIMEOUT_SEC", 7200),
        )
        logging.info("DeepSeek Harness agent enabled with model %s", os.environ.get("DSH_MODEL", "deepseek-v4-flash"))
        _start_progress_monitor(ai_agent)
    else:
        if not deepseek_key:
            logging.warning("DEEPSEEK_API_KEY is missing; chat sẽ không dùng DeepSeek Harness")
        else:
            logging.warning("deepseek-harness-sdk is missing; install requirements.txt")

    offset = load_offset()
    if offset is None:
        pending = api.get_updates(None, 0)
        offset = max((int(item["update_id"]) for item in pending), default=-1) + 1
        save_offset(offset)
        logging.info("Skipped %d old Telegram update(s)", len(pending))

    while True:
        try:
            updates = api.get_updates(offset, 25)
            for update in updates:
                offset = int(update["update_id"]) + 1
                save_offset(offset)
                handle_update(api, allowed_ids, update, ai_agent)
        except KeyboardInterrupt:
            logging.info("Telegram gateway stopped")
            if ai_agent is not None:
                ai_agent.close()
            return 0
        except Exception:
            logging.exception("Polling error; retrying in 5 seconds")
            time.sleep(5)


if __name__ == "__main__":
    raise SystemExit(main())
