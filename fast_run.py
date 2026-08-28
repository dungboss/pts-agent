#!/usr/bin/env python3
"""Fast-path: chạy pipeline từ lệnh có cấu trúc mà KHÔNG cần gọi LLM.

Mục đích: tiết kiệm token DeepSeek. Những lệnh chạy pipeline lặp lại (ví dụ
"chạy tri script" + đầy đủ config) được parse bằng code thường rồi chạy thẳng
qua wrapper — không gọi `harness.run()`, tốn 0 token AI.

Cũng đóng vai trò "1 script gộp" (giải pháp #2): agent có thể gọi
`./run-pipeline.sh "<toàn bộ lệnh>"` bằng ĐÚNG 1 tool thay vì nhiều vòng.

Tối ưu NAS: khi template/output nằm trên NAS (`[NAS]/...`), tải template về
`local-run/tri/`, chạy local (nhanh hơn đọc WebDAV ~5–10×), rồi upload PNG kết
quả lên NAS bằng WebDAV PUT (`curl -T`).

Hiện chỉ hỗ trợ `tri`. Lệnh `age`/`mockup` trả None để fallback về LLM (an toàn
hơn là chạy sai), có thể mở rộng sau.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parent
LOCAL_RUN_DIR = ROOT / "local-run" / "tri"

# Giữ đồng bộ với DATA_SOURCES trong tri-script/tri-script.jsx.
DATA_SOURCES = {
    "phap": {
        "label": "Pháp",
        "csv": "fr_name.csv",
        "sheets": "https://docs.google.com/spreadsheets/d/1kVWACY3JnfUmF37FaQLjRrwysCKRH14uQRh73EpY6cM/edit?gid=0#gid=0",
    },
    "duc": {
        "label": "Đức",
        "csv": "de_name.csv",
        "sheets": "https://docs.google.com/spreadsheets/d/10B0orImwHdhs8pe51LXYVXS5Z2WuJtayN4WKXNkDKiE/edit?gid=0#gid=0",
    },
}

PIPELINE_DIRS = {"tri": "tri-script", "age": "age-script", "mockup": "mockup-script"}
FONT_EXTS = {".ttf", ".otf", ".ttc", ".dfont"}


class FastPathError(Exception):
    """Lỗi do fast-path không tự xử lý được — nên fallback về agent."""


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(int(os.environ.get(name, str(default))), minimum)
    except ValueError:
        return default


def _normalize_nas(value: str) -> str:
    """Chuẩn hoá các kiểu viết NAS: `[NAS: x`, `[NAS]: x`, `[NAS] x`, `[NAS]/x` → `[NAS]/x`."""
    value = value.strip()
    m = re.match(r"^\[?\s*NAS\s*\]?\s*[:/]?\s*(.+)$", value, re.IGNORECASE)
    if m:
        rest = m.group(1).strip().lstrip("/")
        return ("[NAS]/" + rest) if rest else "[NAS]"
    return value


def _field(text: str, keyword: str) -> Optional[str]:
    """Lấy giá trị sau keyword ở đầu dòng (chấp nhận 'keyword folder: value'...)."""
    m = re.search(
        r"(?m)^\s*" + re.escape(keyword) + r"\b\s*(?:folder\s*)?[:\-]?\s*(.+?)\s*$",
        text,
    )
    if not m:
        return None
    return m.group(1).strip()


def _limit(text: str) -> int:
    m = re.search(r"(?m)^\s*limit\b\s*[:\-]?\s*(\d+)", text, re.IGNORECASE)
    return int(m.group(1)) if m else 0


def _source_label(text: str) -> Optional[str]:
    low = text.lower()
    if re.search(r"\b(pháp|phap|france|fr)\b", low):
        return "phap"
    if re.search(r"\b(đức|duc|germany|de)\b", low):
        return "duc"
    return None


def _source_type(text: str) -> str:
    return "sheets" if re.search(r"\bsheets?\b", text, re.IGNORECASE) else "csv"


def parse_job(text: str) -> Optional[Dict]:
    """Nhận diện lệnh chạy có cấu trúc. Trả None nếu không đủ/không khớp (fallback LLM)."""
    t = text.strip()
    low = t.lower()
    if not re.search(r"\b(chạy|chay|run)\b", low):
        return None
    m = re.search(r"\b(tri|age|mockup)\b", low)
    if not m:
        return None
    pipeline = m.group(1)
    if pipeline != "tri":
        return None  # age/mockup chưa hỗ trợ fast-path

    template = _field(t, "template")
    output = _field(t, "output")
    formula = _field(t, "formula")
    # Cần đủ template + output + formula mới chạy được mà không cần hỏi lại.
    if not template or not output or not formula:
        return None

    label = _source_label(t)
    explicit_source = _field(t, "source")
    if explicit_source:
        source = explicit_source
        source_label = DATA_SOURCES.get(label, {}).get("label", "config") if label else "config"
    elif label:
        key = _source_type(t)
        source = DATA_SOURCES[label][key]
        source_label = DATA_SOURCES[label]["label"]
    else:
        return None  # thiếu nguồn → để agent hỏi

    job = {
        "pipeline": "tri",
        "source": source,
        "sourceLabel": source_label,
        "templateFolder": _normalize_nas(template),
        "outputFolder": _normalize_nas(output),
        "outputFormula": formula,
        "limit": _limit(t),
        "install_fonts": bool(re.search(r"\bfonts?\b", low)),
        "smoke": bool(re.search(r"\b(smoke|test)\b", low)),
    }
    if job["smoke"]:
        job["limit"] = 1  # smoke test: chỉ chạy 1 ảnh đầu
    return job


# --- NAS helpers -------------------------------------------------------------

def _load_nas_env(script_dir: Path) -> Dict[str, str]:
    """Đọc tri-script/.env (WEBDAV_USERNAME/PASSWORD + NAS_URL_1..N)."""
    env: Dict[str, str] = {}
    env_file = script_dir / ".env"
    if env_file.is_file():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def _mount_point(script_dir: Path) -> str:
    """Gọi nas-mount.sh, trả mount point (1 dòng)."""
    proc = subprocess.run(
        ["./nas-mount.sh"], cwd=str(script_dir), capture_output=True, text=True, timeout=240
    )
    if proc.returncode != 0:
        raise FastPathError("Không mount được NAS: " + (proc.stderr.strip()[-400:] or "lỗi không rõ"))
    lines = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
    if not lines:
        raise FastPathError("nas-mount.sh không trả mount point.")
    return lines[-1]


def _probe_webdav(url: str, user: str, pwd: str, timeout: int = 4) -> bool:
    proc = subprocess.run(
        [
            "curl", "-sS", "--connect-timeout", str(timeout), "--max-time", str(timeout * 3),
            "-u", "%s:%s" % (user, pwd), "-X", "PROPFIND", "-H", "Depth: 0",
            url, "-o", "/dev/null", "-w", "%{http_code}",
        ],
        capture_output=True, text=True, timeout=timeout * 3 + 5,
    )
    return proc.stdout.strip() == "207"


def _pick_webdav_url(env: Dict[str, str]) -> Optional[str]:
    """Chọn tuyến WebDAV đầu tiên PROPFIND được (cùng thứ tự nas-mount.sh)."""
    user = env.get("WEBDAV_USERNAME", "")
    pwd = env.get("WEBDAV_PASSWORD", "")
    if not user:
        raise FastPathError("Thiếu WEBDAV_USERNAME trong tri-script/.env.")
    for i in range(1, 6):
        url = env.get("NAS_URL_%d" % i, "")
        if url and _probe_webdav(url, user, pwd):
            return url
    return None


def _copy_nas_template(src: Path, dst: Path) -> List[str]:
    """Copy .psd + font từ template NAS về local (top-level + fonts/)."""
    if not src.is_dir():
        raise FastPathError("Template trên NAS không tồn tại: %s" % src)
    copied: List[str] = []
    seen = set()

    def _copy_file(f: Path, target_dir: Path) -> None:
        if (f.suffix.lower() == ".psd" or f.suffix.lower() in FONT_EXTS) and f.name not in seen:
            seen.add(f.name)
            shutil.copy2(str(f), str(target_dir / f.name))
            copied.append(f.name)

    for f in sorted(src.iterdir()):
        if f.is_file():
            _copy_file(f, dst)
    for sub in ("fonts", "Fonts"):
        s = src / sub
        if s.is_dir():
            (dst / sub).mkdir(exist_ok=True)
            for f in sorted(s.iterdir()):
                if f.is_file():
                    _copy_file(f, dst / sub)
    if not any(dst.glob("*.psd")):
        raise FastPathError("Không thấy .psd trong template trên NAS: %s" % src)
    return copied


def _ensure_webdav_dir(base_url: str, rel: str, user: str, pwd: str) -> None:
    """Tạo từng cấp thư mục trên NAS (MKCOL) — bỏ qua lỗi 'đã tồn tại'."""
    cur = base_url.rstrip("/")
    for part in rel.strip("/").split("/"):
        cur += "/" + part
        subprocess.run(
            ["curl", "-sS", "-X", "MKCOL", "-u", "%s:%s" % (user, pwd), cur],
            capture_output=True, text=True, timeout=60,
        )


def _upload_pngs(local_dir: Path, base_url: str, rel: str, user: str, pwd: str) -> int:
    """Upload từng PNG bằng WebDAV PUT (curl -T). Trả số file đã upload."""
    count = 0
    for f in sorted(local_dir.iterdir()):
        if f.suffix.lower() != ".png":
            continue
        url = "%s/%s/%s" % (base_url.rstrip("/"), rel.strip("/"), f.name)
        proc = subprocess.run(
            ["curl", "-sS", "--fail", "-T", str(f), "-u", "%s:%s" % (user, pwd), url],
            capture_output=True, text=True, timeout=120,
        )
        if proc.returncode != 0:
            raise FastPathError(
                "Upload thất bại %s: %s" % (f.name, (proc.stderr.strip() or proc.stdout.strip())[-200:])
            )
        count += 1
    return count


# --- helpers dùng chung ------------------------------------------------------

def _resolve_path(value: str, script_dir: Path, mount_point: Optional[str]) -> str:
    if value.startswith("[NAS]"):
        if not mount_point:
            raise FastPathError("Đường dẫn dùng [NAS] nhưng chưa có mount point.")
        return str(Path(mount_point) / value[len("[NAS]"):].lstrip("/"))
    p = Path(value).expanduser()
    if not p.is_absolute():
        p = (script_dir / p).resolve()
    return str(p)


def scan_rules(folder: Path) -> List[Dict]:
    """Quét .psd trong thư mục template, lấy số ở đuôi tên file làm độ dài tên."""
    if not folder.is_dir():
        raise FastPathError("Template folder không tồn tại: %s" % folder)
    rules: List[Dict] = []
    for f in sorted(folder.iterdir()):
        if f.suffix.lower() != ".psd":
            continue
        m = re.search(r"(\d+)\s*\.psd$", f.name, re.IGNORECASE)
        if not m:
            continue
        rules.append({"min": int(m.group(1)), "max": int(m.group(1)), "template": f.name})
    rules.sort(key=lambda r: (r["min"], r["template"]))
    if not rules:
        raise FastPathError(
            "Không tìm thấy file .psd đánh số (vd 2.psd..11.psd) trong template — để agent xử lý."
        )
    return rules


def install_fonts(folder: Path) -> List[str]:
    """Copy font (.ttf/.otf/.ttc/.dfont) trong template → ~/Library/Fonts (macOS)."""
    font_dir = Path.home() / "Library" / "Fonts"
    font_dir.mkdir(parents=True, exist_ok=True)
    targets = [folder, folder / "fonts", folder / "Fonts"]
    installed: List[str] = []
    seen = set()
    for base in targets:
        if not base.is_dir():
            continue
        for f in base.iterdir():
            if f.suffix.lower() not in FONT_EXTS or f.name in seen:
                continue
            seen.add(f.name)
            try:
                shutil.copy2(str(f), str(font_dir / f.name))
                installed.append(f.name)
            except OSError:
                continue
    return installed


def _count_png(folder: Path) -> int:
    try:
        return sum(1 for f in folder.iterdir() if f.suffix.lower() == ".png")
    except OSError:
        return 0


def run_job(job: Dict, log: Optional[Callable[[str], None]] = None) -> str:
    """Chạy job tri và trả về báo cáo ngắn gọn. Không gọi LLM."""
    def emit(msg: str) -> None:
        if log:
            log(msg)

    script_dir = ROOT / "tri-script"
    template_in = job["templateFolder"]
    output_in = job["outputFolder"]
    needs_nas = template_in.startswith("[NAS]") or output_in.startswith("[NAS]")

    emit("📋 Fast-path: phân tích lệnh (không qua AI)...")

    upload_target: Optional[Tuple[str, str, str, str]] = None  # (base_url, user, pwd, rel)
    report_output: str = ""

    try:
        if needs_nas:
            emit("🗂 NAS: mount + chuẩn bị chạy local (tải template về, upload kết quả lên)...")
            mount_point = _mount_point(script_dir)
            nas_env = _load_nas_env(script_dir)
            user = nas_env.get("WEBDAV_USERNAME", "")
            pwd = nas_env.get("WEBDAV_PASSWORD", "")

            if LOCAL_RUN_DIR.exists():
                shutil.rmtree(LOCAL_RUN_DIR, ignore_errors=True)
            LOCAL_RUN_DIR.mkdir(parents=True)

            if template_in.startswith("[NAS]"):
                nas_template = _resolve_path(template_in, script_dir, mount_point)
                local_template = LOCAL_RUN_DIR / "PTS"
                local_template.mkdir(parents=True)
                copied = _copy_nas_template(Path(nas_template), local_template)
                emit("✅ Tải template về local: %d file." % len(copied))
                template = str(local_template)
            else:
                template = _resolve_path(template_in, script_dir, None)

            if output_in.startswith("[NAS]"):
                local_output = LOCAL_RUN_DIR / "Result"
                local_output.mkdir(parents=True)
                output = str(local_output)
                rel = output_in[len("[NAS]"):].lstrip("/")
                base_url = _pick_webdav_url(nas_env)
                if not base_url:
                    raise FastPathError("Không tìm được tuyến WebDAV để upload kết quả.")
                upload_target = (base_url, user, pwd, rel)
                report_output = output_in
            else:
                output = _resolve_path(output_in, script_dir, None)
                report_output = output
        else:
            template = _resolve_path(template_in, script_dir, None)
            output = _resolve_path(output_in, script_dir, None)
            report_output = output

        rules = scan_rules(Path(template))
        cfg = {
            "source": job["source"],
            "sourceLabel": job["sourceLabel"],
            "templateFolder": template,
            "outputFolder": output,
            "outputFormula": job["outputFormula"],
            "limit": job["limit"],
            "rules": rules,
        }
        config_path = script_dir / ".fastpath-tri-config.json"
        config_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        emit(
            "✅ Config: %s | %d rule template | limit=%s | source=%s"
            % (cfg["sourceLabel"], len(rules), cfg["limit"], cfg["source"])
        )

        if job.get("install_fonts"):
            fonts = install_fonts(Path(template))
            if fonts:
                emit("🔤 Đã cài font: " + ", ".join(fonts))

        emit("🚀 Chạy run-tri.sh (có thể mất nhiều phút)...")
        timeout = _env_int("FASTPATH_TIMEOUT_SEC", 21600)
        try:
            proc = subprocess.run(
                ["./run-tri.sh", str(config_path)],
                cwd=str(script_dir),
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            return "❌ Timeout — tri (quá %ds, Photoshop có thể kẹt; gửi /cancel rồi kiểm tra)." % timeout

        done_path = script_dir / "tri-run.done"
        status = ""
        if done_path.is_file():
            try:
                status = done_path.read_text(encoding="utf-8").strip()
            except OSError:
                status = "?"
        count = _count_png(Path(output))
        tail = (proc.stdout or "").strip()[-1200:]

        success = proc.returncode == 0 and status == "OK"

        if upload_target and success:
            emit("⬆️ Upload kết quả lên NAS...")
            base_url, user, pwd, rel = upload_target
            _ensure_webdav_dir(base_url, rel, user, pwd)
            uploaded = _upload_pngs(Path(output), base_url, rel, user, pwd)
            emit("✅ Đã upload %d ảnh lên NAS." % uploaded)

        lines = []
        if success:
            lines.append("✅ Xong — tri: %d ảnh, exit OK." % count)
        elif proc.returncode == 130:
            lines.append("⛔ Đã huỷ — tri.")
        else:
            lines.append("❌ Lỗi — tri (exit %s, done=%s)." % (proc.returncode, status or "MISSING"))
        lines.append("Output: %s" % report_output)
        if not success and proc.returncode not in (0, 130) and tail:
            lines.append("Log (cuối):\n" + tail)
        return "\n".join(lines)
    finally:
        if needs_nas:
            shutil.rmtree(LOCAL_RUN_DIR, ignore_errors=True)


def main(argv: List[str]) -> int:
    text = " ".join(argv[1:]).strip()
    job = parse_job(text)
    if job is None:
        print(
            "Không nhận diện được lệnh chạy.\n"
            "Fast-path chỉ nhận lệnh 'chạy tri ...' với đủ template/output/formula/nguồn.",
            file=sys.stderr,
        )
        return 2
    try:
        print(run_job(job, log=print))
        return 0
    except FastPathError as exc:
        print("Lỗi fast-path: %s" % exc, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
