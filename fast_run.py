#!/usr/bin/env python3
"""Fast-path: chạy pipeline từ lệnh có cấu trúc mà KHÔNG cần gọi LLM.

Mục đích: tiết kiệm token DeepSeek. Những lệnh chạy pipeline lặp lại (ví dụ
"chạy tri script" + đầy đủ config) được parse bằng code thường rồi chạy thẳng
qua wrapper — không gọi `harness.run()`, tốn 0 token AI.

Hỗ trợ 3 pipeline: `tri`, `age`, `mockup`. Cú pháp:

  chạy tri
  template: [NAS]/...          (hoặc templateFolder:)
  output: [NAS]/...            (hoặc outputFolder:)
  formula: [[name]][name]-xxx-[stt]
  pháp csv | đức sheets | source: <url/csv>
  limit: 2

  chạy age-script với
  templateFolder: [NAS]/...
  fromYear: 1990
  toYear: 1990
  months: 1
  outputFormula: SMOKETEST-[mm]-[year]
  outputFolder: [NAS]/...

  chạy mockup-script
  templateFolder: [NAS]/...
  designFolder: [NAS]/...
  outputFolder: [NAS]/...
  limit: 0

Khi output mockup nằm trên NAS, fast-path tự chạy theo batch 100 design, upload
từng batch rồi đóng Photoshop hoàn toàn trước khi mở lại cho batch kế tiếp.
Lỗi Photoshop, timeout, done khác OK hoặc lỗi upload sẽ tự thử lại tối đa 3 lần
cho cùng job/batch; tri và age dùng `PHOTOSHOP_MAX_ATTEMPTS`, mockup có thể
ghi đè bằng `MOCKUP_BATCH_MAX_ATTEMPTS`.

Cũng đóng vai trò "1 script gộp": agent có thể gọi `./run-pipeline.sh "<toàn bộ lệnh>"`
bằng ĐÚNG 1 tool thay vì nhiều vòng.

Tối ưu NAS: khi template/design/output nằm trên NAS (`[NAS]/...`), tải về
`local-run/<pipeline>/`, chạy local (nhanh hơn đọc WebDAV ~5–10×), rồi upload
kết quả lên NAS bằng WebDAV PUT (`curl -T`).
"""

from __future__ import annotations

import glob
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import unquote
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent

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
DONE_FILES = {"tri": "tri-run.done", "age": "age-run.done", "mockup": "mockup-run.done"}
CONFIG_FILES = {
    "tri": ".fastpath-tri-config.json",
    "age": ".fastpath-age-config.json",
    "mockup": ".fastpath-mockup-config.json",
}
# field -> (thư mục con trong local-run, loại). "output" là thư mục kết quả.
PIPELINE_FOLDERS = {
    "tri": {
        "templateFolder": ("PTS", "template"),
        "outputFolder": ("Result", "output"),
    },
    "age": {
        "templateFolder": ("PTS", "template"),
        "outputFolder": ("Result", "output"),
    },
    "mockup": {
        "templateFolder": ("PTS", "template"),
        "designFolder": ("Design", "design"),
        "outputFolder": ("Result", "output"),
    },
}
FONT_EXTS = {".ttf", ".otf", ".ttc", ".dfont"}
# Mockup chỉ nhận đúng hai định dạng design này; phải đồng bộ với mockup-script.jsx.
DESIGN_EXTS = {".jpg", ".png"}
MOCKUP_BATCH_SIZE = 100
MOCKUP_BATCH_MAX_ATTEMPTS = 3
PHOTOSHOP_MAX_ATTEMPTS = 3
IS_WINDOWS = os.name == "nt"


def _local_run_dir(pipeline: str) -> Path:
    return ROOT / "local-run" / pipeline


def _curl_bin() -> str:
    """curl.exe trên Windows (tránh alias PowerShell), curl trên macOS/Linux."""
    return "curl.exe" if IS_WINDOWS else "curl"


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
        r"(?m)^\s*[-*•]?\s*" + re.escape(keyword) + r"\b\s*(?:folder\s*)?[:\-]?\s*(.+?)\s*$",
        text,
    )
    if not m:
        return None
    return m.group(1).strip()


def _field_ci(text: str, keyword: str) -> Optional[str]:
    """Như _field nhưng không phân biệt hoa thường (cho camelCase: templateFolder...)."""
    m = re.search(
        r"(?m)^\s*[-*•]?\s*" + re.escape(keyword) + r"\b\s*[:\-]?\s*(.+?)\s*$",
        text,
        re.IGNORECASE,
    )
    if not m:
        return None
    return m.group(1).strip()


def _field_any(text: str, *keywords: str) -> Optional[str]:
    for kw in keywords:
        v = _field_ci(text, kw)
        if v:
            return v
    return None


def _int_field_ci(text: str, keyword: str) -> Optional[int]:
    m = re.search(
        r"(?m)^\s*[-*•]?\s*" + re.escape(keyword) + r"\b\s*[:\-]?\s*(\d+)",
        text,
        re.IGNORECASE,
    )
    return int(m.group(1)) if m else None


def _int_field_any(text: str, *keywords: str) -> Optional[int]:
    for kw in keywords:
        v = _int_field_ci(text, kw)
        if v is not None:
            return v
    return None


def _limit(text: str) -> int:
    m = re.search(r"(?m)^\s*[-*•]?\s*limit\b\s*[:\-]?\s*(\d+)", text, re.IGNORECASE)
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


def _months_field(text: str) -> Optional[List[int]]:
    """Parse 'months'. Trả None = tất cả tháng; ngược lại trả list số tháng 1..12."""
    v = _field_ci(text, "months")
    if v is None:
        return None
    v = v.strip().lower()
    if v in ("", "tất cả", "tat ca", "all", "hết", "het", "tự dò", "auto"):
        return None
    nums = sorted(set(int(x) for x in re.findall(r"\d+", v) if 1 <= int(x) <= 12))
    return nums if nums else None


# ---------------------------------------------------------------------------
# parse_job — nhận diện lệnh chạy có cấu trúc cho từng pipeline
# ---------------------------------------------------------------------------

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
    if pipeline == "tri":
        return _parse_tri(t, low)
    if pipeline == "age":
        return _parse_age(t, low)
    return _parse_mockup(t, low)


def _parse_tri(t: str, low: str) -> Optional[Dict]:
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
        "smoke": bool(re.search(r"\bsmoke\b", low)),
    }
    if job["smoke"]:
        job["limit"] = 1  # smoke test: chỉ chạy 1 ảnh đầu
    return job


def _parse_age(t: str, low: str) -> Optional[Dict]:
    template = _field_any(t, "templateFolder", "template")
    output = _field_any(t, "outputFolder", "output")
    formula = _field_any(t, "outputFormula", "formula")
    from_year = _int_field_any(t, "fromYear", "from_year")
    to_year = _int_field_any(t, "toYear", "to_year")
    if not template or not output or from_year is None or to_year is None:
        return None
    if to_year < from_year:
        return None

    months = _months_field(t)
    job = {
        "pipeline": "age",
        "templateFolder": _normalize_nas(template),
        "outputFolder": _normalize_nas(output),
        "outputFormula": formula or "[mm]-[year]",
        "fromYear": from_year,
        "toYear": to_year,
        "months": months,
        "install_fonts": bool(re.search(r"\bfonts?\b", low)),
        "smoke": bool(re.search(r"\bsmoke\b", low)),
    }
    if job["smoke"]:
        job["toYear"] = job["fromYear"]
        job["months"] = [job["months"][0]] if job["months"] else [1]
    return job


def _parse_mockup(t: str, low: str) -> Optional[Dict]:
    template = _field_any(t, "templateFolder", "template")
    design = _field_any(t, "designFolder", "design")
    output = _field_any(t, "outputFolder", "output")
    if not template or not design or not output:
        return None

    limit = _limit(t)
    job = {
        "pipeline": "mockup",
        "templateFolder": _normalize_nas(template),
        "designFolder": _normalize_nas(design),
        "outputFolder": _normalize_nas(output),
        "limit": limit,
        "install_fonts": bool(re.search(r"\bfonts?\b", low)),
        "smoke": bool(re.search(r"\bsmoke\b", low)),
    }
    if job["smoke"]:
        job["limit"] = 1
    return job


# --- NAS helpers -------------------------------------------------------------

def _load_nas_env(script_dir: Path) -> Dict[str, str]:
    """Đọc <pipeline>/.env (WEBDAV_USERNAME/PASSWORD + NAS_URL_1..N)."""
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


class _ProcResult:
    """Kết quả chạy lệnh (thay CompletedProcess) — có thêm cờ timed_out."""

    __slots__ = ("returncode", "stdout", "stderr", "timed_out")

    def __init__(
        self,
        returncode: int,
        stdout: str = "",
        stderr: str = "",
        timed_out: bool = False,
    ) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.timed_out = timed_out


def _run_capture(cmd: List[str], cwd: Path, timeout: int) -> _ProcResult:
    """Chạy lệnh, bắt output, timeout AN TOÀN (tránh kẹt worker vĩnh viễn).

    stdout/stderr ghi vào FILE tạm thay vì pipe: `start` trong run-*.bat cho
    Photoshop thừa kế pipe stdout/stderr và GIỮ pipe mở khiến communicate()
    treo tới khi Photoshop thoát (đúng lỗi: job chạy xong nhưng worker không
    chuyển sang job kế). File thì không bao giờ chặn — khi process con thoát,
    wait() trả về ngay dù con cháu còn giữ handle.
    Khi quá timeout: giết CẢ CÂY (taskkill /T /F) rồi trả kết quả lỗi thay vì treo.
    """
    creationflags = 0
    if IS_WINDOWS:
        # Không bật cửa sổ console chớp nhoáng khi chạy nas-mount/wrapper
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    out_f = tempfile.TemporaryFile(mode="w+", encoding="utf-8", errors="replace")
    err_f = tempfile.TemporaryFile(mode="w+", encoding="utf-8", errors="replace")
    try:
        proc = subprocess.Popen(
            cmd, cwd=str(cwd), stdout=out_f, stderr=err_f,
            text=True, errors="replace", creationflags=creationflags,
        )
    except Exception:
        out_f.close()
        err_f.close()
        raise
    timed_out = False
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        if IS_WINDOWS:
            try:
                subprocess.run(
                    ["taskkill", "/T", "/F", "/PID", str(proc.pid)],
                    capture_output=True, timeout=30,
                )
            except Exception:
                pass
        try:
            proc.kill()
        except OSError:
            pass
        try:
            proc.wait(timeout=15)
        except Exception:
            pass
    out_f.seek(0)
    err_f.seek(0)
    out = out_f.read() or ""
    err = err_f.read() or ""
    out_f.close()
    err_f.close()
    return _ProcResult(
        proc.returncode if proc.returncode is not None else 1,
        out,
        err,
        timed_out,
    )


def _mount_point(script_dir: Path) -> str:
    """Trả gốc đường dẫn NAS — macOS: mount point (/Volumes/...), Windows: UNC/ổ đĩa."""
    if IS_WINDOWS:
        name = "nas-mount.bat"
        cmd = ["cmd", "/c", name]
    else:
        name = "nas-mount.sh"
        cmd = ["./" + name]
    proc = _run_capture(cmd, script_dir, 240)
    if proc.timed_out:
        raise FastPathError("Không kết nối được NAS (quá 240s — NAS chậm/ngắt kết nối?).")
    if proc.returncode != 0:
        raise FastPathError("Không kết nối được NAS: " + (proc.stderr.strip()[-400:] or "lỗi không rõ"))
    lines = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
    if not lines:
        raise FastPathError("%s không trả đường dẫn gốc." % name)
    return lines[-1]


def _probe_webdav(url: str, user: str, pwd: str, timeout: int = 4) -> bool:
    proc = subprocess.run(
        [
            _curl_bin(), "-sS", "--connect-timeout", str(timeout), "--max-time", str(timeout * 3),
            "-u", "%s:%s" % (user, pwd), "-X", "PROPFIND", "-H", "Depth: 0",
            url, "-o", os.devnull, "-w", "%{http_code}",
        ],
        capture_output=True, text=True, timeout=timeout * 3 + 5,
    )
    return proc.stdout.strip() == "207"


def _pick_webdav_url(env: Dict[str, str]) -> Optional[str]:
    """Chọn tuyến WebDAV đầu tiên PROPFIND được (cùng thứ tự nas-mount.sh)."""
    user = env.get("WEBDAV_USERNAME", "")
    pwd = env.get("WEBDAV_PASSWORD", "")
    if not user:
        raise FastPathError("Thiếu WEBDAV_USERNAME trong <pipeline>/.env.")
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


def _copy_design(src: Path, dst: Path) -> List[str]:
    """Copy ảnh design (chỉ jpg/png) từ NAS về local."""
    if not src.is_dir():
        raise FastPathError("Design folder trên NAS không tồn tại: %s" % src)
    copied: List[str] = []
    for f in sorted(src.iterdir()):
        if f.suffix.lower() not in DESIGN_EXTS or not _is_nonempty_file(f):
            continue
        shutil.copy2(str(f), str(dst / f.name))
        copied.append(f.name)
    if not copied:
        raise FastPathError("Không thấy file design nào trong: %s" % src)
    return copied


def _is_nonempty_file(path: Path) -> bool:
    """True nếu path là file và có ít nhất 1 byte; lỗi stat thì coi như không hợp lệ."""
    try:
        return path.is_file() and path.stat().st_size > 0
    except OSError:
        return False


def _ensure_webdav_dir(base_url: str, rel: str, user: str, pwd: str) -> None:
    """Tạo từng cấp thư mục trên NAS (MKCOL) — bỏ qua lỗi 'đã tồn tại'."""
    cur = base_url.rstrip("/")
    for part in rel.strip("/").split("/"):
        cur += "/" + part
        subprocess.run(
            [_curl_bin(), "-sS", "-g", "-X", "MKCOL", "-u", "%s:%s" % (user, pwd), cur],
            capture_output=True, text=True, timeout=60,
        )


def _output_exts(pipeline: str) -> Tuple[str, ...]:
    """Đuôi file kết quả theo pipeline: mockup xuất .jpg, tri/age xuất .png."""
    return (".jpg", ".jpeg") if pipeline == "mockup" else (".png",)


def _upload_outputs(
    local_dir: Path,
    base_url: str,
    rel: str,
    user: str,
    pwd: str,
    pipeline: str,
    only_names: Optional[List[str]] = None,
) -> int:
    """Upload từng file kết quả bằng WebDAV PUT (curl -T). Trả số file đã upload.

    only_names dùng cho mockup batch: chỉ upload ảnh của batch vừa chạy, tránh
    upload lại toàn bộ output local sau mỗi lần restart Photoshop.
    """
    exts = _output_exts(pipeline)
    allowed = set(only_names) if only_names is not None else None
    count = 0
    for f in sorted(local_dir.iterdir()):
        if f.suffix.lower() not in exts:
            continue
        if allowed is not None and f.name not in allowed:
            continue
        # -g/--globoff: tắt glob URL — tên file có ký tự [ ] (vd "[[name]2]..." trong
        # tri) nếu không sẽ bị curl hiểu nhầm thành range và báo "bad range".
        url = "%s/%s/%s" % (base_url.rstrip("/"), rel.strip("/"), f.name)
        proc = subprocess.run(
            [_curl_bin(), "-sS", "-g", "--fail", "-T", str(f), "-u", "%s:%s" % (user, pwd), url],
            capture_output=True, text=True, timeout=120,
        )
        if proc.returncode != 0:
            raise FastPathError(
                "Upload thất bại %s: %s" % (f.name, (proc.stderr.strip() or proc.stdout.strip())[-200:])
            )
        count += 1
    return count


def _webdav_file_sizes(base_url: str, rel: str, user: str, pwd: str) -> Dict[str, int]:
    """Đọc tên và kích thước file trực tiếp trong thư mục WebDAV."""
    url = "%s/%s/" % (base_url.rstrip("/"), rel.strip("/"))
    proc = subprocess.run(
        [
            _curl_bin(), "-sS", "-g", "--fail", "-X", "PROPFIND",
            "-H", "Depth: 1", "-u", "%s:%s" % (user, pwd), url,
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if proc.returncode != 0:
        raise FastPathError(
            "Không đọc được danh sách output trên NAS: "
            + (proc.stderr.strip() or proc.stdout.strip())[-300:]
        )
    try:
        root = ET.fromstring(proc.stdout)
    except ET.ParseError as exc:
        raise FastPathError("NAS trả về dữ liệu PROPFIND không hợp lệ.") from exc

    def local_name(tag: str) -> str:
        return tag.rsplit("}", 1)[-1]

    result: Dict[str, int] = {}
    for response in root.iter():
        if local_name(response.tag) != "response":
            continue
        href = ""
        is_collection = False
        size: Optional[int] = None
        for node in response.iter():
            name = local_name(node.tag)
            if name == "href" and not href:
                href = unquote((node.text or "").strip())
            elif name == "collection":
                is_collection = True
            elif name == "getcontentlength":
                try:
                    size = int((node.text or "0").strip())
                except ValueError:
                    size = None
        if not href or is_collection or href.endswith("/") or size is None:
            continue
        result[href.rstrip("/").rsplit("/", 1)[-1]] = size
    return result


def _verify_uploaded_outputs(
    local_dir: Path,
    base_url: str,
    rel: str,
    user: str,
    pwd: str,
    expected_names: List[str],
) -> int:
    """Bắt buộc xác nhận đủ file và đúng kích thước trên NAS sau PUT."""
    if not expected_names:
        raise FastPathError("Không có file local để xác nhận upload trên NAS.")
    remote = _webdav_file_sizes(base_url, rel, user, pwd)
    missing = [name for name in expected_names if name not in remote]
    mismatched = []
    for name in expected_names:
        if name in missing:
            continue
        local_file = local_dir / name
        try:
            local_size = local_file.stat().st_size
        except OSError as exc:
            raise FastPathError("Không đọc được file local sau upload: %s" % name) from exc
        if remote[name] != local_size:
            mismatched.append("%s (%d/%d bytes)" % (name, local_size, remote[name]))
    if missing or mismatched:
        details = []
        if missing:
            details.append("thiếu %d file: %s" % (len(missing), ", ".join(missing[:3])))
        if mismatched:
            details.append("sai kích thước: %s" % ", ".join(mismatched[:3]))
        raise FastPathError("NAS chưa đủ output (%s)." % "; ".join(details))
    return len(expected_names)


# --- helpers dùng chung ------------------------------------------------------

def _resolve_path(value: str, script_dir: Path, mount_point: Optional[str]) -> str:
    if value.startswith("[NAS]"):
        if not mount_point:
            raise FastPathError("Đường dẫn dùng [NAS] nhưng chưa có gốc NAS.")
        rel = value[len("[NAS]"):].lstrip("/")
        if IS_WINDOWS:
            return mount_point.rstrip("\\/") + "\\" + rel.replace("/", "\\")
        return str(Path(mount_point) / rel)
    p = Path(value).expanduser()
    if not p.is_absolute():
        p = (script_dir / p).resolve()
    return str(p)


def _font_dir() -> Path:
    """Thư mục font người dùng — macOS: ~/Library/Fonts, Windows: %LOCALAPPDATA%\\...\\Fonts."""
    if IS_WINDOWS:
        base = os.environ.get("LOCALAPPDATA")
        if base:
            return Path(base) / "Microsoft" / "Windows" / "Fonts"
        return Path.home() / "AppData" / "Local" / "Microsoft" / "Windows" / "Fonts"
    return Path.home() / "Library" / "Fonts"


def scan_rules(folder: Path) -> List[Dict]:
    """Tạo rule tri từ tên PSD: ``5.psd`` = 5, ``5-6.psd`` = 5..6."""
    if not folder.is_dir():
        raise FastPathError("Template folder không tồn tại: %s" % folder)
    rules: List[Dict] = []
    for f in sorted(folder.iterdir()):
        if f.suffix.lower() != ".psd":
            continue
        stem = f.stem
        range_match = re.search(r"(\d+)\s*-\s*(\d+)\s*$", stem)
        if range_match:
            min_value = int(range_match.group(1))
            max_value = int(range_match.group(2))
        else:
            single_match = re.search(r"(\d+)\s*$", stem)
            if not single_match:
                continue
            min_value = max_value = int(single_match.group(1))
        if max_value < min_value:
            raise FastPathError(
                "Tên template không hợp lệ (khoảng ngược): %s" % f.name
            )
        rules.append({"min": min_value, "max": max_value, "template": f.name})
    rules.sort(key=lambda r: (r["min"], r["max"], r["template"]))
    for previous, current in zip(rules, rules[1:]):
        if current["min"] <= previous["max"]:
            raise FastPathError(
                "Các template tri bị chồng khoảng: %s (%d-%d) và %s (%d-%d)."
                % (
                    previous["template"], previous["min"], previous["max"],
                    current["template"], current["min"], current["max"],
                )
            )
    if not rules:
        raise FastPathError(
            "Không tìm thấy file .psd đánh số hoặc có khoảng (vd 2.psd, 5-6.psd) trong template — để agent xử lý."
        )
    return rules


def _build_months_map(template_dir: Path, requested_months: Optional[List[int]]) -> Dict[str, str]:
    """Ánh xạ tháng → file .psd (số cuối tên file = tháng), giống age-script.jsx."""
    if not template_dir.is_dir():
        raise FastPathError("Template folder không tồn tại: %s" % template_dir)
    month_map: Dict[str, str] = {}
    for f in sorted(template_dir.iterdir()):
        if f.suffix.lower() != ".psd":
            continue
        m = re.search(r"(\d+)\s*$", f.stem)
        if not m:
            continue
        month = int(m.group(1))
        if 1 <= month <= 12 and str(month) not in month_map:
            month_map[str(month)] = f.name
    if not month_map:
        raise FastPathError(
            "Không tìm thấy file .psd đánh số tháng (vd 1.psd..12.psd) trong template — để agent xử lý."
        )
    if requested_months is None:
        return month_map
    result = {}
    for month in requested_months:
        key = str(month)
        if key in month_map:
            result[key] = month_map[key]
    if not result:
        raise FastPathError("Không thấy template cho tháng đã chọn: %s" % requested_months)
    return result


def install_fonts(folder: Path) -> List[str]:
    """Copy font (.ttf/.otf/.ttc/.dfont) trong template → thư mục font người dùng."""
    font_dir = _font_dir()
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


def _design_names(folder: Path) -> List[str]:
    """Lấy danh sách design theo đúng thứ tự batch của mockup."""
    try:
        return sorted(
            f.name for f in folder.iterdir()
            if f.suffix.lower() in DESIGN_EXTS and _is_nonempty_file(f)
        )
    except OSError as exc:
        raise FastPathError("Không đọc được thư mục design: %s" % folder) from exc


def _count_output(folder: Path, pipeline: str, timeout: float = 20.0) -> int:
    exts = _output_exts(pipeline)

    def _count() -> int:
        try:
            return sum(1 for f in folder.iterdir() if f.suffix.lower() in exts)
        except OSError:
            return 0

    # iterdir trên SMB có thể treo vô hạn khi NAS chậm/ngắt → đếm trong thread
    # giới hạn thời gian, không chặn worker fast-path.
    result: Dict[str, int] = {}

    def _run() -> None:
        result["n"] = _count()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        return 0
    return result.get("n", 0)


def _build_config(
    job: Dict,
    resolved: Dict[str, str],
    script_dir: Path,
    mockup_start: int = 0,
    mockup_limit: Optional[int] = None,
) -> Dict:
    """Sinh config JSON cho từng pipeline từ đường dẫn đã resolve."""
    pipeline = job["pipeline"]
    if pipeline == "tri":
        return {
            "source": job["source"],
            "sourceLabel": job["sourceLabel"],
            "templateFolder": resolved["templateFolder"],
            "outputFolder": resolved["outputFolder"],
            "outputFormula": job["outputFormula"],
            "limit": job["limit"],
            "rules": scan_rules(Path(resolved["templateFolder"])),
        }
    if pipeline == "age":
        return {
            "fromYear": job["fromYear"],
            "toYear": job["toYear"],
            "templateFolder": resolved["templateFolder"],
            "outputFolder": resolved["outputFolder"],
            "outputFormula": job["outputFormula"],
            "months": _build_months_map(Path(resolved["templateFolder"]), job.get("months")),
        }
    # mockup
    return {
        "templateFolder": resolved["templateFolder"],
        "designFolder": resolved["designFolder"],
        "outputFolder": resolved["outputFolder"],
        "start": mockup_start,
        "limit": job.get("limit", 0) if mockup_limit is None else mockup_limit,
    }


def _config_summary(job: Dict, cfg: Dict) -> str:
    pipeline = job["pipeline"]
    if pipeline == "tri":
        return "✅ Config: %s | %d rule template | limit=%s | source=%s" % (
            cfg["sourceLabel"], len(cfg["rules"]), cfg["limit"], cfg["source"],
        )
    if pipeline == "age":
        return "✅ Config: %d→%d | %d tháng | công thức: %s" % (
            cfg["fromYear"], cfg["toYear"], len(cfg["months"]), cfg["outputFormula"],
        )
    return "✅ Config: limit=%s | template=%s | design=%s" % (
        cfg["limit"], cfg["templateFolder"], cfg["designFolder"],
    )


def _run_wrapper(script_dir: Path, pipeline: str, config_path: Path, timeout: int) -> _ProcResult:
    """Chạy wrapper đúng theo OS: run-<pipeline>.bat (Windows) hoặc run-<pipeline>.sh (macOS)."""
    if IS_WINDOWS:
        cmd = ["cmd", "/c", "run-%s.bat" % pipeline, str(config_path)]
    else:
        cmd = ["./run-%s.sh" % pipeline, str(config_path)]
    return _run_capture(cmd, script_dir, timeout)


def _close_photoshop() -> None:
    """Tắt Photoshop sau mỗi lần chạy (không mở lại — wrapper run-*.bat/.sh
    sẽ tự mở Photoshop khi chạy job kế tiếp)."""
    if IS_WINDOWS:
        try:
            subprocess.run(
                ["taskkill", "/IM", "Photoshop.exe", "/F"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60,
            )
        except subprocess.TimeoutExpired:
            pass
        return

    # macOS: tìm bundle .app (giống wrapper run-*.sh, tránh lấy nhầm thư mục cài đặt)
    candidates = sorted(
        glob.glob("/Applications/Adobe Photoshop*.app")
        + glob.glob("/Applications/*/Adobe Photoshop*.app")
    )
    if not candidates:
        return
    ps_app = Path(candidates[-1]).stem  # vd "Adobe Photoshop 2025"
    try:
        subprocess.run(
            ["osascript", "-e", 'tell application "%s" to quit' % ps_app],
            capture_output=True, text=True, timeout=60,
        )
    except subprocess.TimeoutExpired:
        pass

    # Quit của macOS là bất đồng bộ. Chờ process biến mất để batch kế tiếp không
    # gửi do javascript vào một Photoshop đang ở trạng thái đóng dở.
    pattern = "/Applications/.*/%s.app/Contents/MacOS/%s$" % (
        re.escape(ps_app), re.escape(ps_app),
    )
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            running = subprocess.run(
                ["pgrep", "-f", pattern],
                capture_output=True, text=True, timeout=5,
            ).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            running = ""
        if not running:
            return
        time.sleep(0.5)


def run_job(job: Dict, log: Optional[Callable[[str], None]] = None) -> str:
    """Chạy pipeline với cùng workflow local-NAS trên macOS và Windows."""
    return _run_local_workflow(job, log)


def _run_local_workflow(job: Dict, log: Optional[Callable[[str], None]] = None) -> str:
    """Chạy local khi dùng NAS trên cả macOS và Windows rồi upload kết quả."""
    def emit(msg: str) -> None:
        if log:
            log(msg)

    pipeline = job["pipeline"]
    script_dir = ROOT / PIPELINE_DIRS[pipeline]
    done_file = script_dir / DONE_FILES[pipeline]
    config_path = script_dir / CONFIG_FILES[pipeline]
    local_root = _local_run_dir(pipeline)
    folders = PIPELINE_FOLDERS[pipeline]

    needs_nas = any(job.get(f, "").startswith("[NAS]") for f in folders)
    emit("📋 Fast-path: phân tích lệnh (không qua AI)...")

    upload_target: Optional[Tuple[str, str, str, str]] = None
    report_output = ""

    try:
        resolved: Dict[str, str] = {}
        if needs_nas:
            emit("🗂 NAS: mount + chuẩn bị chạy local (tải về, upload kết quả lên)...")
            mount_point = _mount_point(script_dir)
            nas_env = _load_nas_env(script_dir)
            user = nas_env.get("WEBDAV_USERNAME", "")
            pwd = nas_env.get("WEBDAV_PASSWORD", "")

            if local_root.exists():
                shutil.rmtree(local_root, ignore_errors=True)
            local_root.mkdir(parents=True)

            for field, (subdir, kind) in folders.items():
                value = job.get(field)
                if not value:
                    continue
                if value.startswith("[NAS]"):
                    nas_path = _resolve_path(value, script_dir, mount_point)
                    if kind == "output":
                        local_out = local_root / subdir
                        local_out.mkdir(parents=True)
                        resolved[field] = str(local_out)
                        rel = value[len("[NAS]"):].lstrip("/")
                        base_url = _pick_webdav_url(nas_env)
                        if not base_url:
                            raise FastPathError("Không tìm được tuyến WebDAV để upload kết quả.")
                        upload_target = (base_url, user, pwd, rel)
                        report_output = value
                    else:
                        local_sub = local_root / subdir
                        local_sub.mkdir(parents=True)
                        if kind == "template":
                            _copy_nas_template(Path(nas_path), local_sub)
                        elif kind == "design":
                            _copy_design(Path(nas_path), local_sub)
                        resolved[field] = str(local_sub)
                else:
                    resolved[field] = _resolve_path(value, script_dir, None)
                    if kind == "output":
                        report_output = resolved[field]
        else:
            for field, (subdir, kind) in folders.items():
                value = job.get(field)
                if not value:
                    continue
                resolved[field] = _resolve_path(value, script_dir, None)
                if kind == "output":
                    report_output = resolved[field]

        # Tạo thư mục output trước khi chạy nếu chưa tồn tại (local + NAS)
        out_dir = Path(resolved.get("outputFolder", ""))
        if out_dir:
            try:
                out_dir.mkdir(parents=True, exist_ok=True)
            except OSError:
                pass
        if upload_target:
            base_url, user, pwd, rel = upload_target
            _ensure_webdav_dir(base_url, rel, user, pwd)

        if job.get("install_fonts"):
            fonts = install_fonts(Path(resolved.get("templateFolder", "")))
            if fonts:
                emit("🔤 Đã cài font: " + ", ".join(fonts))

        timeout = _env_int("FASTPATH_TIMEOUT_SEC", 21600)
        default_attempts = _env_int("PHOTOSHOP_MAX_ATTEMPTS", PHOTOSHOP_MAX_ATTEMPTS)
        max_attempts = (
            _env_int("MOCKUP_BATCH_MAX_ATTEMPTS", default_attempts)
            if pipeline == "mockup" else default_attempts
        )
        # Mockup output trên NAS chạy theo batch để Photoshop không phải xử lý
        # hàng trăm lần mở/đóng Smart Object trong cùng một phiên.
        # Mockup NAS chạy theo batch; tri/age chạy một job nhưng đều có retry.
        is_batched_mockup = pipeline == "mockup" and upload_target is not None
        if is_batched_mockup:
            design_names = _design_names(Path(resolved["designFolder"]))
            requested_limit = int(job.get("limit", 0) or 0)
            total_to_process = (
                min(requested_limit, len(design_names))
                if requested_limit > 0 else len(design_names)
            )
            if total_to_process == 0:
                raise FastPathError("Không thấy design nào để chạy: %s" % resolved["designFolder"])

            proc = None
            status = ""
            tail = ""
            success = True
            cancelled = False
            for batch_start in range(0, total_to_process, MOCKUP_BATCH_SIZE):
                batch_end = min(batch_start + MOCKUP_BATCH_SIZE, total_to_process)
                batch_names = design_names[batch_start:batch_end]
                output_names = [Path(name).stem + ".jpg" for name in batch_names]
                batch_success = False
                batch_error = ""
                for attempt in range(1, max_attempts + 1):
                    cfg = _build_config(
                        job,
                        resolved,
                        script_dir,
                        mockup_start=batch_start,
                        mockup_limit=len(batch_names),
                    )
                    config_path.write_text(
                        json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8"
                    )
                    if batch_start == 0 and attempt == 1:
                        emit(_config_summary(job, cfg))
                    emit(
                        "🚀 Chạy mockup batch %d-%d/%d (lần thử %d/%d)..."
                        % (
                            batch_start + 1,
                            batch_end,
                            total_to_process,
                            attempt,
                            max_attempts,
                        )
                    )
                    try:
                        proc = _run_wrapper(script_dir, pipeline, config_path, timeout)
                    except Exception as exc:
                        proc = _ProcResult(1, "", "")
                        batch_error = "runner: %s" % str(exc)[-300:]
                        status = "RUNNER_ERROR"
                        tail = "Batch error: " + batch_error
                        if attempt >= max_attempts:
                            break
                        if (ROOT / ".cancel-flag").exists():
                            cancelled = True
                            break
                        emit(
                            "⚠️ Mockup batch %d-%d lỗi (%s) — tự khởi động lại Photoshop "
                            "và chạy lại..."
                            % (batch_start + 1, batch_end, batch_error)
                        )
                        _close_photoshop()
                        continue
                    if proc.timed_out:
                        batch_error = "timeout sau %ds" % timeout
                        status = "TIMEOUT"
                    else:
                        status = ""
                        if done_file.is_file():
                            try:
                                status = done_file.read_text(encoding="utf-8").strip()
                            except OSError:
                                status = "?"
                        tail = (proc.stdout or "").strip()[-1200:]
                        if proc.returncode != 0 or status != "OK":
                            batch_error = "exit %s, done=%s" % (
                                proc.returncode,
                                status or "MISSING",
                            )
                        else:
                            try:
                                emit(
                                    "⬆️ Upload batch %d-%d lên NAS..."
                                    % (batch_start + 1, batch_end)
                                )
                                base_url, user, pwd, rel = upload_target
                                _ensure_webdav_dir(base_url, rel, user, pwd)
                                uploaded = _upload_outputs(
                                    Path(resolved["outputFolder"]),
                                    base_url,
                                    rel,
                                    user,
                                    pwd,
                                    pipeline,
                                    only_names=output_names,
                                )
                                if uploaded != len(output_names):
                                    raise FastPathError(
                                        "PUT được %d/%d ảnh của batch lên NAS."
                                        % (uploaded, len(output_names))
                                    )
                                verified = _verify_uploaded_outputs(
                                    Path(resolved["outputFolder"]),
                                    base_url,
                                    rel,
                                    user,
                                    pwd,
                                    output_names,
                                )
                                emit(
                                    "✅ Đã upload và xác nhận %d ảnh của batch trên NAS."
                                    % verified
                                )
                                batch_success = True
                            except Exception as exc:
                                batch_error = "upload: %s" % str(exc)[-300:]

                    if batch_success:
                        break
                    if attempt >= max_attempts:
                        break
                    if (ROOT / ".cancel-flag").exists():
                        cancelled = True
                        break
                    emit(
                        "⚠️ Mockup batch %d-%d lỗi (%s) — tự khởi động lại Photoshop "
                        "và chạy lại..."
                        % (batch_start + 1, batch_end, batch_error)
                    )
                    _close_photoshop()

                if cancelled:
                    break
                if not batch_success:
                    success = False
                    final_returncode = (
                        proc.returncode
                        if proc is not None and proc.returncode != 0 else 1
                    )
                    if batch_error:
                        tail = (tail + "\nBatch error: " + batch_error).strip()
                    break

                if batch_end < total_to_process:
                    if (ROOT / ".cancel-flag").exists():
                        cancelled = True
                        break
                    emit("♻️ Đóng Photoshop để khởi động lại cho batch kế tiếp...")
                    _close_photoshop()

            if cancelled:
                success = False
                final_returncode = 130
            else:
                final_returncode = proc.returncode if proc is not None else 1
        else:
            cfg = _build_config(job, resolved, script_dir)
            config_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
            emit(_config_summary(job, cfg))
            is_retryable_pipeline = pipeline in ("tri", "age", "mockup")
            attempt_limit = max_attempts if is_retryable_pipeline else 1
            for attempt in range(1, attempt_limit + 1):
                emit(
                    "🚀 Chạy run-%s (lần thử %d/%d)..."
                    % (pipeline, attempt, attempt_limit)
                )
                runner_error = False
                try:
                    proc = _run_wrapper(script_dir, pipeline, config_path, timeout)
                except Exception as exc:
                    proc = _ProcResult(1, "", "")
                    status = "RUNNER_ERROR"
                    tail = "Runner error: " + str(exc)[-300:]
                    success = False
                    runner_error = True
                if proc.timed_out:
                    status = "TIMEOUT"
                    tail = ""
                    success = False
                elif not runner_error:
                    status = ""
                    if done_file.is_file():
                        try:
                            status = done_file.read_text(encoding="utf-8").strip()
                        except OSError:
                            status = "?"
                    tail = (proc.stdout or "").strip()[-1200:]
                    success = proc.returncode == 0 and status == "OK"
                final_returncode = proc.returncode
                if success and upload_target:
                    try:
                        expected_names = [
                            f.name
                            for f in sorted(Path(resolved["outputFolder"]).iterdir())
                            if f.is_file() and f.suffix.lower() in _output_exts(pipeline)
                        ]
                        base_url, user, pwd, rel = upload_target
                        emit("⬆️ Upload kết quả lên NAS...")
                        _ensure_webdav_dir(base_url, rel, user, pwd)
                        uploaded = _upload_outputs(
                            Path(resolved["outputFolder"]),
                            base_url,
                            rel,
                            user,
                            pwd,
                            pipeline,
                        )
                        if uploaded != len(expected_names):
                            raise FastPathError(
                                "PUT được %d/%d ảnh lên NAS."
                                % (uploaded, len(expected_names))
                            )
                        verified = _verify_uploaded_outputs(
                            Path(resolved["outputFolder"]),
                            base_url,
                            rel,
                            user,
                            pwd,
                            expected_names,
                        )
                        emit("✅ Đã upload và xác nhận %d ảnh trên NAS." % verified)
                    except Exception as exc:
                        status = "UPLOAD_ERROR"
                        tail = "Upload error: " + str(exc)[-300:]
                        final_returncode = 1
                        success = False
                if success or not is_retryable_pipeline or attempt >= attempt_limit:
                    break
                if (ROOT / ".cancel-flag").exists():
                    final_returncode = 130
                    break
                emit(
                    "⚠️ %s lỗi (exit %s, done=%s) — tự khởi động lại Photoshop "
                    "và chạy lại..." % (pipeline, final_returncode, status or "MISSING")
                )
                _close_photoshop()

        count = _count_output(Path(resolved.get("outputFolder", "")), pipeline)

        lines = []
        if success:
            lines.append("✅ Xong — %s: %d ảnh, exit OK." % (pipeline, count))
        elif final_returncode == 130:
            lines.append("⛔ Đã huỷ — %s." % pipeline)
        else:
            lines.append("❌ Lỗi — %s (exit %s, done=%s)." % (pipeline, final_returncode, status or "MISSING"))
        lines.append("Output: %s" % report_output)
        if not success and final_returncode not in (0, 130) and tail:
            lines.append("Log (cuối):\n" + tail)

        # Tắt Photoshop sau mỗi lần chạy (trừ khi bị huỷ) — job kế tự mở lại khi cần
        if final_returncode != 130 and not (ROOT / ".cancel-flag").exists():
            _close_photoshop()

        return "\n".join(lines)
    finally:
        if needs_nas:
            shutil.rmtree(local_root, ignore_errors=True)


def main(argv: List[str]) -> int:
    text = " ".join(argv[1:]).strip()
    job = parse_job(text)
    if job is None:
        print(
            "Không nhận diện được lệnh chạy.\n"
            "Fast-path nhận lệnh 'chạy tri/age/mockup ...' với đủ config (template/nguồn/output/limit...).",
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
