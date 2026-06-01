"""
Destatis GENESIS API utilities — shared fetch/parse helpers for destatis.py.
"""

import io
import logging
import re
import sqlite3
import time
import zipfile

import pandas as pd
import requests

from policy_monitor.config import DESTATIS_TOKEN as TOKEN, DESTATIS_BASE_URL as API_URL

log = logging.getLogger("destatis")

_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "username":     TOKEN,
    "password":     "",
}

_RETRY_ON = {503, 502, 504, 429}

_MONTH_TO_MONAT = {
    "january": "MONAT01", "february": "MONAT02", "march":    "MONAT03",
    "april":   "MONAT04", "may":      "MONAT05", "june":     "MONAT06",
    "july":    "MONAT07", "august":   "MONAT08", "september":"MONAT09",
    "october": "MONAT10", "november": "MONAT11", "december": "MONAT12",
}


class DesatisNoDataError(RuntimeError):
    """Raised when Destatis returns status code=-1 (no data cube found)."""


def _lower_keys(obj):
    if isinstance(obj, dict):
        return {k.lower(): _lower_keys(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_lower_keys(i) for i in obj]
    return obj


def _is_rate_limited(response) -> bool:
    if response.status_code == 404:
        try:
            return response.json().get("Code") in (2, 6)
        except Exception:
            return '"Code":6' in response.text or '"Code":2' in response.text
    return False


def _parse_datencsv(content: str, structure: dict) -> pd.DataFrame:
    col_specs = structure.get("columns") or []
    MONTH_NAMES = set(_MONTH_TO_MONAT)
    lines = content.split("\n")

    year_idx = month_idx = None
    for line in lines:
        fields = [f.strip() for f in line.split(";")]
        for i in range(len(fields) - 1):
            try:
                yr = int(fields[i].split(".")[0])
                if not (1950 <= yr <= 2060):
                    continue
                mname = "".join(c for c in fields[i + 1].lower() if c.isalpha())
                if mname in MONTH_NAMES:
                    year_idx = i
                    month_idx = i + 1
                    break
            except (ValueError, IndexError):
                pass
        if year_idx is not None:
            break

    if year_idx is None:
        return pd.DataFrame()

    n_subtitles = year_idx

    first_data = last_data = None
    for i, line in enumerate(lines):
        fields = [f.strip() for f in line.split(";")]
        if len(fields) <= month_idx:
            continue
        try:
            yr = int(fields[year_idx].split(".")[0])
            if not (1950 <= yr <= 2060):
                continue
            mname = "".join(c for c in fields[month_idx].lower() if c.isalpha())
            if mname in MONTH_NAMES:
                if first_data is None:
                    first_data = i
                last_data = i
        except (ValueError, IndexError):
            pass

    if first_data is None:
        return pd.DataFrame()

    header_fields = []
    for i in range(first_data - 1, -1, -1):
        if ";" in lines[i] and lines[i].strip():
            header_fields = [f.strip() for f in lines[i].split(";")]
            break

    data_lines = [lines[i] for i in range(first_data, last_data + 1) if lines[i].strip()]

    _NULL_MARKERS = {"...", ".", "-", "/", "x", ""}

    def _is_value_field(s):
        if s in _NULL_MARKERS:
            return True
        try:
            float(s.replace(",", "."))
            return True
        except ValueError:
            return False

    n_text_suffix = 0
    for sample in data_lines[:10]:
        sf = [f.strip() for f in sample.split(";")]
        post_month = sf[month_idx + 1:]
        count = 0
        for v in post_month:
            if not _is_value_field(v):
                count += 1
            else:
                break
        n_text_suffix = max(n_text_suffix, count)

    n_val = max(len(l.split(";")) - month_idx - 1 - n_text_suffix for l in data_lines) if data_lines else 0

    val_specs = []
    if len(col_specs) >= n_val:
        for c in col_specs[:n_val]:
            code  = (c.get("code")      or "").upper().strip()
            label = (c.get("content")   or "").strip()
            fn    = (c.get("functions") or "").strip()
            if fn:
                label = f"{label} ({fn})"
            val_specs.append({"vcode": code, "vlabel": label, "attr3code": None, "attr3label": None})
    else:
        hval_start = month_idx + 1 + n_text_suffix
        hval = header_fields[hval_start:] if len(header_fields) > hval_start else []
        base_vcode  = (col_specs[0].get("code")    or "COL").upper() if col_specs else "COL"
        base_vlabel = (col_specs[0].get("content") or "value").strip() if col_specs else "value"
        for j in range(n_val):
            lbl  = hval[j].strip() if j < len(hval) else f"type_{j+1}"
            code = re.sub(r"[^A-Z0-9_]", "_", lbl.upper())[:50].strip("_")
            val_specs.append({"vcode": base_vcode, "vlabel": base_vlabel,
                               "attr3code": code, "attr3label": lbl})

    _complex_col_attr_num = max(3, n_subtitles + n_text_suffix + 2)

    result = []
    for line in data_lines:
        fields = [f.strip() for f in line.split(";")]
        subtitle_vals = [
            re.sub(r"\.\d+$", "", fields[i]).strip() if i < len(fields) else None
            for i in range(n_subtitles)
        ]
        try:
            year = int(fields[year_idx].split(".")[0])
        except (ValueError, IndexError):
            continue
        raw_month = fields[month_idx].lower() if month_idx < len(fields) else ""
        month_name = "".join(c for c in raw_month if c.isalpha())
        monat = _MONTH_TO_MONAT.get(month_name)
        if not monat:
            continue
        text_suffix_vals = [
            fields[month_idx + 1 + k] if (month_idx + 1 + k) < len(fields) else ""
            for k in range(n_text_suffix)
        ]
        vals = fields[month_idx + 1 + n_text_suffix:]
        for i, spec in enumerate(val_specs):
            if i >= len(vals):
                break
            raw = vals[i]
            cleaned = raw if raw not in _NULL_MARKERS else None
            row = {
                "time": year,
                "1_variable_attribute_code": monat,
                "value_variable_code":       spec["vcode"],
                "value_variable_label":      spec["vlabel"],
                "value":                     cleaned,
            }
            for j, sub_val in enumerate(subtitle_vals):
                attr_num = j + 2
                row[f"{attr_num}_variable_attribute_code"]  = sub_val
                row[f"{attr_num}_variable_attribute_label"] = sub_val
            for j, tsv in enumerate(text_suffix_vals):
                attr_num = n_subtitles + j + 2
                row[f"{attr_num}_variable_attribute_code"]  = tsv
                row[f"{attr_num}_variable_attribute_label"] = tsv
            if spec["attr3code"] is not None:
                row[f"{_complex_col_attr_num}_variable_attribute_code"]  = spec["attr3code"]
                row[f"{_complex_col_attr_num}_variable_attribute_label"] = spec["attr3label"]
            result.append(row)

    return pd.DataFrame(result) if result else pd.DataFrame()


def fetch_table(table_id: str, startyear: str = "2019", endyear: str | None = None,
                contents: str = "", retries: int = 3, backoff: int = 30) -> pd.DataFrame:
    body = {
        "name":      table_id,
        "area":      "all",
        "startyear": startyear,
        "contents":  contents,
        "format":    "ffcsv",
        "language":  "en",
    }
    if endyear is not None:
        body["endyear"] = endyear

    last_error = None
    for attempt in range(retries):
        try:
            response = requests.post(API_URL, headers=_HEADERS, data=body, timeout=120, verify=False)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_error = e
            wait = backoff * (2 ** attempt)
            log.warning(f"    [connection error, retrying in {wait}s ({attempt+2}/{retries})...]")
            time.sleep(wait)
            continue

        if response.status_code in _RETRY_ON or _is_rate_limited(response):
            last_error = RuntimeError(f"HTTP {response.status_code}")
            if attempt < retries - 1:
                wait = backoff * (2 ** attempt)
                log.warning(f"    [HTTP {response.status_code}, retrying in {wait}s ({attempt+2}/{retries})...]")
                time.sleep(wait)
                continue
            raise RuntimeError(f"HTTP {response.status_code}: server unavailable after {retries} attempts")

        if not response.ok:
            raise RuntimeError(f"HTTP {response.status_code}: {response.text[:300]}")

        text = response.text.strip()
        if text.startswith("{"):
            try:
                jdata = response.json()
            except ValueError:
                pass
            else:
                jdata  = _lower_keys(jdata)
                status = jdata.get("status") or {}
                if status.get("code") == -1:
                    msg = status.get("content", "no details")
                    raise DesatisNoDataError(
                        f"Destatis: no data for {table_id} from {startyear}. {msg[:200]}"
                    )
                obj       = jdata.get("object") or {}
                content   = obj.get("content", "")
                structure = obj.get("structure") or {}
                df = _parse_datencsv(content, structure)
                df.columns = [c.lower().strip() for c in df.columns]
                return df

        if response.content.startswith(b"PK"):
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                with zf.open(zf.namelist()[0]) as csv_file:
                    df = pd.read_csv(csv_file, delimiter=";", decimal=",",
                                     na_values=["...", ".", "-", "/", "x"])
        else:
            df = pd.read_csv(io.StringIO(response.text), delimiter=";", decimal=",",
                             na_values=["...", ".", "-", "/", "x"])
        df.columns = [c.lower().strip() for c in df.columns]
        return df

    raise DesatisNoDataError(
        f"Destatis: {table_id} unreachable after {retries} attempts. Last error: {last_error}"
    )


def fetch_raw_datencsv(table_id: str, startyear: str = "2019", endyear: str | None = None,
                       contents: str = "", fmt: str = "datencsv",
                       retries: int = 3, backoff: int = 30) -> str:
    body = {
        "name":      table_id,
        "area":      "all",
        "startyear": startyear,
        "contents":  contents,
        "format":    fmt,
        "language":  "en",
    }
    if endyear is not None:
        body["endyear"] = endyear

    last_error = None
    for attempt in range(retries):
        try:
            response = requests.post(API_URL, headers=_HEADERS, data=body, timeout=120, verify=False)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_error = e
            wait = backoff * (2 ** attempt)
            log.warning(f"    [connection error, retrying in {wait}s ({attempt+2}/{retries})...]")
            time.sleep(wait)
            continue

        if response.status_code in _RETRY_ON or _is_rate_limited(response):
            last_error = RuntimeError(f"HTTP {response.status_code}")
            if attempt < retries - 1:
                wait = backoff * (2 ** attempt)
                log.warning(f"    [HTTP {response.status_code}, retrying in {wait}s ({attempt+2}/{retries})...]")
                time.sleep(wait)
                continue
            raise RuntimeError(f"HTTP {response.status_code}: server unavailable after {retries} attempts")

        if not response.ok:
            raise RuntimeError(f"HTTP {response.status_code}: {response.text[:300]}")

        text = response.text.strip()
        if text.startswith("{"):
            try:
                jdata = response.json()
            except ValueError:
                pass
            else:
                jdata  = _lower_keys(jdata)
                status = jdata.get("status") or {}
                if status.get("code") == -1:
                    msg = status.get("content", "no details")
                    raise DesatisNoDataError(
                        f"Destatis: no data for {table_id} from {startyear}. {msg[:200]}"
                    )
                obj = jdata.get("object") or {}
                return obj.get("content", "")
        raise RuntimeError(f"Unexpected response format for {table_id}: {text[:100]}")
    raise last_error  # type: ignore[misc]


def _find_time_var_col(df: pd.DataFrame) -> str:
    if "1_variable_attribute_code" in df.columns:
        return "1_variable_attribute_code"
    for col in df.columns:
        sample = df[col].dropna().astype(str)
        if sample.str.match(r"^(MONAT|QUARTAL)\d+$").any():
            return col
    raise KeyError(f"Cannot find MONAT/QUARTAL column. Available: {list(df.columns)}")


def build_monthly_period(df: pd.DataFrame) -> pd.Series:
    col = _find_time_var_col(df)
    month_num = df[col].str.replace("MONAT", "").astype(int)
    return pd.to_datetime(df["time"].astype(str) + "-" + month_num.astype(str) + "-01")


def build_quarterly_period(df: pd.DataFrame) -> pd.Series:
    col = _find_time_var_col(df)
    q_to_month = {"QUARTAL1": 1, "QUARTAL2": 4, "QUARTAL3": 7, "QUARTAL4": 10}
    month_num = df[col].map(q_to_month)
    return pd.to_datetime(df["time"].astype(str) + "-" + month_num.astype(str) + "-01")


_META_URL = "https://www-genesis.destatis.de/genesisWS/rest/2020/metadata/table"


def fetch_table_meta(table_id: str, language: str = "en") -> dict:
    body = {"name": table_id, "area": "all", "language": language}
    try:
        resp = requests.post(_META_URL, headers=_HEADERS, data=body, timeout=30, verify=False)
        if not resp.ok:
            log.warning(f"    [metadata HTTP {resp.status_code} for {table_id}]")
            return {"table_id": table_id, "title": "", "period_from": "", "period_to": ""}
        jdata  = _lower_keys(resp.json())
        status = jdata.get("status") or {}
        if status.get("code", 0) != 0:
            log.warning(f"    [metadata API error {status.get('code')} for {table_id}]")
            return {"table_id": table_id, "title": "", "period_from": "", "period_to": ""}
        obj        = jdata.get("object") or {}
        time_range = obj.get("time") or {}
        return {
            "table_id":    table_id,
            "title":       (obj.get("content") or "").strip(),
            "period_from": str(time_range.get("from", "")),
            "period_to":   str(time_range.get("to", "")),
        }
    except Exception as exc:
        log.warning(f"    [metadata exception for {table_id}: {exc}]")
        return {"table_id": table_id, "title": "", "period_from": "", "period_to": ""}


def get_start_year(conn: sqlite3.Connection, table: str, date_col: str = "period",
                   lookback: int = 1, default: int = 2019) -> int:
    """Return the year to start fetching (latest stored year minus lookback)."""
    try:
        row = conn.execute(f'SELECT MAX("{date_col}") FROM "{table}"').fetchone()
        if row and row[0]:
            year = int(str(row[0])[:4])
            return max(default, year - lookback)
        return default
    except Exception:
        return default
