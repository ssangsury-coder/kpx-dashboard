# -*- coding: utf-8 -*-
"""
대한민국 전력계통 현황 대시보드 - 데이터 수집기
================================================
전력거래소(KPX) 공공데이터 Open API 4종을 주기적으로 호출해
SQLite에 저장하고, 대시보드가 읽을 data/dashboard.json 을 만든 뒤
내 PC(127.0.0.1)에서만 열리는 작은 웹서버로 화면을 보여줍니다.

사용하는 API (공공데이터포털에서 각각 '활용신청' 필요)
  1) 한국전력거래소_오늘전력수급현황조회_GW      Sukub5mToday/getSukub5mToday
  2) 한국전력거래소_현재전력수급현황조회_GW      sukub5mMaxDatetime2/getSukub5mMaxDatetime2
  3) 한국전력거래소_발전원별 발전량(계통기준)     PwrAmountByGen/getPwrAmountByGen
  4) 한국전력거래소_계통한계가격 및 수요예측       SmpWithForecastDemand/getSmpWithForecastDemand

실행
  python collector.py            수집 + 대시보드 서버 실행 (기본)
  python collector.py --test     인증키/API 연결만 점검하고 종료
  python collector.py --once     한 번만 수집하고 종료 (작업 스케줄러용)

외부 패키지 없이 파이썬 3.8+ 표준 라이브러리만 사용합니다.
"""
import argparse
import configparser
import json
import logging
import os
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

KST = timezone(timedelta(hours=9))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "kpx.sqlite")
JSON_PATH = os.path.join(DATA_DIR, "dashboard.json")
CONFIG_PATH = os.path.join(BASE_DIR, "config.ini")

API = {
    "today": "/B552115/Sukub5mToday/getSukub5mToday",
    "current": "/B552115/sukub5mMaxDatetime2/getSukub5mMaxDatetime2",
    "gen": "/B552115/PwrAmountByGen/getPwrAmountByGen",
    "smp": "/B552115/SmpWithForecastDemand/getSmpWithForecastDemand",
}
FUEL = [  # (API 필드, 저장 컬럼, 한글명)
    ("fuelPwr1", "hydro", "수력"), ("fuelPwr2", "oil", "유류"), ("fuelPwr3", "coal", "유연탄"),
    ("fuelPwr4", "nuclear", "원자력"), ("fuelPwr5", "pumped", "양수"), ("fuelPwr6", "gas", "가스(LNG)"),
    ("fuelPwr7", "domcoal", "국내탄"), ("fuelPwr8", "renew", "신재생"), ("fuelPwr9", "solar", "태양광"),
    ("fuelPwrTot", "total", "합계"),
]
OK_CODES = {"00", "0", "0000", "INFO-000", "NORMAL_CODE"}

log = logging.getLogger("kpx")


# ----------------------------------------------------------------- 설정
def load_config():
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.read_dict({
        "kpx": {"service_key": "", "api_base": "https://apis.data.go.kr"},
        "server": {"port": "8000", "open_browser": "yes"},
        "schedule": {"supply_minutes": "15", "gen_minutes": "15", "smp_hours": "6"},
        "kepco": {"api_key": "", "api_base": "https://bigdata.kepco.co.kr", "months_back": "24"},
    })
    if os.path.exists(CONFIG_PATH):
        cfg.read(CONFIG_PATH, encoding="utf-8")
    key = os.environ.get("KPX_SERVICE_KEY") or cfg.get("kpx", "service_key").strip()
    # 공공데이터포털의 'Encoding 키'를 넣어도 동작하도록 한 번 디코딩
    if "%" in key:
        key = urllib.parse.unquote(key)
    cfg.set("kpx", "service_key", key)
    cfg.set("kepco", "api_key", (os.environ.get("KEPCO_API_KEY") or cfg.get("kepco", "api_key")).strip())
    return cfg


# ----------------------------------------------------------------- API 호출
class ApiError(Exception):
    pass


def _num(v):
    if v is None:
        return None
    try:
        s = str(v).replace(",", "").strip()
        f = float(s) if s not in ("", "-", "null", "None") else None
        return None if f is None or f != f or f in (float("inf"), float("-inf")) else f
    except ValueError:
        return None


def _find_items(obj):
    """JSON 응답 어디에 있든 item 목록을 찾아 리스트로 반환"""
    if isinstance(obj, dict):
        if "item" in obj:
            it = obj["item"]
            return it if isinstance(it, list) else ([it] if it else [])
        for v in obj.values():
            r = _find_items(v)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = _find_items(v)
            if r is not None:
                return r
    return None


def _find_key(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = _find_key(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = _find_key(v, key)
            if r is not None:
                return r
    return None


def _parse_xml(text):
    root = ET.fromstring(text)
    code = root.findtext(".//resultCode") or root.findtext(".//returnReasonCode")
    msg = root.findtext(".//resultMsg") or root.findtext(".//returnAuthMsg") or root.findtext(".//errMsg")
    items = [{c.tag: c.text for c in it} for it in root.iter("item")]
    return code, msg, items


def call_api(cfg, name, params=None, rows=300):
    key = cfg.get("kpx", "service_key")
    if not key:
        raise ApiError("config.ini 의 service_key 가 비어 있습니다.")
    q = {"serviceKey": key, "dataType": "json", "pageNo": 1, "numOfRows": rows}
    q.update(params or {})
    url = cfg.get("kpx", "api_base").rstrip("/") + API[name] + "?" + urllib.parse.urlencode(q)
    req = urllib.request.Request(url, headers={"User-Agent": "kpx-dashboard/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            text = r.read().decode("utf-8", "replace").strip()
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:300]
        raise ApiError("HTTP %s: %s" % (e.code, body))
    except urllib.error.URLError as e:
        raise ApiError("네트워크 오류: %s" % e.reason)

    if text.startswith("<"):
        code, msg, items = _parse_xml(text)
    else:
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            raise ApiError("응답을 해석할 수 없습니다: " + text[:200])
        code = _find_key(data, "resultCode")
        msg = _find_key(data, "resultMsg")
        items = _find_items(data) or []
    if code is not None and str(code) not in OK_CODES:
        raise ApiError("API 오류 코드 %s: %s" % (code, msg))
    return items


# ----------------------------------------------------------------- 저장소
def db():
    os.makedirs(DATA_DIR, exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.executescript("""
    CREATE TABLE IF NOT EXISTS supply(ts TEXT PRIMARY KEY, demand REAL, supply REAL, forecast REAL,
        reserve REAL, reserve_rate REAL, oper_reserve REAL, oper_rate REAL);
    CREATE TABLE IF NOT EXISTS gen(ts TEXT PRIMARY KEY, hydro REAL, oil REAL, coal REAL, nuclear REAL,
        pumped REAL, gas REAL, domcoal REAL, renew REAL, solar REAL, total REAL);
    CREATE TABLE IF NOT EXISTS kepco_cntr(ym TEXT, cntr TEXT, cust REAL, usage REAL, bill REAL, cntr_pwr REAL,
        PRIMARY KEY(ym, cntr));
    CREATE TABLE IF NOT EXISTS kepco_indus(ym TEXT, biz TEXT, cust REAL, usage REAL, bill REAL, PRIMARY KEY(ym, biz));
    CREATE TABLE IF NOT EXISTS kepco_renew(year TEXT, gensrc TEXT, cnt REAL, capacity REAL, PRIMARY KEY(year, gensrc));
    CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
    CREATE TABLE IF NOT EXISTS kepco_done(api TEXT, period TEXT, rows INTEGER, fetched TEXT, PRIMARY KEY(api, period));
    CREATE TABLE IF NOT EXISTS smp(date TEXT, hour INTEGER, area TEXT, smp REAL, mlfd REAL, jlfd REAL, slfd REAL,
        PRIMARY KEY(date, hour, area));
    """)
    return con


def _ts(v):
    """'20261031134500' / '202610311345' / '2026-10-31 13:45' -> '2026-10-31 13:45'"""
    d = "".join(ch for ch in str(v or "") if ch.isdigit())
    if len(d) < 12:
        return None
    return "%s-%s-%s %s:%s" % (d[0:4], d[4:6], d[6:8], d[8:10], d[10:12])


def save_supply(con, items):
    n = 0
    for it in items:
        ts = _ts(it.get("baseDatetime"))
        if not ts:
            continue
        con.execute("INSERT OR REPLACE INTO supply VALUES(?,?,?,?,?,?,?,?)", (
            ts, _num(it.get("currPwrTot")), _num(it.get("suppAbility")), _num(it.get("forecastLoad")),
            _num(it.get("suppReservePwr")), _num(it.get("suppReserveRate")),
            _num(it.get("operReservePwr")), _num(it.get("operReserveRate"))))
        n += 1
    con.commit()
    return n


def save_gen(con, items):
    n = 0
    for it in items:
        ts = _ts(it.get("baseDatetime"))
        if not ts:
            continue
        con.execute("INSERT OR REPLACE INTO gen VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    [ts] + [_num(it.get(f)) for f, _, _ in FUEL])
        n += 1
    con.commit()
    return n


def save_smp(con, items):
    n = 0
    for it in items:
        d = "".join(ch for ch in str(it.get("date") or "") if ch.isdigit())[:8]
        h = _num(it.get("hour"))
        if len(d) != 8 or h is None:
            continue
        area = (it.get("areaName") or "").strip() or "육지"
        con.execute("INSERT OR REPLACE INTO smp VALUES(?,?,?,?,?,?,?)", (
            "%s-%s-%s" % (d[:4], d[4:6], d[6:8]), int(h), area, _num(it.get("smp")),
            _num(it.get("mlfd")), _num(it.get("jlfd")), _num(it.get("slfd"))))
        n += 1
    con.commit()
    return n


# ----------------------------------------------------------------- 수집 작업
def collect_supply(cfg, con):
    n = save_supply(con, call_api(cfg, "today", rows=300))
    # 오늘 API의 최신값이 10분 이상 늦을 때만 현재 API로 보강 (일일 호출 한도 절약)
    last = con.execute("SELECT MAX(ts) FROM supply").fetchone()[0]
    stale = True
    if last:
        try:
            age = datetime.now(KST).replace(tzinfo=None) - datetime.strptime(last, "%Y-%m-%d %H:%M")
            stale = age > timedelta(minutes=10)
        except ValueError:
            pass
    if stale:
        try:
            n += save_supply(con, call_api(cfg, "current", rows=10))
        except ApiError as e:
            log.warning("현재전력수급 API 실패(무시하고 계속): %s", e)
    return n


def collect_gen(cfg, con):
    today = datetime.now(KST).strftime("%Y%m%d")
    return save_gen(con, call_api(cfg, "gen", {"baseDate": today}, rows=300))


def collect_smp(cfg, con):
    now = datetime.now(KST)
    n = 0
    for d in (now, now + timedelta(days=1)):
        try:
            n += save_smp(con, call_api(cfg, "smp", {"date": d.strftime("%Y%m%d")}, rows=100))
        except ApiError as e:
            if d is now:
                raise
            log.info("내일 SMP는 아직 미공개일 수 있습니다(보통 23시경 갱신): %s", e)
    return n



# ----------------------------------------------------------------- 한전 전력데이터 개방포털 (월간 통계)
KEPCO = {
    "cntr": "/openapi/v1/powerUsage/contractType.do",    # 계약종별 전력사용량·요금·판매단가
    "indus": "/openapi/v1/powerUsage/industryType.do",   # 산업분류별 전력사용량·요금·판매단가
    "renew": "/openapi/v1/renewEnergy.do",               # 신재생 에너지 발전원별 설치현황
}
TOTAL_NAMES = {"전체", "합계", "전국", "계", "총계"}


def _loads_lenient(text):
    """한전 API 응답이 표준 JSON과 조금 달라도 최대한 읽어냅니다.
    1) 제어문자 허용  2) 뒤에 붙은 내용 무시  3) NaN·쉼표 누락 보정  4) 행 단위로 하나씩 해석"""
    import re
    text = text.lstrip("\ufeff").strip()
    last_err = None
    try:
        return json.loads(text, strict=False)
    except json.JSONDecodeError as e:
        last_err = e
    try:  # 정상 JSON 뒤에 다른 내용이 붙은 경우
        obj, _ = json.JSONDecoder(strict=False).raw_decode(text)
        return obj
    except json.JSONDecodeError:
        pass
    fixed = re.sub(r"\b(NaN|Infinity|-Infinity|undefined)\b", "null", text)
    fixed = re.sub(r'([0-9"\]}eEl])(\s*)("[A-Za-z_][A-Za-z0-9_]*"\s*:)', r"\1,\2\3", fixed)   # "a":1 "b":2 -> 쉼표 보정
    fixed = re.sub(r":\s*(-?)\.(\d)", r":\g<1>0.\2", fixed)                                    # .5 -> 0.5
    fixed = re.sub(r":\s*,", ": null,", fixed)                                                       # "a":, -> null
    fixed = re.sub(r",\s*([}\]])", r"\1", fixed)                                                   # 끝 쉼표 제거
    try:
        return json.loads(fixed, strict=False)
    except json.JSONDecodeError:
        pass
    # 마지막 수단: {...} 행을 하나씩 해석 (깨진 행만 건너뜀)
    rows, bad = [], 0
    for m in re.finditer(r"\{[^{}\[\]]*\}", fixed):
        try:
            rows.append(json.loads(m.group(0), strict=False))
        except json.JSONDecodeError:
            bad += 1
    if rows:
        if bad:
            log.warning("한전 응답 중 %d개 행을 해석하지 못해 건너뛰었습니다.", bad)
        return {"totData": rows}
    pos = last_err.pos if last_err else 0
    raise ApiError("응답을 해석할 수 없습니다 (%s, 위치 %d 주변: %r)" % (
        last_err.msg if last_err else "?", pos, text[max(0, pos - 80):pos + 80]))


def call_kepco(cfg, name, params):
    key = cfg.get("kepco", "api_key")
    if not key:
        raise ApiError("config.ini [kepco] api_key 가 비어 있습니다.")
    q = {"apiKey": key, "returnType": "json"}
    q.update(params)
    url = cfg.get("kepco", "api_base").rstrip("/") + KEPCO[name] + "?" + urllib.parse.urlencode(q)
    req = urllib.request.Request(url, headers={"User-Agent": "kpx-dashboard/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            text = r.read().decode("utf-8", "replace").strip()
    except urllib.error.HTTPError as e:
        raise ApiError("HTTP %s: %s" % (e.code, e.read().decode("utf-8", "replace")[:200]))
    except urllib.error.URLError as e:
        raise ApiError("네트워크 오류: %s" % e.reason)
    if text.startswith("<"):
        try:
            root = ET.fromstring(text)
            rows = [{c.tag: c.text for c in it} for it in root.iter("data")] or \
                   [{c.tag: c.text for c in it} for it in root.iter("totData")]
            return [r for r in rows if r]
        except ET.ParseError:
            raise ApiError("응답을 해석할 수 없습니다: " + text[:200])
    data = _loads_lenient(text)
    for k in ("totData", "data"):
        v = _find_key(data, k)
        if isinstance(v, list):
            return v
    err = _find_key(data, "errMsg") or _find_key(data, "message") or _find_key(data, "resultMsg")
    if err:
        raise ApiError(str(err))
    return []


def _national(rows, cat):
    """지역별 행을 전국 합계로 묶기 (전국 합계 행이 있으면 그것을 우선 사용)"""
    tot = [r for r in rows if (r.get("metro") or "").strip() in TOTAL_NAMES]
    if not tot:
        metro_lvl = [r for r in rows if not (r.get("city") or "").strip() or (r.get("city") or "").strip() in TOTAL_NAMES]
        tot = metro_lvl or rows
    out = {}
    for r in tot:
        k = "".join(ch for ch in str(r.get(cat) or "") if ch >= " ").strip()
        if not k or k in TOTAL_NAMES:
            continue
        o = out.setdefault(k, {"cust": 0.0, "usage": 0.0, "bill": 0.0, "cntr_pwr": 0.0})
        o["cust"] += _num(r.get("custCnt")) or 0
        o["usage"] += _num(r.get("powerUsage") if r.get("powerUsage") is not None else r.get("powerUseage")) or 0
        o["bill"] += _num(r.get("bill")) or 0
        o["cntr_pwr"] += _num(r.get("cntrPwr")) or 0
    return out


def collect_kepco(cfg, con):
    """최근 N개월 월간 통계 중 아직 없는 달(또는 최근 3개월)만 받아 호출 횟수를 줄입니다."""
    if not cfg.get("kepco", "api_key"):
        return 0
    now = datetime.now(KST)
    back = max(3, int(cfg.get("kepco", "months_back")))
    done = {(a, p): n for a, p, n in con.execute("SELECT api, period, rows FROM kepco_done")}
    n = 0
    ym = now.replace(day=1)
    months = []
    for i in range(1, back + 1):
        ym = (ym - timedelta(days=1)).replace(day=1)
        months.append(ym.strftime("%Y%m"))
    for idx, m in enumerate(months):
        for api, cat in (("cntr", "cntr"), ("indus", "biz")):
            if done.get((api, m)) and idx >= 3:
                continue
            try:
                rows = call_kepco(cfg, api, {"year": m[:4], "month": m[4:]})
            except ApiError as e:
                log.warning("한전 %s %s 실패: %s", api, m, e)
                continue
            agg = _national(rows, cat)
            table = "kepco_cntr" if api == "cntr" else "kepco_indus"
            for k, o in agg.items():
                if api == "cntr":
                    con.execute("INSERT OR REPLACE INTO kepco_cntr VALUES(?,?,?,?,?,?)",
                                (m, k, o["cust"], o["usage"], o["bill"], o["cntr_pwr"]))
                else:
                    con.execute("INSERT OR REPLACE INTO kepco_indus VALUES(?,?,?,?,?)",
                                (m, k, o["cust"], o["usage"], o["bill"]))
            if agg:
                con.execute("INSERT OR REPLACE INTO kepco_done VALUES(?,?,?,?)",
                            (api, m, len(agg), now.strftime("%Y-%m-%d %H:%M")))
            n += len(agg)
            con.commit()
            time.sleep(0.3)
    for y in (now.year - 2, now.year - 1, now.year):
        y = str(y)
        if done.get(("renew", y)) and y != str(now.year):
            continue
        try:
            rows = call_kepco(cfg, "renew", {"year": y})
        except ApiError as e:
            log.warning("한전 신재생 %s 실패: %s", y, e)
            continue
        agg = {}
        seen_metro = set()
        for r in rows:
            g = (r.get("genSrc") or "").strip()
            if not g:
                continue
            o = agg.setdefault(g, {"cnt": 0.0, "cap": 0.0})
            mk = (g, (r.get("metro") or "").strip())
            if r.get("areaCapacity") is not None and mk not in seen_metro:
                seen_metro.add(mk)  # 시도 합계가 있으면 시도 단위로 한 번만 더함
                o["cnt"] += _num(r.get("areaCnt")) or 0
                o["cap"] += _num(r.get("areaCapacity")) or 0
            elif r.get("areaCapacity") is None:
                o["cnt"] += _num(r.get("cnt")) or 0
                o["cap"] += _num(r.get("capacity")) or 0
        for g, o in agg.items():
            con.execute("INSERT OR REPLACE INTO kepco_renew VALUES(?,?,?,?)", (y, g, o["cnt"], o["cap"]))
        if agg:
            con.execute("INSERT OR REPLACE INTO kepco_done VALUES(?,?,?,?)", ("renew", y, len(agg), now.strftime("%Y-%m-%d %H:%M")))
        n += len(agg)
        con.commit()
    return n


def kepco_json(con):
    cntr = _rows(con, "SELECT * FROM kepco_cntr ORDER BY ym")
    if not cntr:
        return None
    yms = sorted({r["ym"] for r in cntr})
    by = {}
    for r in cntr:
        by.setdefault(r["cntr"], {})[r["ym"]] = r
    total = {}
    for r in cntr:
        t = total.setdefault(r["ym"], {"usage": 0.0, "bill": 0.0, "cust": 0.0})
        t["usage"] += r["usage"] or 0; t["bill"] += r["bill"] or 0; t["cust"] += r["cust"] or 0
    order = sorted(by, key=lambda k: -sum((v["usage"] or 0) for v in by[k].values()))
    series = {k: {"usage": [(by[k].get(m) or {}).get("usage") for m in yms],
                  "unit": [((by[k][m]["bill"] / by[k][m]["usage"]) if m in by[k] and by[k][m]["usage"] else None) for m in yms]}
              for k in order}
    tot_unit = [(total[m]["bill"] / total[m]["usage"]) if total[m]["usage"] else None for m in yms]
    last = yms[-1]
    prev_y = str(int(last[:4]) - 1) + last[4:]
    indus = _rows(con, "SELECT * FROM kepco_indus WHERE ym=? ORDER BY usage DESC", (last,))
    indus_prev = {r["biz"]: r for r in _rows(con, "SELECT * FROM kepco_indus WHERE ym=?", (prev_y,))}
    for r in indus:
        p = indus_prev.get(r["biz"])
        r["yoy"] = round((r["usage"] / p["usage"] - 1) * 100, 1) if p and p["usage"] else None
        r["unit"] = r["bill"] / r["usage"] if r["usage"] else None
    ry = _rows(con, "SELECT MAX(year) AS y FROM kepco_renew")[0]["y"]
    renew = _rows(con, "SELECT * FROM kepco_renew WHERE year=? ORDER BY capacity DESC", (ry,)) if ry else []
    renew_years = _rows(con, "SELECT year, SUM(capacity) AS capacity, SUM(cnt) AS cnt FROM kepco_renew GROUP BY year ORDER BY year")
    return {"months": yms, "contract": series, "total_usage": [total[m]["usage"] for m in yms],
            "total_unit": tot_unit, "total_cust": [total[m]["cust"] for m in yms],
            "last": last, "prev_year": prev_y if prev_y in total else None,
            "last_total": total[last], "prev_total": total.get(prev_y),
            "indus": indus[:12], "renew_year": ry, "renew": renew, "renew_years": renew_years}

# ----------------------------------------------------------------- 대시보드 JSON
def _rows(con, sql, args=()):
    cur = con.execute(sql, args)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def build_json(con):
    now = datetime.now(KST)
    today = now.strftime("%Y-%m-%d")
    tomorrow = (now + timedelta(days=1)).strftime("%Y-%m-%d")

    sup = _rows(con, "SELECT * FROM supply WHERE ts LIKE ? ORDER BY ts", (today + "%",))
    gen = _rows(con, "SELECT * FROM gen WHERE ts LIKE ? ORDER BY ts", (today + "%",))
    latest = _rows(con, "SELECT * FROM supply ORDER BY ts DESC LIMIT 1")
    latest_gen = _rows(con, "SELECT * FROM gen ORDER BY ts DESC LIMIT 1")

    def smp_day(day):
        rows = _rows(con, "SELECT * FROM smp WHERE date=? ORDER BY hour", (day,))
        if not rows:
            return None
        out = {"date": day, "mainland": [None] * 24, "jeju": [None] * 24,
               "fcst_mainland": [None] * 24, "fcst_jeju": [None] * 24, "fcst_total": [None] * 24}
        for r in rows:
            i = int(r["hour"]) - 1
            if not 0 <= i < 24:
                continue
            key = "jeju" if "제주" in (r["area"] or "") else "mainland"
            out[key][i] = r["smp"]
            out["fcst_mainland"][i] = r["mlfd"]
            out["fcst_jeju"][i] = r["jlfd"]
            out["fcst_total"][i] = r["slfd"]
        return out

    since = (now - timedelta(days=60)).strftime("%Y-%m-%d")
    hist = _rows(con, """
        SELECT substr(ts,1,10) AS date, MAX(demand) AS max_demand, MIN(reserve) AS min_reserve,
               MIN(reserve_rate) AS min_reserve_rate, AVG(demand) AS avg_demand
        FROM supply WHERE ts >= ? GROUP BY 1 ORDER BY 1""", (since,))
    ghist = {r["date"]: r for r in _rows(con, """
        SELECT substr(ts,1,10) AS date, AVG(gas) AS gas, AVG(total) AS total, MAX(solar) AS max_solar,
               MAX(gas) AS max_gas, MIN(gas) AS min_gas
        FROM gen WHERE ts >= ? GROUP BY 1""", (since,))}
    shist = {r["date"]: r for r in _rows(con, """
        SELECT date, AVG(smp) AS avg_smp, MAX(smp) AS max_smp, MIN(smp) AS min_smp
        FROM smp WHERE area NOT LIKE '%제주%' AND date >= ? GROUP BY date""", (since,))}
    for h in hist:
        g, s = ghist.get(h["date"]), shist.get(h["date"])
        h["gas_share"] = round(g["gas"] / g["total"] * 100, 2) if g and g["total"] else None
        h["max_solar"] = g["max_solar"] if g else None
        h["avg_smp"] = s["avg_smp"] if s else None
        h["max_smp"] = s["max_smp"] if s else None

    def rec(sql):
        r = _rows(con, sql)
        return r[0] if r and r[0].get("v") is not None else None

    records = {
        "max_demand": rec("SELECT demand AS v, ts AS t FROM supply ORDER BY demand DESC LIMIT 1"),
        "min_reserve": rec("SELECT reserve AS v, ts AS t FROM supply WHERE reserve IS NOT NULL ORDER BY reserve ASC LIMIT 1"),
        "max_solar": rec("SELECT solar AS v, ts AS t FROM gen ORDER BY solar DESC LIMIT 1"),
        "max_gas": rec("SELECT gas AS v, ts AS t FROM gen ORDER BY gas DESC LIMIT 1"),
        "max_smp": rec("SELECT smp AS v, date||' '||hour||'시' AS t FROM smp WHERE area NOT LIKE '%제주%' AND date <= '" + today + "' ORDER BY smp DESC LIMIT 1"),
        "min_smp": rec("SELECT smp AS v, date||' '||hour||'시' AS t FROM smp WHERE area NOT LIKE '%제주%' AND smp IS NOT NULL AND date <= '" + today + "' ORDER BY smp ASC LIMIT 1"),
        "since": (_rows(con, "SELECT MIN(ts) AS t FROM supply") or [{}])[0].get("t"),
    }
    payload = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "sample": False,
        "latest": latest[0] if latest else None,
        "latest_gen": latest_gen[0] if latest_gen else None,
        "supply_today": sup,
        "gen_today": gen,
        "smp_today": smp_day(today),
        "smp_tomorrow": smp_day(tomorrow),
        "history": hist,
        "records": records,
        "fuel_labels": {col: ko for _, col, ko in FUEL},
        "kepco": kepco_json(con),
    }
    tmp = JSON_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp, JSON_PATH)
    return payload


# ----------------------------------------------------------------- 스케줄러
class Collector(threading.Thread):
    daemon = True

    def __init__(self, cfg):
        super().__init__()
        self.cfg = cfg
        s = cfg["schedule"]
        self.jobs = [
            ["전력수급", collect_supply, max(5, int(s.get("supply_minutes"))) * 60, 0],
            ["발전원별", collect_gen, max(5, int(s.get("gen_minutes"))) * 60, 0],
            ["SMP", collect_smp, max(1, int(s.get("smp_hours"))) * 3600, 0],
        ]
        if cfg.get("kepco", "api_key"):
            self.jobs.append(["한전 통계", collect_kepco, 24 * 3600, 0])

    def run_once(self, con, force=False, persist=False):
        """persist=True(--once, GitHub Actions): 1시간 이상 주기 작업(SMP·한전)의 다음 실행 시각을 DB에 기억"""
        changed = False
        for job in self.jobs:
            name, fn, every, nxt = job
            key = "next:" + name
            if persist:
                if every < 3600:
                    nxt = 0
                else:
                    row = con.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
                    nxt = float(row[0]) if row else 0
            if force or time.time() >= nxt:
                try:
                    n = fn(self.cfg, con)
                    log.info("%s 수집 완료 (%d건)", name, n)
                    changed = True
                except ApiError as e:
                    log.error("%s 수집 실패: %s", name, e)
                except Exception:
                    log.exception("%s 수집 중 예외", name)
                job[3] = time.time() + every
                if persist and every >= 3600:
                    con.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, str(job[3])))
                    con.commit()
            elif persist:
                log.info("%s 건너뜀 (다음 수집 %s)", name, datetime.fromtimestamp(nxt, KST).strftime("%m-%d %H:%M"))
        if changed or not os.path.exists(JSON_PATH):
            build_json(con)
            log.info("dashboard.json 갱신")

    def run(self):
        con = db()
        self.run_once(con, force=True)
        while True:
            time.sleep(20)
            self.run_once(con)


# ----------------------------------------------------------------- 웹서버
class Handler(BaseHTTPRequestHandler):
    ROUTES = {"/": "dashboard.html", "/dashboard.html": "dashboard.html",
              "/data/dashboard.json": os.path.join("data", "dashboard.json")}

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        rel = self.ROUTES.get(path)  # 허용 목록만 제공 (config.ini 등은 노출하지 않음)
        if not rel:
            self.send_error(404)
            return
        full = os.path.join(BASE_DIR, rel)
        if not os.path.exists(full):
            self.send_error(404, "아직 데이터가 없습니다")
            return
        with open(full, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8" if rel.endswith(".json")
                         else "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


# ----------------------------------------------------------------- 점검 모드
def self_test(cfg):
    print("\n[연결 점검] 인증키 앞 6자리: %s******" % cfg.get("kpx", "service_key")[:6])
    today = datetime.now(KST).strftime("%Y%m%d")
    tests = [("오늘전력수급현황", "today", {}, 3), ("현재전력수급현황", "current", {}, 3),
             ("발전원별 발전량", "gen", {"baseDate": today}, 3), ("SMP·수요예측", "smp", {"date": today}, 3)]
    ok = 0
    for label, name, params, rows in tests:
        try:
            items = call_api(cfg, name, params, rows=rows)
            print("  ✔ %-12s 정상  (받은 행 %d개)  예: %s" % (label, len(items),
                  json.dumps(items[0], ensure_ascii=False)[:160] if items else "(데이터 없음)"))
            ok += 1
        except ApiError as e:
            print("  ✘ %-12s 실패  %s" % (label, e))
    print("\n결과: %d/4 정상" % ok)
    if cfg.get("kepco", "api_key"):
        print("\n[한전 전력데이터 개방포털 점검]")
        ym = (datetime.now(KST).replace(day=1) - timedelta(days=75)).strftime("%Y%m")
        for label, name, params in [("계약종별 전력사용량", "cntr", {"year": ym[:4], "month": ym[4:]}),
                                    ("산업분류별 전력사용량", "indus", {"year": ym[:4], "month": ym[4:]}),
                                    ("신재생 에너지 현황", "renew", {"year": str(datetime.now(KST).year - 1)})]:
            try:
                rows = call_kepco(cfg, name, params)
                print("  ✔ %-12s 정상  (%s, 받은 행 %d개)  예: %s" % (label, params.get("month") and ym or params["year"], len(rows),
                      json.dumps(rows[0], ensure_ascii=False)[:140] if rows else "(데이터 없음)"))
            except ApiError as e:
                print("  ✘ %-12s 실패  %s" % (label, e))
    else:
        print("\n(한전 통계는 config.ini [kepco] api_key 를 넣으면 함께 점검합니다)")
    if ok < 4:
        print("  · 공공데이터포털에서 해당 API를 각각 '활용신청'했는지 확인하세요 (승인 후 반영까지 1~2시간 걸릴 수 있음).")
        print("  · 오류 코드 30(등록되지 않은 키)은 키 복사 오류나 미승인, 22는 일일 호출 한도 초과입니다.")
    return ok


def main():
    ap = argparse.ArgumentParser(description="대한민국 전력계통 현황 대시보드 수집기")
    ap.add_argument("--test", action="store_true", help="API 연결만 점검")
    ap.add_argument("--once", action="store_true", help="한 번 수집 후 종료 (SMP·한전은 주기에 맞을 때만)")
    ap.add_argument("--force", action="store_true", help="--once 와 함께: 모든 항목을 지금 수집")
    ap.add_argument("--port", type=int, help="웹서버 포트 (기본 8000)")
    args = ap.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%m-%d %H:%M:%S",
                        handlers=[logging.StreamHandler(sys.stdout),
                                  logging.FileHandler(os.path.join(DATA_DIR, "collector.log"), encoding="utf-8")])
    cfg = load_config()
    if not cfg.get("kpx", "service_key"):
        print("config.ini 파일의 service_key 에 공공데이터포털 인증키를 넣어주세요. (README.md 참고)")
        sys.exit(1)

    if args.test:
        sys.exit(0 if self_test(cfg) == 4 else 1)
    if args.once:
        Collector(cfg).run_once(db(), force=args.force, persist=True)
        return

    Collector(cfg).start()
    port = args.port or cfg.getint("server", "port")
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    url = "http://127.0.0.1:%d/" % port
    log.info("대시보드 주소: %s  (종료: Ctrl+C)", url)
    if cfg.get("server", "open_browser").lower() in ("yes", "true", "1"):
        threading.Timer(2.5, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        log.info("종료합니다.")


if __name__ == "__main__":
    main()
