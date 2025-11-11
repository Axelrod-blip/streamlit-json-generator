"""
Генератор и обновление JSON (один файл)
=======================================
Простой Streamlit-инструмент для работы с Product Offering Group и Category.
Минимальный UI: одна кнопка "Выполнить", счётчики, скачивание ZIP.
+ Детализация пропусков (skipped_existing) и выгрузка CSV.
"""

import io
import json
import zipfile
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
import streamlit as st

# =========================
# КОНСТАНТЫ
# =========================
DEFAULT_LOCALE = "en-US"
POG_DIR = "productOfferingGroup"
POC_DIR = "productOfferingCategory"

SAFE_NAME_PATTERN = re.compile(r"[^0-9A-Za-z_\-\u0400-\u04FF]")
WHITESPACE_PATTERN = re.compile(r"\s+")


# =========================
# УТИЛИТЫ
# =========================
def _safe_name(name: str) -> str:
    if not isinstance(name, str):
        name = str(name)
    s = WHITESPACE_PATTERN.sub("_", name.strip())
    s = SAFE_NAME_PATTERN.sub("", s)
    return s or "file"


def _normalize_str(v: Any) -> str:
    if pd.isna(v):
        return ""
    s = str(v).strip()
    return "" if s.lower() == "nan" else s


def _normalize_id(v: Any) -> str:
    s = _normalize_str(v)
    return s if s else ""


def _json_dumps_stable(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=4, sort_keys=True)


def _read_table(excel_bytes: bytes, expected_cols: List[str]) -> pd.DataFrame:
    """
    Универсальный ридер: пробует XLSX/XLS, при ошибке — CSV (UTF-8/auto).
    Строго проверяет наличие expected_cols.
    """
    buf = io.BytesIO(excel_bytes)

    # Пробуем Excel
    try:
        df = pd.read_excel(buf, engine="openpyxl")
    except Exception:
        # Fallback: CSV с автодетектом
        buf.seek(0)
        try:
            df = pd.read_csv(buf)
        except Exception:
            buf.seek(0)
            df = pd.read_csv(buf, sep=";", engine="python")

    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise KeyError(f"Нет требуемого столбца(ов): {', '.join(missing)}")

    return df[expected_cols].copy()


# =========================
# ZIP/JSON I/O
# =========================
def _read_zip(zip_bytes: bytes) -> Tuple[List[str], Dict[str, bytes]]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        names = zf.namelist()
        content = {n: zf.read(n) for n in names}
    return names, content


def _list_json_in_dir(bytes_map: Dict[str, bytes], dir_name: str) -> List[str]:
    prefix = f"{dir_name}/"
    return [n for n in bytes_map if n.startswith(prefix) and n.endswith(".json")]


def _load_json(data: bytes) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(data.decode("utf-8"))
    except Exception:
        return None


def _build_new_zip(original_names: List[str], original_bytes: Dict[str, bytes],
                   updated_json_map: Dict[str, str]) -> io.BytesIO:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name in original_names:
            if name in updated_json_map:
                zf.writestr(name, updated_json_map[name].encode("utf-8"))
            else:
                zf.writestr(name, original_bytes[name])
    buf.seek(0)
    return buf


# =========================
# BUILDERS
# =========================
def _make_offering(offer_id: str, name: Optional[str] = None,
                   locale: str = DEFAULT_LOCALE, expired: bool = False) -> Dict[str, Any]:
    item: Dict[str, Any] = {
        "id": offer_id,
        "isBundle": False,
        "expiredForSales": expired
    }
    if name:
        item["name"] = [{"locale": locale, "value": name}]
    return item


def _build_pog_addon(json_name: str, json_id: str, locale: str,
                     offerings: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "effective": True,
        "externalId": [],
        "id": json_id,
        "localizedName": [{"locale": locale, "value": json_name}],
        "name": _safe_name(json_name),
        "policy": [],
        "productOfferingsInGroup": sorted(offerings, key=lambda x: x["id"]),
        "purpose": ["addOn"],
        "restriction": []
    }


def _build_pog_replace(json_name: str, json_id: str, locale: str,
                       offerings: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "description": [{"locale": locale, "value": json_name}],
        "effective": True,
        "externalId": [],
        "id": json_id,
        "localizedName": [{"locale": locale, "value": json_name}],
        "name": _safe_name(json_name),
        "policy": [],
        "productOfferingsInGroup": sorted(offerings, key=lambda x: x["id"]),
        "purpose": ["replaceOffer"],
        "restriction": []
    }


def _build_category(offer_id: str, category_ids: List[str]) -> Dict[str, Any]:
    unique_sorted = sorted({cid for cid in (_normalize_id(c) for c in category_ids) if cid})
    return {
        "id": offer_id,
        "category": unique_sorted,
        "categoryRef": [{"id": cid} for cid in unique_sorted]
    }


# =========================
# РЕЗУЛЬТАТ ОПЕРАЦИЙ
# =========================
@dataclass
class SimpleResult:
    ok: bool
    msg: str
    zip_data: Optional[io.BytesIO]
    counts: Dict[str, int]
    details: Optional[Dict[str, Any]] = None


# =========================
# ОПЕРАЦИИ
# =========================
def generate_addon_from_excel(excel_bytes: bytes) -> SimpleResult:
    """
    1. Доступность услуги для некоторых тарифных планов.
    Excel/CSV: столбцы ТОЛЬКО с точными именами:
      - Addons name, Addons ID, Имя услуги, ID услуги
    Выход: ZIP с productOfferingGroup/<Addons ID>.json
    """
    try:
        expected = ["Addons name", "Addons ID", "Имя услуги", "ID услуги"]
        df = _read_table(excel_bytes, expected)
        for c in expected:
            df[c] = df[c].apply(_normalize_str)

        df = df[(df["Addons ID"] != "") & (df["ID услуги"] != "")]
        if df.empty:
            return SimpleResult(False, "В Excel нет валидных строк", None, {})

        groups = df.groupby(["Addons name", "Addons ID"])
        buf = io.BytesIO()
        created_jsons = 0
        services_total = 0

        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for (json_name, json_id), g in groups:
                # дедуп по ID услуги
                g = g.drop_duplicates(subset=["ID услуги"])
                offerings = []
                for _, r in g.iterrows():
                    sid = _normalize_id(r["ID услуги"])
                    sname = _normalize_str(r["Имя услуги"])
                    if not sid:
                        continue
                    offerings.append(_make_offering(sid, sname, DEFAULT_LOCALE))
                if not offerings:
                    continue
                pog = _build_pog_addon(_normalize_str(json_name), _normalize_id(json_id), DEFAULT_LOCALE, offerings)
                zf.writestr(f"{POG_DIR}/{_safe_name(json_id)}.json", _json_dumps_stable(pog))
                created_jsons += 1
                services_total += len(offerings)

        if created_jsons == 0:
            return SimpleResult(False, "Не удалось построить ни одного JSON", None, {})

        buf.seek(0)
        return SimpleResult(True, "Готово", buf, {
            "created_jsons": created_jsons,
            "services_total": services_total
        })
    except KeyError as e:
        return SimpleResult(False, f"Нет требуемого столбца: {e}", None, {})
    except Exception as e:
        return SimpleResult(False, f"Ошибка: {e}", None, {})


def add_services_to_existing_pogs(zip_bytes: bytes, excel_bytes: bytes) -> SimpleResult:
    """
    2. Добавление услуги в существующие планы.
    Excel/CSV: Addons ID, Имя услуги, ID услуги
    """
    try:
        names, blob = _read_zip(zip_bytes)
        json_files = _list_json_in_dir(blob, POG_DIR)
        if not json_files:
            return SimpleResult(False, f"В ZIP нет JSON в {POG_DIR}/", None, {})

        expected = ["Addons ID", "Имя услуги", "ID услуги"]
        df = _read_table(excel_bytes, expected)
        for c in expected:
            df[c] = df[c].apply(_normalize_str)
        df = df[(df["Addons ID"] != "") & (df["ID услуги"] != "")]
        service_map = df.groupby("Addons ID")[["Имя услуги", "ID услуги"]].apply(lambda x: x.to_dict("records")).to_dict()

        updated: Dict[str, str] = {}
        counts = {"files_processed": 0, "added": 0, "skipped_existing": 0, "not_found_json_id": 0, "invalid_target_type": 0}

        found_ids = set()
        skipped_rows: List[Dict[str, str]] = []  # детали пропусков

        for path in json_files:
            data = _load_json(blob[path])
            if not data:
                continue
            json_id = _normalize_id(data.get("id", ""))
            if not json_id or json_id not in service_map:
                continue

            found_ids.add(json_id)
            if data.get("purpose") != ["addOn"]:
                counts["invalid_target_type"] += 1
                continue

            offerings = data.get("productOfferingsInGroup", [])
            existing = {_normalize_id(o.get("id", "")) for o in offerings}

            modified = False
            for rec in service_map[json_id]:
                sid = _normalize_id(rec["ID услуги"])
                sname = _normalize_str(rec["Имя услуги"])
                if not sid:
                    continue
                if sid in existing:
                    counts["skipped_existing"] += 1
                    skipped_rows.append({
                        "json_id": json_id,
                        "service_id": sid,
                        "service_name": sname,
                        "reason": "already_exists_in_group"
                    })
                else:
                    offerings.append(_make_offering(sid, sname, DEFAULT_LOCALE))
                    existing.add(sid)
                    counts["added"] += 1
                    modified = True

            if modified:
                data["productOfferingsInGroup"] = sorted(offerings, key=lambda x: x["id"])
                updated[path] = _json_dumps_stable(data)
                counts["files_processed"] += 1

        for want_id in service_map.keys():
            if want_id not in found_ids:
                counts["not_found_json_id"] += 1

        details = {"skipped_existing": skipped_rows}
        if not updated:
            return SimpleResult(True, "Нет изменений", None, counts, details=details)

        buf = _build_new_zip(names, blob, updated)
        return SimpleResult(True, "Готово", buf, counts, details=details)
    except KeyError as e:
        return SimpleResult(False, f"Нет требуемого столбца: {e}", None, {})
    except Exception as e:
        return SimpleResult(False, f"Ошибка: {e}", None, {})


def expire_services_in_pogs(zip_bytes: bytes, excel_bytes: bytes) -> SimpleResult:
    """
    3. Экспайр услуги.
    Excel/CSV: json_id, service_id
    """
    try:
        names, blob = _read_zip(zip_bytes)
        json_files = _list_json_in_dir(blob, POG_DIR)
        if not json_files:
            return SimpleResult(False, f"В ZIP нет JSON в {POG_DIR}/", None, {})

        df = _read_table(excel_bytes, ["json_id", "service_id"])
        for c in ["json_id", "service_id"]:
            df[c] = df[c].apply(_normalize_str)
        df = df[(df["json_id"] != "") & (df["service_id"] != "")]
        expire_map = df.groupby("json_id")["service_id"].apply(list).to_dict()

        updated: Dict[str, str] = {}
        counts = {
            "files_processed": 0,
            "expired": 0,
            "already_expired": 0,
            "invalid_target_type": 0,
            "not_found_json_id": 0,     # отсутствующие файлы
            "not_found_service_id": 0,  # отсутствующие услуги внутри найденного файла
        }

        found_ids = set()

        for path in json_files:
            data = _load_json(blob[path])
            if not data:
                continue
            json_id = _normalize_id(data.get("id", ""))
            if not json_id or json_id not in expire_map:
                continue

            found_ids.add(json_id)

            if data.get("purpose") != ["addOn"]:
                counts["invalid_target_type"] += 1
                continue

            offerings = data.get("productOfferingsInGroup", [])
            index_by_id = {_normalize_id(o.get("id", "")): o for o in offerings}

            modified = False
            for sid in expire_map[json_id]:
                sid = _normalize_id(sid)
                o = index_by_id.get(sid)
                if o is None:
                    counts["not_found_service_id"] += 1
                    continue
                if not o.get("expiredForSales", False):
                    o["expiredForSales"] = True
                    counts["expired"] += 1
                    modified = True
                else:
                    counts["already_expired"] += 1

            if modified:
                data["productOfferingsInGroup"] = sorted(offerings, key=lambda x: x["id"])
                updated[path] = _json_dumps_stable(data)
                counts["files_processed"] += 1

        for want_id in expire_map.keys():
            if want_id not in found_ids:
                counts["not_found_json_id"] += 1

        if not updated:
            return SimpleResult(True, "Нет изменений", None, counts)

        buf = _build_new_zip(names, blob, updated)
        return SimpleResult(True, "Готово", buf, counts)
    except KeyError as e:
        return SimpleResult(False, f"Нет требуемого столбца: {e}", None, {})
    except Exception as e:
        return SimpleResult(False, f"Ошибка: {e}", None, {})


def create_replace_offer_from_excel(excel_bytes: bytes, json_name: str, json_id: str) -> SimpleResult:
    """
    1. Добавление перехода для одного тарифного плана.
    Excel/CSV: offer_id (одна колонка)
    """
    try:
        df = _read_table(excel_bytes, ["offer_id"])
        df["offer_id"] = df["offer_id"].apply(_normalize_str)
        df = df[df["offer_id"] != ""]
        if df.empty:
            return SimpleResult(False, "В Excel нет валидных строк", None, {})

        offers = [_make_offering(_normalize_id(r["offer_id"])) for _, r in df.iterrows() if _normalize_id(r["offer_id"])]
        pog = _build_pog_replace(_normalize_str(json_name), _normalize_id(json_id), DEFAULT_LOCALE, offers)

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(f"{POG_DIR}/{_safe_name(json_id)}.json", _json_dumps_stable(pog))
        buf.seek(0)
        return SimpleResult(True, "Готово", buf, {"created_jsons": 1, "offers_total": len(offers)})
    except KeyError as e:
        return SimpleResult(False, f"Нет требуемого столбца: {e}", None, {})
    except Exception as e:
        return SimpleResult(False, f"Ошибка: {e}", None, {})


def add_offer_to_transitions(zip_bytes: bytes, excel_bytes: bytes, offer_id: str) -> SimpleResult:
    """
    2. Добавление нового тарифа в переходы.
    Excel/CSV: json_id (список переходов)
    """
    try:
        names, blob = _read_zip(zip_bytes)
        json_files = _list_json_in_dir(blob, POG_DIR)
        if not json_files:
            return SimpleResult(False, f"В ZIP нет JSON в {POG_DIR}/", None, {})

        df = _read_table(excel_bytes, ["json_id"])
        df["json_id"] = df["json_id"].apply(_normalize_str)
        target_ids = {x for x in df["json_id"].tolist() if x}

        updated: Dict[str, str] = {}
        counts = {"files_processed": 0, "added": 0, "skipped_existing": 0, "not_found_json_id": 0, "invalid_target_type": 0}
        seen = set()

        skipped_rows: List[Dict[str, str]] = []  # детали пропусков

        for path in json_files:
            data = _load_json(blob[path])
            if not data:
                continue
            jid = _normalize_id(data.get("id", ""))
            if not jid or jid not in target_ids:
                continue

            seen.add(jid)
            if data.get("purpose") != ["replaceOffer"]:
                counts["invalid_target_type"] += 1
                continue

            offerings = data.get("productOfferingsInGroup", [])
            existing = {_normalize_id(o.get("id", "")) for o in offerings}
            want = _normalize_id(offer_id)

            if want in existing:
                counts["skipped_existing"] += 1
                skipped_rows.append({
                    "json_id": jid,
                    "offer_id": want,
                    "reason": "already_exists_in_group"
                })
                continue

            offerings.append(_make_offering(want))
            data["productOfferingsInGroup"] = sorted(offerings, key=lambda x: x["id"])
            updated[path] = _json_dumps_stable(data)
            counts["added"] += 1
            counts["files_processed"] += 1

        for want in target_ids:
            if want not in seen:
                counts["not_found_json_id"] += 1

        details = {"skipped_existing": skipped_rows}
        if not updated:
            return SimpleResult(True, "Нет изменений", None, counts, details=details)

        buf = _build_new_zip(names, blob, updated)
        return SimpleResult(True, "Готово", buf, counts, details=details)
    except KeyError as e:
        return SimpleResult(False, f"Нет требуемого столбца: {e}", None, {})
    except Exception as e:
        return SimpleResult(False, f"Ошибка: {e}", None, {})


def expire_offer_in_transitions(zip_bytes: bytes, excel_bytes: bytes) -> SimpleResult:
    """
    3. Экспайр тарифного плана в переходах.
    Excel/CSV: json_id, offer_id
    """
    try:
        names, blob = _read_zip(zip_bytes)
        json_files = _list_json_in_dir(blob, POG_DIR)
        if not json_files:
            return SimpleResult(False, f"В ZIP нет JSON в {POG_DIR}/", None, {})

        df = _read_table(excel_bytes, ["json_id", "offer_id"])
        for c in ["json_id", "offer_id"]:
            df[c] = df[c].apply(_normalize_str)
        df = df[(df["json_id"] != "") & (df["offer_id"] != "")]
        expire_map = df.groupby("json_id")["offer_id"].apply(list).to_dict()

        updated: Dict[str, str] = {}
        counts = {
            "files_processed": 0,
            "expired": 0,
            "already_expired": 0,
            "invalid_target_type": 0,
            "not_found_json_id": 0,   # отсутствующие файлы
            "not_found_offer_id": 0,  # отсутствующие офферы внутри найденного файла
        }

        found_ids = set()

        for path in json_files:
            data = _load_json(blob[path])
            if not data:
                continue
            jid = _normalize_id(data.get("id", ""))
            if not jid or jid not in expire_map:
                continue

            found_ids.add(jid)

            if data.get("purpose") != ["replaceOffer"]:
                counts["invalid_target_type"] += 1
                continue

            offerings = data.get("productOfferingsInGroup", [])
            index_by_id = {_normalize_id(o.get("id", "")): o for o in offerings}

            modified = False
            for oid in expire_map[jid]:
                oid = _normalize_id(oid)
                o = index_by_id.get(oid)
                if o is None:
                    counts["not_found_offer_id"] += 1
                    continue
                if not o.get("expiredForSales", False):
                    o["expiredForSales"] = True
                    counts["expired"] += 1
                    modified = True
                else:
                    counts["already_expired"] += 1

            if modified:
                data["productOfferingsInGroup"] = sorted(offerings, key=lambda x: x["id"])
                updated[path] = _json_dumps_stable(data)
                counts["files_processed"] += 1

        for want_id in expire_map.keys():
            if want_id not in found_ids:
                counts["not_found_json_id"] += 1

        if not updated:
            return SimpleResult(True, "Нет изменений", None, counts)

        buf = _build_new_zip(names, blob, updated)
        return SimpleResult(True, "Готово", buf, counts)
    except KeyError as e:
        return SimpleResult(False, f"Нет требуемого столбца: {e}", None, {})
    except Exception as e:
        return SimpleResult(False, f"Ошибка: {e}", None, {})


def generate_categories_from_excel(excel_bytes: bytes) -> SimpleResult:
    """
    Категории (ProductOfferingCategory).
    Excel/CSV: offer_id, category_id (replace-модель)
    """
    try:
        df = _read_table(excel_bytes, ["offer_id", "category_id"])
        for c in ["offer_id", "category_id"]:
            df[c] = df[c].apply(_normalize_str)
        df = df[(df["offer_id"] != "") & (df["category_id"] != "")]
        if df.empty:
            return SimpleResult(False, "В Excel нет валидных строк", None, {})

        groups = df.groupby("offer_id")["category_id"].apply(list).to_dict()
        buf = io.BytesIO()
        created = 0
        added = 0

        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for offer_id, cats in groups.items():
                cat_json = _build_category(_normalize_id(offer_id), [_normalize_id(x) for x in cats])
                zf.writestr(f"{POC_DIR}/{_safe_name(offer_id)}.json", _json_dumps_stable(cat_json))
                created += 1
                added += len(cat_json["category"])

        buf.seek(0)
        return SimpleResult(True, "Готово", buf, {"created_jsons": created, "added": added})
    except KeyError as e:
        return SimpleResult(False, f"Нет требуемого столбца: {e}", None, {})
    except Exception as e:
        return SimpleResult(False, f"Ошибка: {e}", None, {})


# =========================
# UI
# =========================
st.set_page_config(page_title="Генератор и обновление JSON", layout="wide", initial_sidebar_state="expanded")

st.title("Генератор и обновление JSON")
st.caption("Управление услугами (AddOns), переходами тарифных планов и категориями")

st.sidebar.title("Навигация")
main_section = st.sidebar.radio("Выберите раздел:", ["Услуги (AddOns)", "Переходы тарифных планов", "Категории"])

def _show_counts(counts: Dict[str, int]):
    if not counts:
        return
    items = list(counts.items())
    for i in range(0, len(items), 4):
        cols = st.columns(4)
        for j, (k, v) in enumerate(items[i:i+4]):
            with cols[j]:
                st.metric(k, v)

def _show_skipped_details(details: Optional[Dict[str, Any]], filename: str = "skipped_details.csv"):
    if not details:
        return
    rows = details.get("skipped_existing") or []
    if not rows:
        return
    with st.expander(f"Детали пропусков (skipped_existing): {len(rows)}", expanded=False):
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, height=320)
        csv_buf = io.StringIO()
        df.to_csv(csv_buf, index=False)
        st.download_button(
            "Скачать детали (CSV)",
            csv_buf.getvalue().encode("utf-8-sig"),
            file_name=filename,
            mime="text/csv",
        )

# --------- Раздел 1: Услуги ----------
if main_section == "Услуги (AddOns)":
    st.header("Работа с услугами")
    scenario = st.radio(
        "Выберите операцию:",
        [
            "1. Доступность услуги для некоторых тарифных планов",
            "2. Добавление услуги в существующие планы",
            "3. Экспайр услуги"
        ]
    )

    if scenario.startswith("1."):
        st.subheader("Доступность услуги для некоторых тарифных планов")
        st.info("Excel/CSV должен содержать столбцы: Addons name, Addons ID, Имя услуги, ID услуги")
        excel_file = st.file_uploader("Загрузите Excel/CSV", type=["xlsx", "xls", "csv"])
        if st.button("Выполнить"):
            if not excel_file:
                st.error("Загрузите Excel/CSV")
            else:
                with st.spinner("Обработка..."):
                    res = generate_addon_from_excel(excel_file.read())
                if not res.ok:
                    st.error(res.msg)
                else:
                    st.success(res.msg)
                    _show_counts(res.counts)
                    st.download_button("Скачать ZIP", res.zip_data, "addons.zip", "application/zip")

    elif scenario.startswith("2."):
        st.subheader("Добавление услуги в существующие планы")
        st.info("Excel/CSV должен содержать столбцы: Addons ID, Имя услуги, ID услуги")
        zip_file = st.file_uploader("Загрузите ZIP с планами", type=["zip"])
        excel_file = st.file_uploader("Загрузите Excel/CSV с услугами", type=["xlsx", "xls", "csv"])
        if st.button("Выполнить"):
            if not zip_file or not excel_file:
                st.error("Загрузите ZIP и Excel/CSV")
            else:
                with st.spinner("Обработка..."):
                    res = add_services_to_existing_pogs(zip_file.read(), excel_file.read())
                if not res.ok:
                    st.error(res.msg)
                else:
                    st.success(res.msg)
                    _show_counts(res.counts)
                    if res.details:
                        _show_skipped_details(res.details, filename="skipped_services_existing.csv")
                    if res.zip_data:
                        st.download_button("Скачать ZIP", res.zip_data, "updated_addons.zip", "application/zip")

    else:
        st.subheader("Экспайр услуги")
        st.info("Excel/CSV должен содержать столбцы: json_id, service_id")
        zip_file = st.file_uploader("Загрузите ZIP с планами", type=["zip"])
        excel_file = st.file_uploader("Загрузите Excel/CSV со списком к экспайру", type=["xlsx", "xls", "csv"])
        if st.button("Выполнить"):
            if not zip_file or not excel_file:
                st.error("Загрузите ZIP и Excel/CSV")
            else:
                with st.spinner("Обработка..."):
                    res = expire_services_in_pogs(zip_file.read(), excel_file.read())
                if not res.ok:
                    st.error(res.msg)
                else:
                    st.success(res.msg)
                    _show_counts(res.counts)
                    if res.zip_data:
                        st.download_button("Скачать ZIP", res.zip_data, "expired_addons.zip", "application/zip")

# --------- Раздел 2: Переходы ----------
elif main_section == "Переходы тарифных планов":
    st.header("Работа с переходами (replaceOffer)")
    scenario = st.radio(
        "Выберите операцию:",
        [
            "1. Создать переход для одного тарифного плана",
            "2. Добавить тариф в переходы",
            "3. Экспайр тарифа в переходах"
        ]
    )

    if scenario.startswith("1."):
        st.subheader("Создать новый переход")
        st.info("Excel/CSV должен содержать столбец: offer_id")
        excel_file = st.file_uploader("Загрузите Excel/CSV с offer_id", type=["xlsx", "xls", "csv"])
        col1, col2 = st.columns(2)
        with col1:
            json_name = st.text_input("Название перехода", placeholder="Replace for ...")
        with col2:
            json_id = st.text_input("ID перехода")
        if st.button("Выполнить"):
            if not excel_file or not json_name or not json_id:
                st.error("Заполните все поля и загрузите Excel/CSV")
            else:
                with st.spinner("Обработка..."):
                    res = create_replace_offer_from_excel(excel_file.read(), json_name, json_id)
                if not res.ok:
                    st.error(res.msg)
                else:
                    st.success(res.msg)
                    _show_counts(res.counts)
                    st.download_button("Скачать ZIP", res.zip_data, "replace_offer.zip", "application/zip")

    elif scenario.startswith("2."):
        st.subheader("Добавить тариф в переходы")
        st.info("Excel/CSV должен содержать столбец: json_id (ID перехода)")
        zip_file = st.file_uploader("Загрузите ZIP с переходами", type=["zip"])
        excel_file = st.file_uploader("Загрузите Excel/CSV со списком переходов", type=["xlsx", "xls", "csv"])
        offer_id = st.text_input("ID тарифного плана (offer_id)")
        if st.button("Выполнить"):
            if not zip_file or not excel_file or not offer_id:
                st.error("Заполните все поля и загрузите файлы")
            else:
                with st.spinner("Обработка..."):
                    res = add_offer_to_transitions(zip_file.read(), excel_file.read(), offer_id)
                if not res.ok:
                    st.error(res.msg)
                else:
                    st.success(res.msg)
                    _show_counts(res.counts)
                    if res.details:
                        _show_skipped_details(res.details, filename="skipped_offers_existing.csv")
                    if res.zip_data:
                        st.download_button("Скачать ZIP", res.zip_data, "updated_replace_offers.zip", "application/zip")

    else:
        st.subheader("Экспайр тарифа в переходах")
        st.info("Excel/CSV должен содержать столбцы: json_id, offer_id")
        zip_file = st.file_uploader("Загрузите ZIP с переходами", type=["zip"])
        excel_file = st.file_uploader("Загрузите Excel/CSV", type=["xlsx", "xls", "csv"])
        if st.button("Выполнить"):
            if not zip_file or not excel_file:
                st.error("Загрузите ZIP и Excel/CSV")
            else:
                with st.spinner("Обработка..."):
                    res = expire_offer_in_transitions(zip_file.read(), excel_file.read())
                if not res.ok:
                    st.error(res.msg)
                else:
                    st.success(res.msg)
                    _show_counts(res.counts)
                    if res.zip_data:
                        st.download_button("Скачать ZIP", res.zip_data, "expired_replace_offers.zip", "application/zip")

# --------- Раздел 3: Категории ----------
else:
    st.header("Категории (ProductOfferingCategory)")
    st.subheader("Сгенерировать категории из Excel/CSV")
    st.info("Excel/CSV должен содержать столбцы: offer_id, category_id (несколько строк на один offer_id объединяются)")
    excel_file = st.file_uploader("Загрузите Excel/CSV", type=["xlsx", "xls", "csv"])
    if st.button("Выполнить"):
        if not excel_file:
            st.error("Загрузите Excel/CSV")
        else:
            with st.spinner("Обработка..."):
                res = generate_categories_from_excel(excel_file.read())
            if not res.ok:
                st.error(res.msg)
            else:
                st.success(res.msg)
                _show_counts(res.counts)
                st.download_button("Скачать ZIP", res.zip_data, "categories.zip", "application/zip")
