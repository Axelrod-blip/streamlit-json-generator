"""
Генератор и обновление JSON (один файл)
=======================================
Простой Streamlit-инструмент для работы с Product Offering Group и Category.
Минимальный UI + детальный вывод всех ошибок и пропусков.
"""

import io
import json
import zipfile
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple, Optional
from enum import Enum

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
# ТИПЫ ПРОБЛЕМ
# =========================
class IssueType(Enum):
    ALREADY_EXISTS = "already_exists"
    ALREADY_EXPIRED = "already_expired"
    ALREADY_ACTIVE = "already_active"  # <-- НОВЫЙ ТИП
    DUPLICATE_IN_SOURCE = "duplicate_in_source"
    NOT_FOUND_JSON_ID = "not_found_json_id"
    NOT_FOUND_SERVICE_ID = "not_found_service_id"
    NOT_FOUND_OFFER_ID = "not_found_offer_id"
    INVALID_TARGET_TYPE = "invalid_target_type"
    EMPTY_ID = "empty_id"
    INVALID_JSON = "invalid_json"
    MISSING_FIELD = "missing_field"


@dataclass
class Issue:
    """Детальная информация об ошибке или пропуске"""
    type: IssueType
    severity: str  # "warning", "error", "info"
    message: str
    context: Dict[str, Any] = field(default_factory=dict)
    row_number: Optional[int] = None
    file_path: Optional[str] = None


@dataclass
class SimpleResult:
    ok: bool
    msg: str
    zip_data: Optional[io.BytesIO]
    counts: Dict[str, int]
    issues: List[Issue] = field(default_factory=list)
    details: Optional[Dict[str, Any]] = None
    
    def add_issue(self, issue: Issue):
        self.issues.append(issue)


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


def _read_table(excel_bytes: bytes, expected_cols: List[str]) -> Tuple[pd.DataFrame, List[Issue]]:
    """Универсальный ридер с отслеживанием проблем"""
    issues = []
    buf = io.BytesIO(excel_bytes)

    try:
        df = pd.read_excel(buf, engine="openpyxl")
    except Exception as e:
        issues.append(Issue(
            type=IssueType.INVALID_JSON,
            severity="info",
            message=f"Не Excel, пробуем CSV: {str(e)[:50]}"
        ))
        buf.seek(0)
        try:
            df = pd.read_csv(buf)
        except Exception:
            buf.seek(0)
            try:
                df = pd.read_csv(buf, sep=";", engine="python")
            except Exception as e2:
                issues.append(Issue(
                    type=IssueType.INVALID_JSON,
                    severity="error",
                    message=f"Не удалось прочитать файл: {str(e2)}"
                ))
                raise

    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        issues.append(Issue(
            type=IssueType.MISSING_FIELD,
            severity="error",
            message=f"Нет столбца(ов): {', '.join(missing)}",
            context={"missing": missing, "available": list(df.columns)}
        ))
        raise KeyError(f"Нет требуемого столбца(ов): {', '.join(missing)}")

    return df[expected_cols].copy(), issues


# =========================
# ZIP/JSON I/O
# =========================
def _read_zip(zip_bytes: bytes) -> Tuple[List[str], Dict[str, bytes], List[Issue]]:
    issues = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            names = zf.namelist()
            content = {n: zf.read(n) for n in names}
        return names, content, issues
    except Exception as e:
        issues.append(Issue(
            type=IssueType.INVALID_JSON,
            severity="error",
            message=f"Ошибка чтения ZIP: {str(e)}"
        ))
        raise


def _list_json_in_dir(bytes_map: Dict[str, bytes], dir_name: str) -> List[str]:
    prefix = f"{dir_name}/"
    return [n for n in bytes_map if n.startswith(prefix) and n.endswith(".json")]


def _load_json(data: bytes, path: str, issues: List[Issue]) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        issues.append(Issue(
            type=IssueType.INVALID_JSON,
            severity="error",
            message=f"Невалидный JSON",
            file_path=path,
            context={"error": str(e)[:100]}
        ))
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
# ОПЕРАЦИИ
# =========================
def generate_addon_from_excel(excel_bytes: bytes) -> SimpleResult:
    """1. Доступность услуги для некоторых тарифных планов."""
    result = SimpleResult(False, "", None, {})
    
    try:
        expected = ["Addons name", "Addons ID", "Имя услуги", "ID услуги"]
        df, read_issues = _read_table(excel_bytes, expected)
        result.issues.extend(read_issues)
        
        for c in expected:
            df[c] = df[c].apply(_normalize_str)
        
        total_rows = len(df)
        
        # Отслеживание пустых ID
        for idx, row in df.iterrows():
            if not row["Addons ID"]:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="warning",
                    message="Пустой Addons ID",
                    row_number=idx + 2,
                    context={"addons_name": row["Addons name"]}
                ))
            if not row["ID услуги"]:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="warning",
                    message="Пустой ID услуги",
                    row_number=idx + 2,
                    context={"service_name": row["Имя услуги"]}
                ))
        
        df = df[(df["Addons ID"] != "") & (df["ID услуги"] != "")]
        
        if df.empty:
            result.msg = "В Excel нет валидных строк"
            return result
        
        result.counts["total_rows"] = total_rows
        result.counts["valid_rows"] = len(df)
        result.counts["skipped_rows"] = total_rows - len(df)
        
        groups = df.groupby(["Addons name", "Addons ID"])
        buf = io.BytesIO()
        created_jsons = 0
        services_total = 0
        
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for (json_name, json_id), g in groups:
                initial_count = len(g)
                g = g.drop_duplicates(subset=["ID услуги"])
                duplicates_count = initial_count - len(g)
                
                if duplicates_count > 0:
                    result.add_issue(Issue(
                        type=IssueType.DUPLICATE_IN_SOURCE,
                        severity="info",
                        message=f"Удалено дубликатов: {duplicates_count}",
                        context={"addons_id": json_id, "addons_name": json_name}
                    ))
                
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
            result.msg = "Не удалось построить ни одного JSON"
            return result
        
        result.counts["created_jsons"] = created_jsons
        result.counts["services_total"] = services_total
        buf.seek(0)
        result.ok = True
        result.msg = "Готово"
        result.zip_data = buf
        
    except Exception as e:
        result.add_issue(Issue(
            type=IssueType.INVALID_JSON,
            severity="error",
            message=f"Критическая ошибка: {str(e)}"
        ))
        result.msg = f"Ошибка: {e}"
    
    return result


def add_services_to_existing_pogs(zip_bytes: bytes, excel_bytes: bytes) -> SimpleResult:
    """2. Добавление услуги в существующие планы."""
    result = SimpleResult(False, "", None, {})
    
    try:
        names, blob, zip_issues = _read_zip(zip_bytes)
        result.issues.extend(zip_issues)
        
        json_files = _list_json_in_dir(blob, POG_DIR)
        if not json_files:
            result.msg = f"В ZIP нет JSON в {POG_DIR}/"
            return result
        
        result.counts["json_files_in_zip"] = len(json_files)
        
        expected = ["Addons ID", "Имя услуги", "ID услуги"]
        df, read_issues = _read_table(excel_bytes, expected)
        result.issues.extend(read_issues)
        
        total_rows = len(df)
        
        for c in expected:
            df[c] = df[c].apply(_normalize_str)
        
        for idx, row in df.iterrows():
            if not row["Addons ID"]:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="warning",
                    message="Пустой Addons ID",
                    row_number=idx + 2
                ))
            if not row["ID услуги"]:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="warning",
                    message="Пустой ID услуги",
                    row_number=idx + 2
                ))
        
        df = df[(df["Addons ID"] != "") & (df["ID услуги"] != "")]
        
        result.counts["total_rows"] = total_rows
        result.counts["valid_rows"] = len(df)
        
        service_map = df.groupby("Addons ID")[["Имя услуги", "ID услуги"]].apply(lambda x: x.to_dict("records")).to_dict()
        
        updated: Dict[str, str] = {}
        found_ids = set()
        skipped_rows: List[Dict[str, str]] = []
        
        for path in json_files:
            data = _load_json(blob[path], path, result.issues)
            if not data:
                continue
            
            json_id = _normalize_id(data.get("id", ""))
            if not json_id:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="error",
                    message="JSON без ID",
                    file_path=path
                ))
                continue
            
            if json_id not in service_map:
                continue
            
            found_ids.add(json_id)
            
            if data.get("purpose") != ["addOn"]:
                result.add_issue(Issue(
                    type=IssueType.INVALID_TARGET_TYPE,
                    severity="error",
                    message=f"Неверный purpose (ожидается addOn)",
                    file_path=path,
                    context={"json_id": json_id, "purpose": data.get("purpose")}
                ))
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
                    result.add_issue(Issue(
                        type=IssueType.ALREADY_EXISTS,
                        severity="info",
                        message=f"Услуга уже существует",
                        file_path=path,
                        context={"json_id": json_id, "service_id": sid, "service_name": sname}
                    ))
                    skipped_rows.append({
                        "json_id": json_id,
                        "service_id": sid,
                        "service_name": sname,
                        "reason": "already_exists_in_group"
                    })
                else:
                    offerings.append(_make_offering(sid, sname, DEFAULT_LOCALE))
                    existing.add(sid)
                    modified = True
            
            if modified:
                data["productOfferingsInGroup"] = sorted(offerings, key=lambda x: x["id"])
                updated[path] = _json_dumps_stable(data)
        
        for want_id in service_map.keys():
            if want_id not in found_ids:
                result.add_issue(Issue(
                    type=IssueType.NOT_FOUND_JSON_ID,
                    severity="error",
                    message=f"JSON файл не найден",
                    context={"addons_id": want_id}
                ))
        
        result.counts["files_processed"] = len(updated)
        result.counts["added"] = sum(1 for i in result.issues if i.type == IssueType.ALREADY_EXISTS)
        result.counts["skipped_existing"] = len(skipped_rows)
        result.details = {"skipped_existing": skipped_rows}
        
        if not updated:
            result.ok = True
            result.msg = "Нет изменений"
            return result
        
        buf = _build_new_zip(names, blob, updated)
        result.ok = True
        result.msg = "Готово"
        result.zip_data = buf
        
    except Exception as e:
        result.add_issue(Issue(
            type=IssueType.INVALID_JSON,
            severity="error",
            message=f"Критическая ошибка: {str(e)}"
        ))
        result.msg = f"Ошибка: {e}"
    
    return result


def expire_services_in_pogs(zip_bytes: bytes, excel_bytes: bytes) -> SimpleResult:
    """3. Экспайр услуги."""
    result = SimpleResult(False, "", None, {})
    
    try:
        names, blob, zip_issues = _read_zip(zip_bytes)
        result.issues.extend(zip_issues)
        
        json_files = _list_json_in_dir(blob, POG_DIR)
        if not json_files:
            result.msg = f"В ZIP нет JSON в {POG_DIR}/"
            return result
        
        result.counts["json_files_in_zip"] = len(json_files)
        
        df, read_issues = _read_table(excel_bytes, ["json_id", "service_id"])
        result.issues.extend(read_issues)
        
        total_rows = len(df)
        
        for c in ["json_id", "service_id"]:
            df[c] = df[c].apply(_normalize_str)
        
        for idx, row in df.iterrows():
            if not row["json_id"]:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="warning",
                    message="Пустой json_id",
                    row_number=idx + 2
                ))
            if not row["service_id"]:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="warning",
                    message="Пустой service_id",
                    row_number=idx + 2
                ))
        
        df = df[(df["json_id"] != "") & (df["service_id"] != "")]
        
        result.counts["total_rows"] = total_rows
        result.counts["valid_rows"] = len(df)
        
        expire_map = df.groupby("json_id")["service_id"].apply(list).to_dict()
        
        updated: Dict[str, str] = {}
        found_ids = set()
        
        for path in json_files:
            data = _load_json(blob[path], path, result.issues)
            if not data:
                continue
            
            json_id = _normalize_id(data.get("id", ""))
            if not json_id or json_id not in expire_map:
                continue
            
            found_ids.add(json_id)
            
            if data.get("purpose") != ["addOn"]:
                result.add_issue(Issue(
                    type=IssueType.INVALID_TARGET_TYPE,
                    severity="error",
                    message=f"Неверный purpose (ожидается addOn)",
                    file_path=path,
                    context={"json_id": json_id}
                ))
                continue
            
            offerings = data.get("productOfferingsInGroup", [])
            index_by_id = {_normalize_id(o.get("id", "")): o for o in offerings}
            
            modified = False
            for sid in expire_map[json_id]:
                sid = _normalize_id(sid)
                o = index_by_id.get(sid)
                if o is None:
                    result.add_issue(Issue(
                        type=IssueType.NOT_FOUND_SERVICE_ID,
                        severity="error",
                        message=f"Услуга не найдена",
                        file_path=path,
                        context={"json_id": json_id, "service_id": sid}
                    ))
                    continue
                
                if not o.get("expiredForSales", False):
                    o["expiredForSales"] = True
                    modified = True
                else:
                    result.add_issue(Issue(
                        type=IssueType.ALREADY_EXPIRED,
                        severity="info",
                        message=f"Услуга уже экспайрнута",
                        file_path=path,
                        context={"json_id": json_id, "service_id": sid}
                    ))
            
            if modified:
                data["productOfferingsInGroup"] = sorted(offerings, key=lambda x: x["id"])
                updated[path] = _json_dumps_stable(data)
        
        for want_id in expire_map.keys():
            if want_id not in found_ids:
                result.add_issue(Issue(
                    type=IssueType.NOT_FOUND_JSON_ID,
                    severity="error",
                    message=f"JSON файл не найден",
                    context={"json_id": want_id}
                ))
        
        result.counts["files_processed"] = len(updated)
        result.counts["expired"] = sum(1 for i in result.issues if i.type == IssueType.ALREADY_EXPIRED)
        
        if not updated:
            result.ok = True
            result.msg = "Нет изменений"
            return result
        
        buf = _build_new_zip(names, blob, updated)
        result.ok = True
        result.msg = "Готово"
        result.zip_data = buf
        
    except Exception as e:
        result.add_issue(Issue(
            type=IssueType.INVALID_JSON,
            severity="error",
            message=f"Критическая ошибка: {str(e)}"
        ))
        result.msg = f"Ошибка: {e}"
    
    return result


def activate_services_in_pogs(zip_bytes: bytes, excel_bytes: bytes) -> SimpleResult:
    """4. Активация услуги (снятие флага expiredForSales)."""
    result = SimpleResult(False, "", None, {})
    
    try:
        names, blob, zip_issues = _read_zip(zip_bytes)
        result.issues.extend(zip_issues)
        
        json_files = _list_json_in_dir(blob, POG_DIR)
        if not json_files:
            result.msg = f"В ZIP нет JSON в {POG_DIR}/"
            return result
        
        result.counts["json_files_in_zip"] = len(json_files)
        
        df, read_issues = _read_table(excel_bytes, ["json_id", "service_id"])
        result.issues.extend(read_issues)
        
        total_rows = len(df)
        
        for c in ["json_id", "service_id"]:
            df[c] = df[c].apply(_normalize_str)
        
        for idx, row in df.iterrows():
            if not row["json_id"]:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="warning",
                    message="Пустой json_id",
                    row_number=idx + 2
                ))
            if not row["service_id"]:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="warning",
                    message="Пустой service_id",
                    row_number=idx + 2
                ))
        
        df = df[(df["json_id"] != "") & (df["service_id"] != "")]
        
        result.counts["total_rows"] = total_rows
        result.counts["valid_rows"] = len(df)
        
        activate_map = df.groupby("json_id")["service_id"].apply(list).to_dict()
        
        updated: Dict[str, str] = {}
        found_ids = set()
        
        for path in json_files:
            data = _load_json(blob[path], path, result.issues)
            if not data:
                continue
            
            json_id = _normalize_id(data.get("id", ""))
            if not json_id or json_id not in activate_map:
                continue
            
            found_ids.add(json_id)
            
            if data.get("purpose") != ["addOn"]:
                result.add_issue(Issue(
                    type=IssueType.INVALID_TARGET_TYPE,
                    severity="error",
                    message=f"Неверный purpose (ожидается addOn)",
                    file_path=path,
                    context={"json_id": json_id}
                ))
                continue
            
            offerings = data.get("productOfferingsInGroup", [])
            index_by_id = {_normalize_id(o.get("id", "")): o for o in offerings}
            
            modified = False
            for sid in activate_map[json_id]:
                sid = _normalize_id(sid)
                o = index_by_id.get(sid)
                if o is None:
                    result.add_issue(Issue(
                        type=IssueType.NOT_FOUND_SERVICE_ID,
                        severity="error",
                        message=f"Услуга не найдена",
                        file_path=path,
                        context={"json_id": json_id, "service_id": sid}
                    ))
                    continue
                
                # Если флаг True, меняем на False.
                if o.get("expiredForSales", False) is True:
                    o["expiredForSales"] = False
                    modified = True
                else:
                    result.add_issue(Issue(
                        type=IssueType.ALREADY_ACTIVE,
                        severity="info",
                        message=f"Услуга уже активна",
                        file_path=path,
                        context={"json_id": json_id, "service_id": sid}
                    ))
            
            if modified:
                data["productOfferingsInGroup"] = sorted(offerings, key=lambda x: x["id"])
                updated[path] = _json_dumps_stable(data)
        
        for want_id in activate_map.keys():
            if want_id not in found_ids:
                result.add_issue(Issue(
                    type=IssueType.NOT_FOUND_JSON_ID,
                    severity="error",
                    message=f"JSON файл не найден",
                    context={"json_id": want_id}
                ))
        
        result.counts["files_processed"] = len(updated)
        result.counts["activated"] = sum(1 for i in result.issues if i.type == IssueType.ALREADY_ACTIVE)
        
        if not updated:
            result.ok = True
            result.msg = "Нет изменений"
            return result
        
        buf = _build_new_zip(names, blob, updated)
        result.ok = True
        result.msg = "Готово"
        result.zip_data = buf
        
    except Exception as e:
        result.add_issue(Issue(
            type=IssueType.INVALID_JSON,
            severity="error",
            message=f"Критическая ошибка: {str(e)}"
        ))
        result.msg = f"Ошибка: {e}"
    
    return result


def expire_and_add_services(zip_bytes: bytes, expire_excel: bytes, add_excel: bytes) -> SimpleResult:
    """5. Экспайр + Добавление услуги (две независимые операции)."""
    result = SimpleResult(False, "", None, {})
    
    try:
        # Читаем ZIP
        names, blob, zip_issues = _read_zip(zip_bytes)
        result.issues.extend(zip_issues)
        
        json_files = _list_json_in_dir(blob, POG_DIR)
        if not json_files:
            result.msg = f"В ZIP нет JSON в {POG_DIR}/"
            return result
        
        result.counts["json_files_in_zip"] = len(json_files)
        
        # === ЭТАП 1: Читаем файл для экспайра ===
        df_expire, expire_issues = _read_table(expire_excel, ["ID услуги", "Имя услуги"])
        result.issues.extend(expire_issues)
        
        total_expire_rows = len(df_expire)
        
        for c in ["ID услуги", "Имя услуги"]:
            df_expire[c] = df_expire[c].apply(_normalize_str)
        
        for idx, row in df_expire.iterrows():
            if not row["ID услуги"]:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="warning",
                    message="Пустой ID услуги для экспайра",
                    row_number=idx + 2
                ))
        
        df_expire = df_expire[df_expire["ID услуги"] != ""]
        
        result.counts["expire_total_rows"] = total_expire_rows
        result.counts["expire_valid_rows"] = len(df_expire)
        
        # Создаем set для быстрого поиска
        services_to_expire = {_normalize_id(row["ID услуги"]) for _, row in df_expire.iterrows()}
        
        # === ЭТАП 2: Читаем файл для добавления ===
        df_add, add_issues = _read_table(add_excel, ["ID услуги", "Имя услуги"])
        result.issues.extend(add_issues)
        
        total_add_rows = len(df_add)
        
        for c in ["ID услуги", "Имя услуги"]:
            df_add[c] = df_add[c].apply(_normalize_str)
        
        for idx, row in df_add.iterrows():
            if not row["ID услуги"]:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="warning",
                    message="Пустой ID услуги для добавления",
                    row_number=idx + 2
                ))
        
        df_add = df_add[df_add["ID услуги"] != ""]
        
        result.counts["add_total_rows"] = total_add_rows
        result.counts["add_valid_rows"] = len(df_add)
        
        # Проверка на пересечение (warning)
        services_to_add_ids = {_normalize_id(row["ID услуги"]) for _, row in df_add.iterrows()}
        overlap = services_to_expire & services_to_add_ids
        if overlap:
            result.add_issue(Issue(
                type=IssueType.DUPLICATE_IN_SOURCE,
                severity="warning",
                message=f"Услуги присутствуют в обоих файлах: {', '.join(list(overlap)[:5])}",
                context={"overlap_count": len(overlap)}
            ))
        
        # Создаем список услуг для добавления с именами
        services_to_add = []
        for _, row in df_add.iterrows():
            sid = _normalize_id(row["ID услуги"])
            sname = _normalize_str(row["Имя услуги"])
            if sid:
                services_to_add.append({"id": sid, "name": sname})
        
        # === ЭТАП 3: Обработка JSON файлов ===
        updated: Dict[str, str] = {}
        expired_count = 0
        added_count = 0
        skipped_expire_not_found = []
        skipped_add_existing = []
        
        for path in json_files:
            data = _load_json(blob[path], path, result.issues)
            if not data:
                continue
            
            json_id = _normalize_id(data.get("id", ""))
            if not json_id:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="error",
                    message="JSON без ID",
                    file_path=path
                ))
                continue
            
            # Проверяем purpose
            if data.get("purpose") != ["addOn"]:
                result.add_issue(Issue(
                    type=IssueType.INVALID_TARGET_TYPE,
                    severity="error",
                    message=f"Неверный purpose (ожидается addOn)",
                    file_path=path,
                    context={"json_id": json_id, "purpose": data.get("purpose")}
                ))
                continue
            
            offerings = data.get("productOfferingsInGroup", [])
            existing_ids = {_normalize_id(o.get("id", "")) for o in offerings}
            modified = False
            
            # --- Операция 1: Экспайр ---
            for offering in offerings:
                sid = _normalize_id(offering.get("id", ""))
                if sid in services_to_expire:
                    if not offering.get("expiredForSales", False):
                        offering["expiredForSales"] = True
                        expired_count += 1
                        modified = True
                    else:
                        result.add_issue(Issue(
                            type=IssueType.ALREADY_EXPIRED,
                            severity="info",
                            message=f"Услуга уже экспайрнута",
                            file_path=path,
                            context={"json_id": json_id, "service_id": sid}
                        ))
            
            # --- Операция 2: Добавление ---
            for service in services_to_add:
                sid = service["id"]
                sname = service["name"]
                
                if sid in existing_ids:
                    skipped_add_existing.append({
                        "json_id": json_id,
                        "service_id": sid,
                        "service_name": sname,
                        "reason": "already_exists"
                    })
                    result.add_issue(Issue(
                        type=IssueType.ALREADY_EXISTS,
                        severity="info",
                        message=f"Услуга уже существует",
                        file_path=path,
                        context={"json_id": json_id, "service_id": sid, "service_name": sname}
                    ))
                else:
                    offerings.append(_make_offering(sid, sname, DEFAULT_LOCALE))
                    existing_ids.add(sid)
                    added_count += 1
                    modified = True
            
            # Сохраняем изменения
            if modified:
                data["productOfferingsInGroup"] = sorted(offerings, key=lambda x: x["id"])
                updated[path] = _json_dumps_stable(data)
        
        # Проверяем, какие услуги для экспайра не были найдены
        found_expired = set()
        for path in json_files:
            data = _load_json(blob[path], path, [])
            if data:
                offerings = data.get("productOfferingsInGroup", [])
                for o in offerings:
                    sid = _normalize_id(o.get("id", ""))
                    if sid in services_to_expire:
                        found_expired.add(sid)
        
        not_found_expire = services_to_expire - found_expired
        for sid in not_found_expire:
            skipped_expire_not_found.append({
                "service_id": sid,
                "reason": "not_found_in_any_json"
            })
            result.add_issue(Issue(
                type=IssueType.NOT_FOUND_SERVICE_ID,
                severity="info",
                message=f"Услуга для экспайра не найдена ни в одном JSON",
                context={"service_id": sid}
            ))
        
        # === ЭТАП 4: Формирование результата ===
        result.counts["files_processed"] = len(updated)
        result.counts["services_expired"] = expired_count
        result.counts["services_added"] = added_count
        result.counts["skipped_expire_not_found"] = len(skipped_expire_not_found)
        result.counts["skipped_add_existing"] = len(skipped_add_existing)
        
        result.details = {
            "skipped_expire_not_found": skipped_expire_not_found,
            "skipped_add_existing": skipped_add_existing
        }
        
        if not updated:
            result.ok = True
            result.msg = "Нет изменений"
            return result
        
        buf = _build_new_zip(names, blob, updated)
        result.ok = True
        result.msg = "Готово"
        result.zip_data = buf
        
    except Exception as e:
        result.add_issue(Issue(
            type=IssueType.INVALID_JSON,
            severity="error",
            message=f"Критическая ошибка: {str(e)}"
        ))
        result.msg = f"Ошибка: {e}"
    
    return result


def create_replace_offer_from_excel(excel_bytes: bytes, json_name: str, json_id: str) -> SimpleResult:
    """1. Добавление перехода для одного тарифного плана."""
    result = SimpleResult(False, "", None, {})
    
    try:
        df, read_issues = _read_table(excel_bytes, ["offer_id"])
        result.issues.extend(read_issues)
        
        total_rows = len(df)
        df["offer_id"] = df["offer_id"].apply(_normalize_str)
        
        for idx, row in df.iterrows():
            if not row["offer_id"]:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="warning",
                    message="Пустой offer_id",
                    row_number=idx + 2
                ))
        
        df = df[df["offer_id"] != ""]
        
        result.counts["total_rows"] = total_rows
        result.counts["valid_rows"] = len(df)
        
        if df.empty:
            result.msg = "В Excel нет валидных строк"
            return result
        
        offers = [_make_offering(_normalize_id(r["offer_id"])) for _, r in df.iterrows() if _normalize_id(r["offer_id"])]
        pog = _build_pog_replace(_normalize_str(json_name), _normalize_id(json_id), DEFAULT_LOCALE, offers)
        
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(f"{POG_DIR}/{_safe_name(json_id)}.json", _json_dumps_stable(pog))
        buf.seek(0)
        
        result.counts["created_jsons"] = 1
        result.counts["offers_total"] = len(offers)
        result.ok = True
        result.msg = "Готово"
        result.zip_data = buf
        
    except Exception as e:
        result.add_issue(Issue(
            type=IssueType.INVALID_JSON,
            severity="error",
            message=f"Критическая ошибка: {str(e)}"
        ))
        result.msg = f"Ошибка: {e}"
    
    return result


def add_offer_to_transitions(zip_bytes: bytes, excel_bytes: bytes, offer_id: str) -> SimpleResult:
    """2. Добавление нового тарифа в переходы."""
    result = SimpleResult(False, "", None, {})
    
    try:
        names, blob, zip_issues = _read_zip(zip_bytes)
        result.issues.extend(zip_issues)
        
        json_files = _list_json_in_dir(blob, POG_DIR)
        if not json_files:
            result.msg = f"В ZIP нет JSON в {POG_DIR}/"
            return result
        
        result.counts["json_files_in_zip"] = len(json_files)
        
        df, read_issues = _read_table(excel_bytes, ["json_id"])
        result.issues.extend(read_issues)
        
        total_rows = len(df)
        df["json_id"] = df["json_id"].apply(_normalize_str)
        
        for idx, row in df.iterrows():
            if not row["json_id"]:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="warning",
                    message="Пустой json_id",
                    row_number=idx + 2
                ))
        
        target_ids = {x for x in df["json_id"].tolist() if x}
        
        result.counts["total_rows"] = total_rows
        result.counts["valid_rows"] = len(target_ids)
        
        updated: Dict[str, str] = {}
        seen = set()
        want = _normalize_id(offer_id)
        skipped_rows: List[Dict[str, str]] = []
        
        for path in json_files:
            data = _load_json(blob[path], path, result.issues)
            if not data:
                continue
            
            jid = _normalize_id(data.get("id", ""))
            if not jid or jid not in target_ids:
                continue
            
            seen.add(jid)
            
            if data.get("purpose") != ["replaceOffer"]:
                result.add_issue(Issue(
                    type=IssueType.INVALID_TARGET_TYPE,
                    severity="error",
                    message=f"Неверный purpose (ожидается replaceOffer)",
                    file_path=path,
                    context={"json_id": jid}
                ))
                continue
            
            offerings = data.get("productOfferingsInGroup", [])
            existing = {_normalize_id(o.get("id", "")) for o in offerings}
            
            if want in existing:
                result.add_issue(Issue(
                    type=IssueType.ALREADY_EXISTS,
                    severity="info",
                    message=f"Тариф уже существует",
                    file_path=path,
                    context={"json_id": jid, "offer_id": want}
                ))
                skipped_rows.append({
                    "json_id": jid,
                    "offer_id": want,
                    "reason": "already_exists_in_group"
                })
                continue
            
            offerings.append(_make_offering(want))
            data["productOfferingsInGroup"] = sorted(offerings, key=lambda x: x["id"])
            updated[path] = _json_dumps_stable(data)
        
        for want_id in target_ids:
            if want_id not in seen:
                result.add_issue(Issue(
                    type=IssueType.NOT_FOUND_JSON_ID,
                    severity="error",
                    message=f"JSON файл не найден",
                    context={"json_id": want_id}
                ))
        
        result.counts["files_processed"] = len(updated)
        result.counts["added"] = len(updated)
        result.counts["skipped_existing"] = len(skipped_rows)
        result.details = {"skipped_existing": skipped_rows}
        
        if not updated:
            result.ok = True
            result.msg = "Нет изменений"
            return result
        
        buf = _build_new_zip(names, blob, updated)
        result.ok = True
        result.msg = "Готово"
        result.zip_data = buf
        
    except Exception as e:
        result.add_issue(Issue(
            type=IssueType.INVALID_JSON,
            severity="error",
            message=f"Критическая ошибка: {str(e)}"
        ))
        result.msg = f"Ошибка: {e}"
    
    return result


def expire_offer_in_transitions(zip_bytes: bytes, excel_bytes: bytes) -> SimpleResult:
    """3. Экспайр тарифного плана в переходах."""
    result = SimpleResult(False, "", None, {})
    
    try:
        names, blob, zip_issues = _read_zip(zip_bytes)
        result.issues.extend(zip_issues)
        
        json_files = _list_json_in_dir(blob, POG_DIR)
        if not json_files:
            result.msg = f"В ZIP нет JSON в {POG_DIR}/"
            return result
        
        result.counts["json_files_in_zip"] = len(json_files)
        
        df, read_issues = _read_table(excel_bytes, ["json_id", "offer_id"])
        result.issues.extend(read_issues)
        
        total_rows = len(df)
        
        for c in ["json_id", "offer_id"]:
            df[c] = df[c].apply(_normalize_str)
        
        for idx, row in df.iterrows():
            if not row["json_id"]:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="warning",
                    message="Пустой json_id",
                    row_number=idx + 2
                ))
            if not row["offer_id"]:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="warning",
                    message="Пустой offer_id",
                    row_number=idx + 2
                ))
        
        df = df[(df["json_id"] != "") & (df["offer_id"] != "")]
        
        result.counts["total_rows"] = total_rows
        result.counts["valid_rows"] = len(df)
        
        expire_map = df.groupby("json_id")["offer_id"].apply(list).to_dict()
        
        updated: Dict[str, str] = {}
        found_ids = set()
        
        for path in json_files:
            data = _load_json(blob[path], path, result.issues)
            if not data:
                continue
            
            jid = _normalize_id(data.get("id", ""))
            if not jid or jid not in expire_map:
                continue
            
            found_ids.add(jid)
            
            if data.get("purpose") != ["replaceOffer"]:
                result.add_issue(Issue(
                    type=IssueType.INVALID_TARGET_TYPE,
                    severity="error",
                    message=f"Неверный purpose (ожидается replaceOffer)",
                    file_path=path,
                    context={"json_id": jid}
                ))
                continue
            
            offerings = data.get("productOfferingsInGroup", [])
            index_by_id = {_normalize_id(o.get("id", "")): o for o in offerings}
            
            modified = False
            for oid in expire_map[jid]:
                oid = _normalize_id(oid)
                o = index_by_id.get(oid)
                if o is None:
                    result.add_issue(Issue(
                        type=IssueType.NOT_FOUND_OFFER_ID,
                        severity="error",
                        message=f"Тариф не найден",
                        file_path=path,
                        context={"json_id": jid, "offer_id": oid}
                    ))
                    continue
                
                if not o.get("expiredForSales", False):
                    o["expiredForSales"] = True
                    modified = True
                else:
                    result.add_issue(Issue(
                        type=IssueType.ALREADY_EXPIRED,
                        severity="info",
                        message=f"Тариф уже экспайрнут",
                        file_path=path,
                        context={"json_id": jid, "offer_id": oid}
                    ))
            
            if modified:
                data["productOfferingsInGroup"] = sorted(offerings, key=lambda x: x["id"])
                updated[path] = _json_dumps_stable(data)
        
        for want_id in expire_map.keys():
            if want_id not in found_ids:
                result.add_issue(Issue(
                    type=IssueType.NOT_FOUND_JSON_ID,
                    severity="error",
                    message=f"JSON файл не найден",
                    context={"json_id": want_id}
                ))
        
        result.counts["files_processed"] = len(updated)
        result.counts["expired"] = sum(1 for i in result.issues if i.type == IssueType.ALREADY_EXPIRED)
        
        if not updated:
            result.ok = True
            result.msg = "Нет изменений"
            return result
        
        buf = _build_new_zip(names, blob, updated)
        result.ok = True
        result.msg = "Готово"
        result.zip_data = buf
        
    except Exception as e:
        result.add_issue(Issue(
            type=IssueType.INVALID_JSON,
            severity="error",
            message=f"Критическая ошибка: {str(e)}"
        ))
        result.msg = f"Ошибка: {e}"
    
    return result


def generate_categories_from_excel(excel_bytes: bytes) -> SimpleResult:
    """Категории (ProductOfferingCategory)."""
    result = SimpleResult(False, "", None, {})
    
    try:
        df, read_issues = _read_table(excel_bytes, ["offer_id", "category_id"])
        result.issues.extend(read_issues)
        
        total_rows = len(df)
        
        for c in ["offer_id", "category_id"]:
            df[c] = df[c].apply(_normalize_str)
        
        for idx, row in df.iterrows():
            if not row["offer_id"]:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="warning",
                    message="Пустой offer_id",
                    row_number=idx + 2
                ))
            if not row["category_id"]:
                result.add_issue(Issue(
                    type=IssueType.EMPTY_ID,
                    severity="warning",
                    message="Пустой category_id",
                    row_number=idx + 2
                ))
        
        df = df[(df["offer_id"] != "") & (df["category_id"] != "")]
        
        result.counts["total_rows"] = total_rows
        result.counts["valid_rows"] = len(df)
        
        if df.empty:
            result.msg = "В Excel нет валидных строк"
            return result
        
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
        result.counts["created_jsons"] = created
        result.counts["categories_total"] = added
        result.ok = True
        result.msg = "Готово"
        result.zip_data = buf
        
    except Exception as e:
        result.add_issue(Issue(
            type=IssueType.INVALID_JSON,
            severity="error",
            message=f"Критическая ошибка: {str(e)}"
        ))
        result.msg = f"Ошибка: {e}"
    
    return result


# =========================
# UI ФУНКЦИИ
# =========================
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
    rows = (details or {}).get("skipped_existing") or []
    with st.expander(f"Детали пропусков (skipped_existing): {len(rows)}", expanded=False):
        if not rows:
            st.caption("Нет пропусков.")
            return
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


def _show_all_issues(issues: List[Issue]):
    """Отображение всех проблем с группировкой по severity"""
    if not issues:
        st.success("✅ Ошибок и предупреждений нет")
        return
    
    errors = [i for i in issues if i.severity == "error"]
    warnings = [i for i in issues if i.severity == "warning"]
    infos = [i for i in issues if i.severity == "info"]
    
    # Краткая сводка
    col1, col2, col3 = st.columns(3)
    with col1:
        if errors:
            st.metric("🔴 Ошибки", len(errors))
    with col2:
        if warnings:
            st.metric("🟡 Предупреждения", len(warnings))
    with col3:
        if infos:
            st.metric("🔵 Информация", len(infos))
    
    # Детальные списки
    if errors:
        with st.expander(f"🔴 Ошибки ({len(errors)})", expanded=True):
            _show_issues_table(errors)
    
    if warnings:
        with st.expander(f"🟡 Предупреждения ({len(warnings)})", expanded=False):
            _show_issues_table(warnings)
    
    if infos:
        with st.expander(f"🔵 Информация ({len(infos)})", expanded=False):
            _show_issues_table(infos)
    
    # Экспорт всех проблем
    _export_all_issues_csv(issues)


def _show_issues_table(issues: List[Issue]):
    """Таблица проблем"""
    data = []
    for issue in issues:
        row = {
            "Тип": issue.type.value,
            "Сообщение": issue.message,
            "Файл": issue.file_path or "-",
            "Строка": issue.row_number or "-",
        }
        if issue.context:
            for k, v in issue.context.items():
                row[k] = str(v)
        data.append(row)
    
    if data:
        df = pd.DataFrame(data)
        st.dataframe(df, use_container_width=True, height=min(400, len(df) * 35 + 38))


def _export_all_issues_csv(issues: List[Issue]):
    """Экспорт всех проблем в CSV"""
    if not issues:
        return
    
    data = []
    for issue in issues:
        row = {
            "severity": issue.severity,
            "type": issue.type.value,
            "message": issue.message,
            "file_path": issue.file_path or "",
            "row_number": issue.row_number or "",
        }
        if issue.context:
            for k, v in issue.context.items():
                row[f"context_{k}"] = str(v)
        data.append(row)
    
    df = pd.DataFrame(data)
    csv_buf = io.StringIO()
    df.to_csv(csv_buf, index=False)
    
    st.download_button(
        "Скачать полный отчет (CSV)",
        csv_buf.getvalue().encode("utf-8-sig"),
        file_name="full_issues_report.csv",
        mime="text/csv",
    )


# =========================
# STREAMLIT UI
# =========================
st.set_page_config(page_title="Генератор и обновление JSON", layout="wide", initial_sidebar_state="expanded")

st.title("Генератор и обновление JSON")
st.caption("Управление услугами (AddOns), переходами тарифных планов и категориями")

st.sidebar.title("Навигация")
main_section = st.sidebar.radio("Выберите раздел:", ["Услуги (AddOns)", "Переходы тарифных планов", "Категории"])

# --------- Раздел 1: Услуги ----------
if main_section == "Услуги (AddOns)":
    st.header("Работа с услугами")
    scenario = st.radio(
        "Выберите операцию:",
        [
            "1. Доступность услуги для некоторых тарифных планов",
            "2. Добавление услуги в существующие планы",
            "3. Экспайр услуги (скрыть из продажи)",
            "4. Активация услуги (вернуть в продажу)",
            "5. Экспайр + Добавление услуги (смешанный режим)"
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
                    if res.zip_data:
                        st.download_button("Скачать ZIP", res.zip_data, "addons.zip", "application/zip")
                
                # Показываем все проблемы
                if res.issues:
                    st.markdown("---")
                    _show_all_issues(res.issues)

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
                    _show_skipped_details(res.details, filename="skipped_services_existing.csv")
                    if res.zip_data:
                        st.download_button("Скачать ZIP", res.zip_data, "updated_addons.zip", "application/zip")
                
                # Показываем все проблемы
                if res.issues:
                    st.markdown("---")
                    _show_all_issues(res.issues)

    elif scenario.startswith("3."):
        st.subheader("Экспайр услуги")
        st.info("Excel/CSV должен содержать столбцы: json_id, service_id")
        zip_file = st.file_uploader("Загрузите ZIP с планами", type=["zip"], key="zip_expire")
        excel_file = st.file_uploader("Загрузите Excel/CSV со списком к экспайру", type=["xlsx", "xls", "csv"], key="excel_expire")
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
                
                # Показываем все проблемы
                if res.issues:
                    st.markdown("---")
                    _show_all_issues(res.issues)

    elif scenario.startswith("4."):
        st.subheader("Активация услуги")
        st.info("Убирает флаг expiredForSales. Excel/CSV должен содержать столбцы: json_id, service_id")
        zip_file = st.file_uploader("Загрузите ZIP с планами", type=["zip"], key="zip_activate")
        excel_file = st.file_uploader("Загрузите Excel/CSV со списком для активации", type=["xlsx", "xls", "csv"], key="excel_activate")
        if st.button("Выполнить"):
            if not zip_file or not excel_file:
                st.error("Загрузите ZIP и Excel/CSV")
            else:
                with st.spinner("Обработка..."):
                    res = activate_services_in_pogs(zip_file.read(), excel_file.read())
                if not res.ok:
                    st.error(res.msg)
                else:
                    st.success(res.msg)
                    _show_counts(res.counts)
                    if res.zip_data:
                        st.download_button("Скачать ZIP", res.zip_data, "activated_addons.zip", "application/zip")
                
                # Показываем все проблемы
                if res.issues:
                    st.markdown("---")
                    _show_all_issues(res.issues)

    else:  # 5. Экспайр + Добавление услуги
        st.subheader("Экспайр + Добавление услуги")
        st.info("""
        **Две независимые операции:**
        1. Экспайр услуг из файла 1 (где они найдены)
        2. Добавление услуг из файла 2 (во все JSON)
        
        Оба файла должны содержать столбцы: **ID услуги, Имя услуги**
        """)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("##### 📁 Файлы для экспайра")
            zip_file = st.file_uploader("Загрузите ZIP с планами", type=["zip"], key="expire_add_zip")
            expire_file = st.file_uploader(
                "Excel/CSV со списком услуг для экспайра",
                type=["xlsx", "xls", "csv"],
                key="expire_file"
            )
        
        with col2:
            st.markdown("##### 📁 Файлы для добавления")
            st.write("") 
            st.write("")
            add_file = st.file_uploader(
                "Excel/CSV со списком услуг для добавления",
                type=["xlsx", "xls", "csv"],
                key="add_file"
            )
        
        if st.button("Выполнить", type="primary"):
            if not zip_file or not expire_file or not add_file:
                st.error("Загрузите все три файла")
            else:
                with st.spinner("Обработка..."):
                    res = expire_and_add_services(
                        zip_file.read(),
                        expire_file.read(),
                        add_file.read()
                    )
                
                if not res.ok:
                    st.error(res.msg)
                else:
                    st.success(res.msg)
                    _show_counts(res.counts)
                    
                    # Детали пропусков
                    if res.details:
                        col1, col2 = st.columns(2)
                        with col1:
                            expire_skipped = res.details.get("skipped_expire_not_found", [])
                            with st.expander(f"❌ Не найдено для экспайра: {len(expire_skipped)}", expanded=False):
                                if expire_skipped:
                                    df = pd.DataFrame(expire_skipped)
                                    st.dataframe(df, use_container_width=True)
                        
                        with col2:
                            add_skipped = res.details.get("skipped_add_existing", [])
                            with st.expander(f"⚠️ Уже существуют: {len(add_skipped)}", expanded=False):
                                if add_skipped:
                                    df = pd.DataFrame(add_skipped)
                                    st.dataframe(df, use_container_width=True)
                    
                    if res.zip_data:
                        st.download_button(
                            "Скачать обновленный ZIP",
                            res.zip_data,
                            "expire_and_add_services.zip",
                            "application/zip"
                        )
                
                # Показываем все проблемы
                if res.issues:
                    st.markdown("---")
                    _show_all_issues(res.issues)

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
                    if res.zip_data:
                        st.download_button("Скачать ZIP", res.zip_data, "replace_offer.zip", "application/zip")
                
                # Показываем все проблемы
                if res.issues:
                    st.markdown("---")
                    _show_all_issues(res.issues)

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
                    _show_skipped_details(res.details, filename="skipped_offers_existing.csv")
                    if res.zip_data:
                        st.download_button("Скачать ZIP", res.zip_data, "updated_replace_offers.zip", "application/zip")
                
                # Показываем все проблемы
                if res.issues:
                    st.markdown("---")
                    _show_all_issues(res.issues)

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
                
                # Показываем все проблемы
                if res.issues:
                    st.markdown("---")
                    _show_all_issues(res.issues)

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
                if res.zip_data:
                    st.download_button("Скачать ZIP", res.zip_data, "categories.zip", "application/zip")
            
            # Показываем все проблемы
            if res.issues:
                st.markdown("---")
                _show_all_issues(res.issues)
