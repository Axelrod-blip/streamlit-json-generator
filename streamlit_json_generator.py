import io
import json
import re
import zipfile
import pandas as pd
import streamlit as st
from typing import List, Dict, Any

st.set_page_config(page_title="JSON Generator", layout="centered")

# --- Утилиты ---
def safe_name(n: str) -> str:
    if not isinstance(n, str): return str(n)
    s = re.sub(r"\s+", "_", str(n).strip())
    return re.sub(r"[^0-9A-Za-z_\-\u0400-\u04FF]", "", s)

def build_pog_offering_row(row_id: str) -> Dict[str, Any]:
    return {"expiredForSales": False, "id": row_id, "isBundle": False}

def build_addon_offering_row(row_id: str, row_name: str, locale: str) -> Dict[str, Any]:
    return {
        "expiredForSales": False,
        "id": str(row_id),
        "isBundle": False,
        "name": [{"locale": locale, "value": str(row_name)}],
    }

def build_json(user_name: str, user_id: str, locale: str,
               offerings: List[Dict[str, Any]], is_addon: bool) -> Dict[str, Any]:
    base = {
        "effective": True,
        "externalId": [],
        "localizedName": [{"locale": locale, "value": user_name}],
        "name": safe_name(user_name),
        "policy": [],
        "productOfferingsInGroup": offerings,
        "restriction": [],
        "id": str(user_id),
    }
    if is_addon:
        base["purpose"] = ["addOn"]  # услуги для тарифных планов
    else:  # переходы тарифных планов
        base["description"] = [{"locale": locale, "value": user_name}]
    return base

def download_zip(json_obj: Dict[str, Any], user_id: str, user_name: str):
    pretty = json.dumps(json_obj, ensure_ascii=False, indent=4)
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"productOfferingGroup/{safe_name(user_id)}.json", pretty)
    zip_buffer.seek(0)
    st.download_button("Скачать ZIP", zip_buffer, f"{safe_name(user_name)}.zip", "application/zip")


# --- Навигация ---
page = st.sidebar.radio(
    "Навигация",
    [
        "Доступность услуг для одного тарифного плана",
        "Доступность услуг для нескольких тарифных планов",
        "Переходы тарифных планов",
    ],
)


# --- Одна услуга ---
if page == "Доступность услуг для одного тарифного плана":
    st.title("Доступность услуг для одного тарифного плана")
    name = st.text_input("Название услуги")
    uid = st.text_input("ID услуги")
    locale = st.text_input("Language", value="en-US")
    file = st.file_uploader("Excel: ID и Name услуги", type=["xls", "xlsx"])
    if st.button("Сгенерировать") and file:
        df = pd.read_excel(file, engine="openpyxl")
        id_col, name_col = df.columns[0], df.columns[1]
        offerings = [build_addon_offering_row(r[id_col], r[name_col], locale) for _, r in df.iterrows() if pd.notna(r[id_col])]
        final = build_json(name, uid, locale, offerings, is_addon=True)
        st.json(final)
        download_zip(final, uid, name)


# --- Несколько услуг ---
elif page == "Доступность услуг для нескольких тарифных планов":
    st.title("Доступность услуг для нескольких тарифных планов")
    locale = st.text_input("Language", value="en-US")
    file = st.file_uploader("Excel: Имя JSON, ID JSON, ID услуги, Name услуги", type=["xls", "xlsx"])
    if st.button("Сгенерировать ZIP") and file:
        df = pd.read_excel(file, engine="openpyxl")
        grouped = df.groupby([df.columns[0], df.columns[1]])
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for (json_name, json_id), group in grouped:
                offerings = [build_addon_offering_row(r[df.columns[2]], r[df.columns[3]], locale) for _, r in group.iterrows() if pd.notna(r[df.columns[2]])]
                final = build_json(json_name, json_id, locale, offerings, is_addon=True)
                pretty = json.dumps(final, ensure_ascii=False, indent=4)
                zf.writestr(f"productOfferingGroup/{safe_name(json_id)}.json", pretty)
        zip_buffer.seek(0)
        st.download_button("Скачать ZIP", zip_buffer, "services_jsons.zip", "application/zip")


# --- Переходы ---
elif page == "Переходы тарифных планов":
    st.title("Переходы тарифных планов")
    name = st.text_input("Название перехода")
    uid = st.text_input("ID перехода")
    locale = st.text_input("Language", value="en-US")
    file = st.file_uploader("Excel: колонка ID тарифов", type=["xls", "xlsx"])
    if st.button("Сгенерировать") and file:
        df = pd.read_excel(file, engine="openpyxl")
        id_col = df.columns[0]
        offerings = [build_pog_offering_row(str(r[id_col]).strip()) for _, r in df.iterrows() if pd.notna(r[id_col])]
        final = build_json(name, uid, locale, offerings, is_addon=False)
        st.json(final)
        download_zip(final, uid, name)
