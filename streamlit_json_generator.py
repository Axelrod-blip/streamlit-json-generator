import io
import json
import re
from typing import List, Dict, Any

import pandas as pd
import streamlit as st
import zipfile

# Прототип Streamlit для генерации JSON строго по требуемой структуре.
# Запуск: streamlit run streamlit_json_generator.py
# Зависимости: pip install streamlit pandas openpyxl

st.set_page_config(page_title="JSON Generator for AddOns", layout="centered")

# ---- Белый фон ----

st.title("Генератор JSON из Excel — AddOns")

# ---- Ввод пользователя ----
with st.form("main_form"):
    user_name = st.text_input("Название Add-On", placeholder="Имя Add-Ons")
    user_id = st.text_input("ID Add-On", placeholder="0ff0d3db-c9b9-4e78-9d93-0b7d88a85751")
    locale = st.text_input("Language", value="en-US")
    uploaded_file = st.file_uploader("Загрузите Excel с двумя колонками (ID Add-on и Name Add-on)",
                                     type=["xls", "xlsx"])
    submitted = st.form_submit_button("Сгенерировать JSON")


# Утилиты
def safe_name(n: str) -> str:
    """Генерируем поле name и имена файлов: замена пробелов на подчёркивания, удаление лишних символов."""
    if not isinstance(n, str):
        return str(n)
    s = n.strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^0-9A-Za-z_\-\u0400-\u04FF]", "", s)
    return s


def build_product_offering_row(row_id: str, row_name: str, locale: str) -> Dict[str, Any]:
    return {
        "expiredForSales": False,
        "id": str(row_id),
        "isBundle": False,
        "name": [
            {
                "locale": locale,
                "value": str(row_name)
            }
        ]
    }


def build_final_json(user_name: str, user_id: str, locale: str,
                     offerings: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "effective": True,
        "externalId": [],
        "localizedName": [
            {
                "locale": locale,
                "value": user_name
            }
        ],
        "name": safe_name(user_name),
        "policy": [],
        "productOfferingsInGroup": offerings,
        "purpose": ["addOn"],
        "restriction": [],
        "id": str(user_id)
    }


# ---- Обработка ----
if submitted:
    errors = []
    if not user_name:
        errors.append("Введите название (localizedName.value).")
    if not user_id:
        errors.append("Введите основной ID.")
    if not uploaded_file:
        errors.append("Загрузите Excel-файл.")

    if errors:
        for e in errors:
            st.error(e)
        st.stop()

    try:
        df = pd.read_excel(uploaded_file, engine="openpyxl")
    except Exception as e:
        st.exception(f"Ошибка чтения Excel: {e}")
        st.stop()

    st.success("Файл загружен — первые строки:")
    st.dataframe(df.head(10))

    cols = list(df.columns)

    def guess_column(possible_names: List[str]) -> str:
        for p in possible_names:
            for c in cols:
                if p.lower() in c.lower():
                    return c
        return None

    default_id_col = guess_column(["id add", "add-id"]) or (cols[0] if cols else None)
    default_name_col = guess_column(["name add", "name add-on"]) or (cols[1] if len(cols) > 1 else None)

    id_col = st.selectbox("Колонка с ID (ID ADD-id)", options=cols,
                          index=cols.index(default_id_col) if default_id_col in cols else 0)
    name_col = st.selectbox("Колонка с названием (Name Add-on)", options=cols,
                            index=cols.index(default_name_col) if default_name_col in cols else (1 if len(cols) > 1 else 0))

    offerings = []
    for _, r in df.iterrows():
        rid = r.get(id_col)
        rname = r.get(name_col)
        if pd.isna(rid) and pd.isna(rname):
            continue
        if pd.isna(rid):
            st.warning(f"Строка пропущена (пустой ID): {r.to_dict()}")
            continue
        offerings.append(build_product_offering_row(rid, rname, locale))

    final_json = build_final_json(user_name=user_name, user_id=user_id, locale=locale, offerings=offerings)

    st.subheader("Результат JSON")
    pretty = json.dumps(final_json, ensure_ascii=False, indent=4)
    with st.expander("Показать / скрыть JSON"):
        st.code(pretty, language="json")

    # ---- Генерация ZIP ----
    json_filename = f"{safe_name(user_id)}.json"   # имя JSON = ID
    zip_filename = f"{safe_name(user_name)}.zip"   # имя архива = Название Add-On
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        # кладём файл в папку productOfferingGroup
        zip_file.writestr(f"productOfferingGroup/{json_filename}", pretty)

    zip_buffer.seek(0)

    st.download_button(
        label="Скачать ZIP с JSON",
        data=zip_buffer,
        file_name=zip_filename,
        mime="application/zip"
    )
