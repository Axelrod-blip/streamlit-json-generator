import io
import re
import json
import zipfile
from typing import Dict, Any, List

import pandas as pd
import streamlit as st

# ------------------------
# CONFIG
# ------------------------
st.set_page_config(page_title="JSON Generator & Updater", layout="centered")
st.sidebar.title("Навигация")

page = st.sidebar.radio(
    "Выберите действие:",
    [
        "Добавить услугу в существующий ZIP",
        "Сгенерировать новые JSON ZIP файлы",
    ],
)

# =======================================================
# ============ МОДУЛЬ 1: Добавление услуги ===============
# =======================================================
def create_new_service(service_id: str, service_name: str, expired_for_sales: bool) -> Dict[str, Any]:
    return {
        "expiredForSales": expired_for_sales,
        "id": service_id,
        "isBundle": False,
        "name": [{"locale": "en-US", "value": service_name}],
    }


def update_all_jsons_in_zip(zip_file, new_service: Dict[str, Any]):
    file_list = zip_file.namelist()
    json_files = [f for f in file_list if f.lower().endswith(".json") and "productofferinggroup/" in f.lower()]

    if not json_files:
        raise ValueError("JSON файлы не найдены в папке productOfferingGroup/")

    original_structure = {}
    updated_jsons = {}

    for name in file_list:
        with zip_file.open(name) as f:
            original_structure[name] = f.read()

    for json_filename in json_files:
        try:
            json_data = json.loads(original_structure[json_filename].decode("utf-8"))
        except json.JSONDecodeError:
            st.warning(f"Пропущен невалидный JSON: {json_filename}")
            continue

        if "productOfferingsInGroup" not in json_data:
            st.warning(f"Пропущен: нет productOfferingsInGroup → {json_filename}")
            continue

        existing_ids = [item.get("id") for item in json_data["productOfferingsInGroup"]]
        if new_service["id"] in existing_ids:
            st.info(f"Услуга '{new_service['id']}' уже есть в {json_filename}")
            continue

        json_data["productOfferingsInGroup"].append(new_service)
        updated_jsons[json_filename] = json.dumps(json_data, ensure_ascii=False, indent=4)

    return updated_jsons, original_structure


if page == "Добавить услугу в существующий ZIP":
    st.title("Добавление новой услуги в AddOn JSON файлы")

    with st.form("update_form"):
        uploaded_zip = st.file_uploader("Загрузите ZIP архив", type=["zip"])
        service_id = st.text_input("ID услуги", placeholder="ee1374db-4a25-4ae7-b78a-aa493a288f9f")
        service_name = st.text_input("Название услуги", placeholder="4G Bonus 5GB BEEPUL")
        expired_for_sales = st.selectbox("expiredForSales", [False, True], format_func=lambda x: "false" if not x else "true")
        submitted = st.form_submit_button("Добавить услугу")

    if submitted:
        errors = []
        if not uploaded_zip:
            errors.append("Загрузите ZIP архив.")
        if not service_id.strip():
            errors.append("Введите ID услуги.")
        if not service_name.strip():
            errors.append("Введите название услуги.")

        if errors:
            for e in errors:
                st.error(e)
            st.stop()

        try:
            zip_buffer = io.BytesIO(uploaded_zip.read())
            with zipfile.ZipFile(zip_buffer, "r") as zip_file:
                new_service = create_new_service(service_id.strip(), service_name.strip(), expired_for_sales)
                updated_jsons, original_structure = update_all_jsons_in_zip(zip_file, new_service)

            if not updated_jsons:
                st.warning("Не найдено JSON для обновления или все уже содержат данную услугу.")
                st.stop()

            st.success(f"Услуга добавлена в {len(updated_jsons)} JSON файлов!")

            first_file, first_json = next(iter(updated_jsons.items()))
            with st.expander(f"Пример обновлённого JSON ({first_file})"):
                st.code(first_json, language="json")

            new_zip_buffer = io.BytesIO()
            with zipfile.ZipFile(new_zip_buffer, "w", zipfile.ZIP_DEFLATED) as new_zip:
                for name, data in original_structure.items():
                    if name in updated_jsons:
                        data = updated_jsons[name].encode("utf-8")
                    new_zip.writestr(name, data)

            new_zip_buffer.seek(0)
            new_zip_filename = uploaded_zip.name.replace(".zip", "_updated.zip")

            st.download_button("Скачать обновлённый ZIP", new_zip_buffer, new_zip_filename, "application/zip")

        except Exception as e:
            st.error(f"Ошибка: {e}")
            st.exception(e)

# =======================================================
# ============ МОДУЛЬ 2: Генератор JSON ZIP =============
# =======================================================
else:
    def safe_name(n: str) -> str:
        if not isinstance(n, str):
            return str(n)
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

    def build_json(user_name: str, user_id: str, locale: str, offerings: List[Dict[str, Any]], mode: str) -> Dict[str, Any]:
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
        if mode == "addon":
            base["purpose"] = ["addOn"]
        elif mode == "replace":
            base["purpose"] = ["replaceOffer"]
            base["description"] = [{"locale": locale, "value": user_name}]
        return base

    def download_zip(json_obj: Dict[str, Any], user_id: str, user_name: str):
        pretty = json.dumps(json_obj, ensure_ascii=False, indent=4)
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(f"productOfferingGroup/{safe_name(user_id)}.json", pretty)
        zip_buffer.seek(0)
        st.download_button("Скачать ZIP", zip_buffer, f"{safe_name(user_name)}.zip", "application/zip")

    subpage = st.radio(
        "Режим генерации:",
        [
            "Доступность услуг для одного тарифного плана",
            "Доступность услуг для нескольких тарифных планов",
            "Swap Offer (переходы тарифных планов)",
        ],
    )

    if subpage == "Доступность услуг для одного тарифного плана":
        st.title("Доступность услуг для одного тарифного плана")
        name = st.text_input("Название услуги")
        uid = st.text_input("ID услуги")
        locale = st.text_input("Language", value="en-US")
        file = st.file_uploader("Excel: ID и Name услуги", type=["xls", "xlsx"])
        if st.button("Сгенерировать") and file:
            df = pd.read_excel(file, engine="openpyxl")
            id_col, name_col = df.columns[0], df.columns[1]
            offerings = [build_addon_offering_row(r[id_col], r[name_col], locale) for _, r in df.iterrows() if pd.notna(r[id_col])]
            final = build_json(name, uid, locale, offerings, mode="addon")
            st.json(final)
            download_zip(final, uid, name)

    elif subpage == "Доступность услуг для нескольких тарифных планов":
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
                    final = build_json(json_name, json_id, locale, offerings, mode="addon")
                    pretty = json.dumps(final, ensure_ascii=False, indent=4)
                    zf.writestr(f"productOfferingGroup/{safe_name(json_id)}.json", pretty)
            zip_buffer.seek(0)
            st.download_button("Скачать ZIP", zip_buffer, "services_jsons.zip", "application/zip")

    elif subpage == "Swap Offer (переходы тарифных планов)":
        st.title("Swap Offer (переходы тарифных планов)")
        name = st.text_input("Название swap offer")
        uid = st.text_input("ID swap offer")
        locale = st.text_input("Language", value="en-US")
        file = st.file_uploader("Excel: колонка ID тарифов", type=["xls", "xlsx"])
        if st.button("Сгенерировать") and file:
            df = pd.read_excel(file, engine="openpyxl")
            id_col = df.columns[0]
            offerings = [build_pog_offering_row(str(r[id_col]).strip()) for _, r in df.iterrows() if pd.notna(r[id_col])]
            final = build_json(name, uid, locale, offerings, mode="replace")
            st.json(final)
            download_zip(final, uid, name)
