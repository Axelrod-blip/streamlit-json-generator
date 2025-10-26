import io
import re
import json
import zipfile
from typing import Dict, Any, List, Tuple
from dataclasses import dataclass

import pandas as pd
import streamlit as st

# ------------------------
# CONFIG & CONSTANTS
# ------------------------
st.set_page_config(page_title="JSON Generator & Updater", layout="centered")

@dataclass
class ExcelColumns:
    """Define expected Excel column structures"""
    SINGLE_PLAN = ["service_id", "service_name"]
    MULTI_PLAN = ["json_name", "json_id", "service_id", "service_name"]
    SWAP_OFFER = ["tariff_id"]
    CATEGORY = ["offer_id", "category_id"]

# ------------------------
# UTILITY FUNCTIONS
# ------------------------
def safe_name(n: str) -> str:
    """Sanitize string for filename usage"""
    if not isinstance(n, str):
        return str(n)
    s = re.sub(r"\s+", "_", str(n).strip())
    return re.sub(r"[^0-9A-Za-z_\-\u0400-\u04FF]", "", s)

def validate_excel_columns(df: pd.DataFrame, expected_count: int, mode: str) -> Tuple[bool, str]:
    """Validate Excel file structure"""
    if df.empty:
        return False, "Excel файл пустой"
    
    if len(df.columns) < expected_count:
        return False, f"Ожидается минимум {expected_count} колонок для режима '{mode}'"
    
    # Check for data in first rows
    if df.iloc[:, :expected_count].isnull().all().any():
        return False, "Обнаружены пустые обязательные колонки"
    
    return True, ""

def remove_duplicates(df: pd.DataFrame, subset_cols: List[int]) -> Tuple[pd.DataFrame, int]:
    """Remove duplicate rows based on specified columns"""
    initial_count = len(df)
    # Используем индексы колонок для удаления дубликатов
    cols_to_check = [df.columns[i] for i in subset_cols if i < len(df.columns)]
    df_cleaned = df.drop_duplicates(subset=cols_to_check, keep='first')
    duplicates_count = initial_count - len(df_cleaned)
    return df_cleaned, duplicates_count

def create_offering(service_id: str, service_name: str = None, locale: str = "en-US", 
                   expired: bool = False) -> Dict[str, Any]:
    """Create a service offering object"""
    offering = {
        "expiredForSales": expired,
        "id": str(service_id),
        "isBundle": False,
    }
    if service_name:
        offering["name"] = [{"locale": locale, "value": str(service_name)}]
    return offering

def create_category_json(offer_id: str, category_id: str) -> Dict[str, Any]:
    """Create category JSON structure"""
    return {
        "id": str(offer_id),
        "category": [str(category_id)],
        "categoryRef": [
            {
                "id": str(category_id)
            }
        ]
    }

def build_json(name: str, uid: str, locale: str, offerings: List[Dict[str, Any]], 
               purpose: str = "addOn") -> Dict[str, Any]:
    """Build complete JSON structure"""
    json_obj = {
        "effective": True,
        "externalId": [],
        "localizedName": [{"locale": locale, "value": name}],
        "name": safe_name(name),
        "policy": [],
        "productOfferingsInGroup": offerings,
        "restriction": [],
        "id": str(uid),
    }
    
    if purpose == "addOn":
        json_obj["purpose"] = ["addOn"]
    elif purpose == "replaceOffer":
        json_obj["purpose"] = ["replaceOffer"]
        json_obj["description"] = [{"locale": locale, "value": name}]
    
    return json_obj

def create_zip_buffer(json_obj: Dict[str, Any], file_id: str, folder: str = "productOfferingGroup") -> io.BytesIO:
    """Create ZIP buffer with JSON file"""
    zip_buffer = io.BytesIO()
    pretty_json = json.dumps(json_obj, ensure_ascii=False, indent=4)
    
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{folder}/{safe_name(file_id)}.json", pretty_json)
    
    zip_buffer.seek(0)
    return zip_buffer

# ------------------------
# MODULE 1: UPDATE SERVICE
# ------------------------
def update_zip_with_service(zip_file: zipfile.ZipFile, new_service: Dict[str, Any]) -> Tuple[Dict, Dict]:
    """Update all JSON files in ZIP with new service"""
    file_list = zip_file.namelist()
    json_files = [f for f in file_list if f.lower().endswith(".json") 
                  and "productofferinggroup/" in f.lower()]

    if not json_files:
        raise ValueError("JSON файлы не найдены в папке productOfferingGroup/")

    original_structure = {name: zip_file.open(name).read() for name in file_list}
    updated_jsons = {}

    for json_filename in json_files:
        try:
            json_data = json.loads(original_structure[json_filename].decode("utf-8"))
            
            if "productOfferingsInGroup" not in json_data:
                st.warning(f"Пропущен: нет productOfferingsInGroup → {json_filename}")
                continue

            existing_ids = {item.get("id") for item in json_data["productOfferingsInGroup"]}
            
            if new_service["id"] in existing_ids:
                st.info(f"Услуга уже существует в {json_filename}")
                continue

            json_data["productOfferingsInGroup"].append(new_service)
            updated_jsons[json_filename] = json.dumps(json_data, ensure_ascii=False, indent=4)
            
        except json.JSONDecodeError as e:
            st.warning(f"Невалидный JSON ({json_filename}): {e}")
            continue
        except Exception as e:
            st.error(f"Ошибка обработки {json_filename}: {e}")
            continue

    return updated_jsons, original_structure

# ------------------------
# UI: NAVIGATION
# ------------------------
st.sidebar.title("Навигация")
page = st.sidebar.radio(
    "Выберите действие:",
    [
        "Добавить услугу в существующие тарифные планы",
        "Сгенерировать новые JSON",
    ],
)

# =======================================================
# MODULE 1 UI: ADD SERVICE TO EXISTING ZIP
# =======================================================
if page == "Добавить услугу в существующие тарифные планы":
    st.title("Добавление новой услуги в AddOn JSON файлы")
    
    with st.form("update_form"):
        uploaded_zip = st.file_uploader("Загрузите ZIP архив", type=["zip"])
        
        col1, col2 = st.columns(2)
        with col1:
            service_id = st.text_input("ID услуги", 
                                      placeholder="ee1374db-4a25-4ae7-b78a-aa493a288f9f")
        with col2:
            expired_for_sales = st.selectbox("expiredForSales", [False, True], 
                                            format_func=lambda x: "false" if not x else "true")
        
        service_name = st.text_input("Название услуги", 
                                    placeholder="4G Bonus 5GB BEEPUL")
        
        submitted = st.form_submit_button("Добавить услугу", type="primary")

    if submitted:
        # Validation
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
            with st.spinner("Обработка ZIP архива..."):
                zip_buffer = io.BytesIO(uploaded_zip.read())
                
                with zipfile.ZipFile(zip_buffer, "r") as zip_file:
                    new_service = create_offering(service_id.strip(), service_name.strip(), 
                                                 expired=expired_for_sales)
                    updated_jsons, original_structure = update_zip_with_service(zip_file, new_service)

            if not updated_jsons:
                st.warning("Не найдено JSON для обновления или все уже содержат данную услугу.")
                st.stop()

            st.success(f"Услуга добавлена в {len(updated_jsons)} JSON файлов")

            # Preview
            first_file, first_json = next(iter(updated_jsons.items()))
            with st.expander(f"Пример обновлённого JSON ({first_file})"):
                st.code(first_json, language="json")

            # Create new ZIP
            new_zip_buffer = io.BytesIO()
            with zipfile.ZipFile(new_zip_buffer, "w", zipfile.ZIP_DEFLATED) as new_zip:
                for name, data in original_structure.items():
                    if name in updated_jsons:
                        data = updated_jsons[name].encode("utf-8")
                    new_zip.writestr(name, data)

            new_zip_buffer.seek(0)
            new_zip_filename = uploaded_zip.name.replace(".zip", "_updated.zip")

            st.download_button(
                "Скачать обновлённый ZIP",
                new_zip_buffer,
                new_zip_filename,
                "application/zip",
                type="primary"
            )

        except zipfile.BadZipFile:
            st.error("Загруженный файл не является корректным ZIP архивом")
        except Exception as e:
            st.error(f"Ошибка: {e}")
            with st.expander("Детали ошибки"):
                st.exception(e)

# =======================================================
# MODULE 2 UI: GENERATE NEW JSON FILES
# =======================================================
else:
    st.title("Генератор JSON ZIP файлов")
    
    subpage = st.radio(
        "Режим генерации:",
        [
            "Доступность услуг для одного тарифного плана",
            "Доступность услуг для нескольких тарифных планов",
            "Swap Offer (переходы тарифных планов)",
            "Изменить категории ProductOfferingCategory",
        ],
    )

    # ===== SINGLE PLAN MODE =====
    if subpage == "Доступность услуг для одного тарифного плана":
        st.subheader("Один тарифный план")
        
        col1, col2 = st.columns(2)
        with col1:
            name = st.text_input("Название услуги")
            uid = st.text_input("ID услуги")
        with col2:
            locale = st.text_input("Language", value="en-US")
        
        file = st.file_uploader("Excel файл (2 колонки: ID услуги, Название)", type=["xls", "xlsx"])
        
        st.info("Excel должен содержать 2 колонки: ID услуги | Название услуги")
        
        if st.button("Сгенерировать", type="primary") and file:
            try:
                with st.spinner("Чтение Excel файла..."):
                    df = pd.read_excel(file, engine="openpyxl")
                
                is_valid, error_msg = validate_excel_columns(df, 2, "single plan")
                if not is_valid:
                    st.error(error_msg)
                    st.stop()
                
                # Удаление дубликатов
                df_cleaned, duplicates_count = remove_duplicates(df, [0])
                if duplicates_count > 0:
                    st.info(f"Удалено дубликатов: {duplicates_count}")
                
                id_col, name_col = df_cleaned.columns[0], df_cleaned.columns[1]
                offerings = [
                    create_offering(r[id_col], r[name_col], locale) 
                    for _, r in df_cleaned.iterrows() 
                    if pd.notna(r[id_col])
                ]
                
                if not offerings:
                    st.warning("Не найдено валидных услуг в Excel файле")
                    st.stop()
                
                final = build_json(name, uid, locale, offerings, purpose="addOn")
                
                st.success(f"Сгенерировано {len(offerings)} услуг")
                with st.expander("Просмотр JSON"):
                    st.json(final)
                
                zip_buffer = create_zip_buffer(final, uid)
                st.download_button(
                    "Скачать ZIP",
                    zip_buffer,
                    f"{safe_name(name)}.zip",
                    "application/zip",
                    type="primary"
                )
                
            except Exception as e:
                st.error(f"Ошибка: {e}")
                with st.expander("Детали ошибки"):
                    st.exception(e)

    # ===== MULTIPLE PLANS MODE =====
    elif subpage == "Доступность услуг для нескольких тарифных планов":
        st.subheader("Несколько тарифных планов")
        
        locale = st.text_input("Language", value="en-US")
        file = st.file_uploader("Excel файл (4 колонки)", type=["xls", "xlsx"])
        
        st.info("Excel должен содержать 4 колонки: Имя JSON | ID JSON | ID услуги | Название услуги")
        
        if st.button("Сгенерировать ZIP", type="primary") and file:
            try:
                with st.spinner("Обработка Excel файла..."):
                    df = pd.read_excel(file, engine="openpyxl")
                
                is_valid, error_msg = validate_excel_columns(df, 4, "multi plan")
                if not is_valid:
                    st.error(error_msg)
                    st.stop()
                
                # Удаление дубликатов по всем 4 колонкам
                df_cleaned, duplicates_count = remove_duplicates(df, [0, 1, 2, 3])
                if duplicates_count > 0:
                    st.info(f"Удалено дубликатов: {duplicates_count}")
                
                grouped = df_cleaned.groupby([df_cleaned.columns[0], df_cleaned.columns[1]])
                
                zip_buffer = io.BytesIO()
                json_count = 0
                
                with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                    for (json_name, json_id), group in grouped:
                        offerings = [
                            create_offering(r[df_cleaned.columns[2]], r[df_cleaned.columns[3]], locale)
                            for _, r in group.iterrows()
                            if pd.notna(r[df_cleaned.columns[2]])
                        ]
                        
                        if not offerings:
                            continue
                        
                        final = build_json(json_name, json_id, locale, offerings, purpose="addOn")
                        pretty_json = json.dumps(final, ensure_ascii=False, indent=4)
                        zf.writestr(f"productOfferingGroup/{safe_name(json_id)}.json", pretty_json)
                        json_count += 1
                
                zip_buffer.seek(0)
                st.success(f"Сгенерировано {json_count} JSON файлов")
                
                st.download_button(
                    "Скачать ZIP",
                    zip_buffer,
                    "services_jsons.zip",
                    "application/zip",
                    type="primary"
                )
                
            except Exception as e:
                st.error(f"Ошибка: {e}")
                with st.expander("Детали ошибки"):
                    st.exception(e)

    # ===== SWAP OFFER MODE =====
    elif subpage == "Swap Offer (переходы тарифных планов)":
        st.subheader("Переходы тарифных планов")
        
        col1, col2 = st.columns(2)
        with col1:
            name = st.text_input("Название swap offer")
            uid = st.text_input("ID swap offer")
        with col2:
            locale = st.text_input("Language", value="en-US")
        
        file = st.file_uploader("Excel файл (1 колонка: ID тарифов)", type=["xls", "xlsx"])
        
        st.info("Excel должен содержать 1 колонку: ID тарифных планов")
        
        if st.button("Сгенерировать", type="primary") and file:
            try:
                with st.spinner("Чтение Excel файла..."):
                    df = pd.read_excel(file, engine="openpyxl")
                
                is_valid, error_msg = validate_excel_columns(df, 1, "swap offer")
                if not is_valid:
                    st.error(error_msg)
                    st.stop()
                
                # Удаление дубликатов
                df_cleaned, duplicates_count = remove_duplicates(df, [0])
                if duplicates_count > 0:
                    st.info(f"Удалено дубликатов: {duplicates_count}")
                
                id_col = df_cleaned.columns[0]
                offerings = [
                    create_offering(str(r[id_col]).strip())
                    for _, r in df_cleaned.iterrows()
                    if pd.notna(r[id_col])
                ]
                
                if not offerings:
                    st.warning("Не найдено валидных тарифов в Excel файле")
                    st.stop()
                
                final = build_json(name, uid, locale, offerings, purpose="replaceOffer")
                
                st.success(f"Сгенерировано {len(offerings)} тарифных планов")
                with st.expander("Просмотр JSON"):
                    st.json(final)
                
                zip_buffer = create_zip_buffer(final, uid)
                st.download_button(
                    "Скачать ZIP",
                    zip_buffer,
                    f"{safe_name(name)}.zip",
                    "application/zip",
                    type="primary"
                )
                
            except Exception as e:
                st.error(f"Ошибка: {e}")
                with st.expander("Детали ошибки"):
                    st.exception(e)

    # ===== CATEGORY MODE =====
    elif subpage == "Изменить категории ProductOfferingCategory":
        st.subheader("Изменить категории ProductOfferingCategory")
        
        file = st.file_uploader("Excel файл (2 колонки: Offer_id, Category_id)", type=["xls", "xlsx"])
        
        st.info("Excel должен содержать 2 колонки: Offer_id | Category_id")
        
        with st.expander("Пример структуры JSON"):
            st.code('''{
    "id": "0a9e12ee-4cbf-47aa-a492-82596254721c",
    "category": [
        "39d54e58-67e0-4a0d-89ae-80a6b91ffe17"
    ],
    "categoryRef": [
        {
            "id": "39d54e58-67e0-4a0d-89ae-80a6b91ffe17"
        }
    ]
}''', language="json")
        
        if st.button("Сгенерировать ZIP", type="primary") and file:
            try:
                with st.spinner("Обработка Excel файла..."):
                    df = pd.read_excel(file, engine="openpyxl")
                
                is_valid, error_msg = validate_excel_columns(df, 2, "category")
                if not is_valid:
                    st.error(error_msg)
                    st.stop()
                
                # Удаление дубликатов по обеим колонкам
                df_cleaned, duplicates_count = remove_duplicates(df, [0, 1])
                if duplicates_count > 0:
                    st.info(f"Удалено дубликатов: {duplicates_count}")
                
                offer_col, category_col = df_cleaned.columns[0], df_cleaned.columns[1]
                
                # Группируем по Offer_id, так как может быть несколько категорий для одного offer
                grouped = df_cleaned.groupby(offer_col)
                
                zip_buffer = io.BytesIO()
                json_count = 0
                
                with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                    for offer_id, group in grouped:
                        if pd.isna(offer_id):
                            continue
                        
                        # Собираем все категории для данного offer
                        categories = [
                            str(r[category_col]).strip()
                            for _, r in group.iterrows()
                            if pd.notna(r[category_col])
                        ]
                        
                        if not categories:
                            continue
                        
                        # Создаем JSON с несколькими категориями
                        category_json = {
                            "id": str(offer_id).strip(),
                            "category": categories,
                            "categoryRef": [{"id": cat_id} for cat_id in categories]
                        }
                        
                        pretty_json = json.dumps(category_json, ensure_ascii=False, indent=4)
                        zf.writestr(f"productOfferingCategory/{safe_name(offer_id)}.json", pretty_json)
                        json_count += 1
                
                zip_buffer.seek(0)
                st.success(f"Сгенерировано {json_count} JSON файлов категорий")
                
                # Показываем пример первого файла
                if json_count > 0:
                    df_preview = df_cleaned.head(3)
                    with st.expander("Предпросмотр данных"):
                        st.dataframe(df_preview)
                
                st.download_button(
                    "Скачать ZIP",
                    zip_buffer,
                    "product_offering_categories.zip",
                    "application/zip",
                    type="primary"
                )
                
            except Exception as e:
                st.error(f"Ошибка: {e}")
                with st.expander("Детали ошибки"):
                    st.exception(e)