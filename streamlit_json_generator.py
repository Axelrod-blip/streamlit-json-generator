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
        n = str(n)
    s = re.sub(r"\s+", "_", n.strip())
    s = re.sub(r"[^0-9A-Za-z_\-\u0400-\u04FF]", "", s)
    return s or "file"

def validate_excel_columns(df: pd.DataFrame, expected_count: int, mode: str) -> Tuple[bool, str]:
    """Validate Excel file structure"""
    if df.empty:
        return False, "Excel файл пустой"
    
    if len(df.columns) < expected_count:
        return False, f"Ожидается минимум {expected_count} колонок для режима '{mode}'"
    
    # Проверяем, что обязательные первые N колонок не полностью пустые
    if df.iloc[:, :expected_count].isnull().all().any():
        return False, "Обнаружены пустые обязательные колонки"
    
    return True, ""

def remove_duplicates(df: pd.DataFrame, subset_cols: List[int]) -> Tuple[pd.DataFrame, int]:
    """Remove duplicate rows based on specified columns"""
    initial_count = len(df)
    cols_to_check = [df.columns[i] for i in subset_cols if i < len(df.columns)]
    df_cleaned = df.drop_duplicates(subset=cols_to_check, keep='first')
    duplicates_count = initial_count - len(df_cleaned)
    return df_cleaned, duplicates_count

def create_offering(service_id: str, service_name: str = None, locale: str = "en-US", 
                   expired: bool = False, include_name: bool = True) -> Dict[str, Any]:
    """Create a service offering object"""
    offering = {
        "expiredForSales": expired,
        "id": str(service_id),
        "isBundle": False,
    }
    if service_name and include_name:
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
# MODULE 1: UPDATE SERVICE (исправлено приведение типов/проверки)
# ------------------------
def update_zip_with_service(zip_file: zipfile.ZipFile, new_service: Dict[str, Any]) -> Tuple[Dict, Dict]:
    """Update all JSON files in ZIP with new service"""
    file_list = zip_file.namelist()
    json_files = [f for f in file_list if f.lower().endswith(".json") 
                  and "productofferinggroup/" in f.lower()]

    if not json_files:
        raise ValueError("JSON файлы не найдены в папке productOfferingGroup/")

    # Сохраняем все файлы (не только JSON), чтобы ничего не потерять
    original_structure = {name: zip_file.open(name).read() for name in file_list}
    updated_jsons = {}

    for json_filename in json_files:
        try:
            raw = original_structure[json_filename]
            json_data = json.loads(raw.decode("utf-8"))
            
            if "productOfferingsInGroup" not in json_data or not isinstance(json_data["productOfferingsInGroup"], list):
                st.warning(f"Пропущен: нет корректного productOfferingsInGroup → {json_filename}")
                continue

            existing_ids = {str(item.get("id")) for item in json_data["productOfferingsInGroup"]}
            if str(new_service.get("id")) in existing_ids:
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
# MODULE 1.5: EXPIRE + ADD (переписано с исправлениями)
# ------------------------
def process_expire_and_add_services(
    uploaded_zip: bytes,
    expire_excel: bytes,
    add_excel: bytes,
    locale: str = "en-US"
) -> Tuple[io.BytesIO, Dict[str, Any]]:
    """
    Обновляет ZIP архив:
    1) Экспайрит указанные услуги по Excel (2 колонки)
    2) Добавляет новые услуги по Excel (3 колонки)
    Сохраняет любые прочие файлы из исходного ZIP без изменений.
    """

    # --- Загружаем ZIP и сохраняем ВСЕ файлы ---
    zbuf = io.BytesIO(uploaded_zip)
    with zipfile.ZipFile(zbuf, "r") as zf:
        all_names = zf.namelist()
        if not all_names:
            raise ValueError("Пустой ZIP архив")
        all_bytes = {name: zf.read(name) for name in all_names}

    json_files = [
        n for n in all_names
        if n.lower().endswith(".json") and "productofferinggroup/" in n.lower()
    ]
    if not json_files:
        raise ValueError("В ZIP не найдено JSON файлов в папке productOfferingGroup/")

    # --- Читаем Excel-файлы из bytes через BytesIO ---
    df_expire = pd.read_excel(io.BytesIO(expire_excel), engine="openpyxl")
    df_add = pd.read_excel(io.BytesIO(add_excel), engine="openpyxl")

    # --- Нормализуем и фильтруем ---
    if df_expire.shape[1] < 2:
        raise ValueError("Excel для экспайра должен содержать 2 колонки: json_id | service_id")
    if df_add.shape[1] < 3:
        raise ValueError("Excel для добавления должен содержать 3 колонки: json_id | service_name | service_id")

    df_expire = df_expire.iloc[:, :2]
    df_expire.columns = ["json_id", "service_id"]
    df_expire = df_expire.dropna(subset=["json_id", "service_id"]).assign(
        json_id=lambda d: d["json_id"].astype(str).str.strip(),
        service_id=lambda d: d["service_id"].astype(str).str.strip()
    )
    df_expire = df_expire[(df_expire["json_id"] != "") & (df_expire["service_id"] != "")]

    df_add = df_add.iloc[:, :3]
    df_add.columns = ["json_id", "service_name", "service_id"]
    df_add = df_add.dropna(subset=["json_id", "service_name", "service_id"]).assign(
        json_id=lambda d: d["json_id"].astype(str).str.strip(),
        service_id=lambda d: d["service_id"].astype(str).str.strip(),
        service_name=lambda d: d["service_name"].astype(str).str.strip()
    )
    # Удаляем явные мусорные значения
    df_add = df_add[
        (df_add["json_id"] != "") &
        (df_add["service_id"] != "") &
        (df_add["service_id"].str.lower() != "nan")
    ]

    # --- Группировки ---
    expire_map: Dict[str, List[str]] = df_expire.groupby("json_id")["service_id"].apply(list).to_dict()
    add_map: Dict[str, List[Dict[str, str]]] = df_add.groupby("json_id")[["service_name", "service_id"]].apply(
        lambda x: x.to_dict("records")
    ).to_dict()

    updated_jsons: Dict[str, str] = {}
    stats = {
        "files_processed": 0,
        "expired": 0,
        "already_expired": 0,
        "added": 0,
        "skipped_existing": 0
    }

    # --- Обработка JSON файлов ---
    for filename in json_files:
        data = all_bytes[filename]
        try:
            json_data = json.loads(data.decode("utf-8"))
        except Exception as e:
            st.warning(f"Ошибка чтения {filename}: {e}")
            continue

        json_id = str(json_data.get("id", "")).strip()
        if not json_id:
            stats["files_processed"] += 1
            continue

        offerings = json_data.get("productOfferingsInGroup")
        if not isinstance(offerings, list):
            offerings = []

        modified = False

        # 1) Экспайр существующих услуг
        for sid in expire_map.get(json_id, []):
            sid = str(sid)
            for item in offerings:
                if str(item.get("id")) == sid:
                    if not item.get("expiredForSales", False):
                        item["expiredForSales"] = True
                        stats["expired"] += 1
                        modified = True
                    else:
                        stats["already_expired"] += 1

        # 2) Добавление новых услуг
        existing_ids = {str(o.get("id")) for o in offerings}
        for rec in add_map.get(json_id, []):
            nid = str(rec.get("service_id", "")).strip()
            nname = str(rec.get("service_name", "")).strip()
            if not nid or nid.lower() == "nan":
                continue
            if nid in existing_ids:
                stats["skipped_existing"] += 1
                continue
            offerings.append({
                "expiredForSales": False,
                "id": nid,
                "isBundle": False,
                "name": [{"locale": locale, "value": nname}]
            })
            existing_ids.add(nid)
            stats["added"] += 1
            modified = True

        # 3) Сохранение изменений
        if modified:
            json_data["productOfferingsInGroup"] = offerings
            updated_jsons[filename] = json.dumps(json_data, ensure_ascii=False, indent=4)

        stats["files_processed"] += 1

    # --- Сборка нового ZIP: сохраняем ВСЕ файлы, меняем только обновлённые JSON ---
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as znew:
        for name in all_names:
            if name in updated_jsons:
                znew.writestr(name, updated_jsons[name])
            else:
                znew.writestr(name, all_bytes[name])
    out.seek(0)

    return out, stats

# ------------------------
# UI: NAVIGATION
# ------------------------
st.sidebar.title("Навигация")
page = st.sidebar.radio(
    "Выберите действие:",
    [
        "Добавить услугу в существующие тарифные планы",
        "ADD NEW AND EXPIRE OLD AddOns",
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
# MODULE 1.5 UI: EXPIRE AND ADD SERVICE (исправлено)
# =======================================================
elif page == "ADD NEW AND EXPIRE OLD AddOns":
    st.title("Экспайр и добавление AddOns с помощью Excel")

    st.markdown("""
    ### 🧩 Инструкция:
    1. **Загрузите ZIP** с JSON-файлами (структура `productOfferingGroup/...json`)  
    2. **Загрузите Excel для экспайра** — 2 колонки:
       - `json_id` → ID POG   
       - `service_id` → ID услуги, которую нужно заэкспайрить (значение expired: `true`)
    3. **Загрузите Excel для добавления новых услуг** — 3 колонки:
       - `POG ID` → POG , куда добавить
       - `name` → имя новой услуги
       - `id` → её уникальный ID  
    4. Нажмите кнопку **Запустить обработку**
    """)

    uploaded_zip = st.file_uploader("📦 ZIP архив с JSON файлами", type=["zip"], key="expire_add_zip")
    excel_expire = st.file_uploader("📘 Excel для экспайра (2 колонки)", type=["xls", "xlsx"])
    excel_add = st.file_uploader("📗 Excel для добавления новых услуг (3 колонки)", type=["xls", "xlsx"])
    locale = st.text_input("🌐 Язык (locale)", value="en-US")

    if st.button("🚀 Запустить обработку", type="primary"):
        if not uploaded_zip or not excel_expire or not excel_add:
            st.error("Пожалуйста, загрузите все три файла.")
            st.stop()

        with st.spinner("Обработка ZIP архива..."):
            try:
                new_zip, stats = process_expire_and_add_services(
                    uploaded_zip.read(),
                    excel_expire.read(),
                    excel_add.read(),
                    locale
                )

                st.success("✅ ZIP успешно обновлён!")

                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Файлов обработано", stats["files_processed"])
                col2.metric("Экспайрено услуг", stats["expired"])
                col3.metric("Добавлено новых", stats["added"])
                col4.metric("Пропущено (уже существовали)", stats["skipped_existing"])

                if stats.get("already_expired", 0) > 0:
                    st.caption(f"Уже были экспайрены: {stats['already_expired']}")

                st.download_button(
                    "📥 Скачать обновлённый ZIP",
                    new_zip,
                    "updated_addons.zip",
                    "application/zip",
                    type="primary"
                )

            except Exception as e:
                st.error(f"Ошибка: {e}")
                with st.expander("Подробности ошибки"):
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
            locale_gen = st.text_input("Language", value="en-US")
        
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
                
                df_cleaned, duplicates_count = remove_duplicates(df, [0])
                if duplicates_count > 0:
                    st.info(f"Удалено дубликатов: {duplicates_count}")
                
                id_col, name_col = df_cleaned.columns[0], df_cleaned.columns[1]
                offerings = [
                    create_offering(r[id_col], r[name_col], locale_gen) 
                    for _, r in df_cleaned.iterrows() 
                    if pd.notna(r[id_col])
                ]
                
                if not offerings:
                    st.warning("Не найдено валидных услуг в Excel файле")
                    st.stop()
                
                if not name.strip() or not uid.strip():
                    st.error("Заполните 'Название услуги' и 'ID услуги' для JSON.")
                    st.stop()

                final = build_json(name, uid, locale_gen, offerings, purpose="addOn")
                
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
        
        locale_gen = st.text_input("Language", value="en-US")
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
                
                df_cleaned, duplicates_count = remove_duplicates(df, [0, 1, 2, 3])
                if duplicates_count > 0:
                    st.info(f"Удалено дубликатов: {duplicates_count}")
                
                grouped = df_cleaned.groupby([df_cleaned.columns[0], df_cleaned.columns[1]])
                
                zip_buffer = io.BytesIO()
                json_count = 0
                
                with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                    for (json_name, json_id), group in grouped:
                        offerings = [
                            create_offering(r[df_cleaned.columns[2]], r[df_cleaned.columns[3]], locale_gen)
                            for _, r in group.iterrows()
                            if pd.notna(r[df_cleaned.columns[2]])
                        ]
                        
                        if not offerings:
                            continue
                        
                        final = build_json(str(json_name), str(json_id), locale_gen, offerings, purpose="addOn")
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
            locale_gen = st.text_input("Language", value="en-US")
        
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

                if not name.strip() or not uid.strip():
                    st.error("Заполните 'Название swap offer' и 'ID swap offer' для JSON.")
                    st.stop()
                
                final = build_json(name, uid, locale_gen, offerings, purpose="replaceOffer")
                
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
                
                df_cleaned, duplicates_count = remove_duplicates(df, [0, 1])
                if duplicates_count > 0:
                    st.info(f"Удалено дубликатов: {duplicates_count}")
                
                offer_col, category_col = df_cleaned.columns[0], df_cleaned.columns[1]
                grouped = df_cleaned.groupby(offer_col)
                
                zip_buffer = io.BytesIO()
                json_count = 0
                
                with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                    for offer_id, group in grouped:
                        if pd.isna(offer_id):
                            continue
                        
                        categories = [
                            str(r[category_col]).strip()
                            for _, r in group.iterrows()
                            if pd.notna(r[category_col])
                        ]
                        
                        if not categories:
                            continue
                        
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
