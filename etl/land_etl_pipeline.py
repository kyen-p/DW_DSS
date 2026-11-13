"""ETL tin đăng Đất: CSV -> Land_Staging -> DW_Land (SQL Server).

Chạy từ thư mục DW_DSS_Land (không cần đổi đường dẫn trong code):
  python etl/land_etl_pipeline.py --mode prepare
  python etl/land_etl_pipeline.py --mode load --sql-server ".\\SQLEXPRESS"

Lưu ý: --mode load chỉ nạp khi tất cả bảng đích đang trống, không xóa dữ liệu.
Đơn vị: Price (triệu đồng), PriceBillion (tỷ), Area (m²), Width/Length (m).
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd


# Các ánh xạ tỉnh thành tái sử dụng từ ETL.py cũ (sửa Huế, thống nhất cách ghi TP).
# Khóa trái là địa bàn nguồn (cũ hoặc mới), giá trị là tỉnh/thành dùng phân tích.
PROVINCE_GROUPS = {
    "Hà Nội": ["Hà Nội", "TP. Hà Nội"],
    "Thanh Hóa": ["Thanh Hóa"],
    "Lạng Sơn": ["Lạng Sơn"],
    "TP. Huế": ["Thừa Thiên Huế", "Thừa Thiên - Huế", "Huế", "TP. Huế"],
    "Nghệ An": ["Nghệ An"],
    "Hà Tĩnh": ["Hà Tĩnh"],
    "Lai Châu": ["Lai Châu"],
    "Điện Biên": ["Điện Biên"],
    "Cao Bằng": ["Cao Bằng"],
    "Quảng Ninh": ["Quảng Ninh"],
    "Sơn La": ["Sơn La"],
    "Tuyên Quang": ["Tuyên Quang", "Hà Giang"],
    "Lào Cai": ["Lào Cai", "Yên Bái"],
    "Thái Nguyên": ["Thái Nguyên", "Bắc Kạn"],
    "Phú Thọ": ["Phú Thọ", "Vĩnh Phúc", "Hòa Bình"],
    "Bắc Ninh": ["Bắc Ninh", "Bắc Giang"],
    "Hưng Yên": ["Hưng Yên", "Thái Bình"],
    "TP. Hải Phòng": ["Hải Phòng", "TP. Hải Phòng", "Hải Dương"],
    "Ninh Bình": ["Ninh Bình", "Nam Định", "Hà Nam"],
    "Quảng Trị": ["Quảng Trị", "Quảng Bình"],
    "TP. Đà Nẵng": ["Đà Nẵng", "TP. Đà Nẵng", "Quảng Nam"],
    "Quảng Ngãi": ["Quảng Ngãi", "Kon Tum"],
    "Gia Lai": ["Gia Lai", "Bình Định"],
    "Khánh Hòa": ["Khánh Hòa", "Ninh Thuận"],
    "Lâm Đồng": ["Lâm Đồng", "Bình Thuận", "Đắk Nông"],
    "Đắk Lắk": ["Đắk Lắk", "Phú Yên"],
    "TP. Hồ Chí Minh": [
        "TP. Hồ Chí Minh", "Thành phố Hồ Chí Minh", "TPHCM", "TP.HCM",
        "Bình Dương", "Bà Rịa - Vũng Tàu", "Bà Rịa Vũng Tàu",
    ],
    "Đồng Nai": ["Đồng Nai", "Bình Phước"],
    "Tây Ninh": ["Tây Ninh", "Long An"],
    "TP. Cần Thơ": ["Cần Thơ", "TP. Cần Thơ", "Sóc Trăng", "Hậu Giang"],
    "Vĩnh Long": ["Vĩnh Long", "Bến Tre", "Trà Vinh"],
    "Đồng Tháp": ["Đồng Tháp", "Tiền Giang"],
    "Cà Mau": ["Cà Mau", "Bạc Liêu"],
    "An Giang": ["An Giang", "Kiên Giang"],
}

# Theo nhóm vùng miền có trong ETL.py cũ, áp dụng sau khi chuẩn hóa tỉnh mới.
REGION_NEW = {
    **{p: "Miền Bắc" for p in [
        "Hà Nội", "Lạng Sơn", "Lai Châu", "Điện Biên", "Cao Bằng", "Quảng Ninh",
        "Sơn La", "Tuyên Quang", "Lào Cai", "Thái Nguyên", "Phú Thọ", "Bắc Ninh",
        "Hưng Yên", "TP. Hải Phòng", "Ninh Bình",
    ]},
    **{p: "Miền Trung" for p in [
        "Thanh Hóa", "TP. Huế", "Nghệ An", "Hà Tĩnh", "Quảng Trị", "TP. Đà Nẵng",
        "Quảng Ngãi", "Gia Lai", "Khánh Hòa", "Lâm Đồng", "Đắk Lắk",
    ]},
    **{p: "Miền Nam" for p in [
        "TP. Hồ Chí Minh", "Đồng Nai", "Tây Ninh", "TP. Cần Thơ", "Vĩnh Long",
        "Đồng Tháp", "Cà Mau", "An Giang",
    ]},
}
assert len(PROVINCE_GROUPS) == len(REGION_NEW) == 34

# Vừa giữ nhãn cũ để tận dụng Cube/DW, vừa xác định ranh giới chính xác.
AREA_GROUPS = [
    (1, "Dưới 50 m2", 0, 50),
    (2, "50 - 100 m2", 50, 100),
    (3, "100 - 200 m2", 100, 200),
    (4, "200 - 500 m2", 200, 500),
    (5, "500 - 1000 m2", 500, 1000),
    (6, "Trên 1000 m2", 1000, None),
]
PRICE_GROUPS = [
    (1, "Dưới 1 tỷ", 0, 1),
    (2, "1 - 3 tỷ", 1, 3),
    (3, "3 - 5 tỷ", 3, 5),
    (4, "5 - 10 tỷ", 5, 10),
    (5, "Trên 10 tỷ", 10, None),
]

# Đúng 28 cột CSV theo đúng thứ tự đầu vào và cột ở stgRawListing.
RAW_MAPPING = {
    "Title": "Title", "Price": "Price", "Area": "Area", "Location": "Location",
    "Listing ID": "ListingID", "Last Updated": "LastUpdated",
    "Property Type": "PropertyType", "Width": "Width", "Length": "Length",
    "Bedrooms": "Bedrooms", "Bathrooms": "Bathrooms", "Floors": "Floors",
    "Position": "Position", "Direction": "Direction", "Alley Width": "AlleyWidth",
    "Road Type": "RoadType", "Description": "Description", "Latitude": "Latitude",
    "Longitude": "Longitude", "VIP Account": "VIPAccount", "Avatar": "Avatar",
    "Agent Role": "AgentRole", "Agent Name": "AgentName",
    "Agent Listing Count": "AgentListingCount", "Province": "Province",
    "Property Type Slug": "PropertyTypeSlug", "Scraped At": "ScrapedAt",
    "Last Updated Date": "LastUpdatedDate",
}

CLEAN_COLUMNS = [
    "ListingID", "SourceRawRowID", "Price", "PriceBillion", "Area", "Width",
    "Length", "PricePerM2", "PriceSegment", "AreaGroup", "ProvinceOriginal",
    "ProvinceNew", "RegionGroup", "Position", "ScrapedAt", "LastUpdatedAt", "TimeKey",
]


def province_key(value: object) -> str:
    """Chuẩn hóa tên/slugs tỉnh để tra cứu, không thay đổi Location gốc."""
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip().lower().replace("đ", "d")
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"[^a-z0-9]+", " ", text).strip()
    text = re.sub(r"^(tinh|thanh pho|tp)\s+", "", text)
    text = " ".join(text.split())
    # Biệt danh phổ biến của thành phố Hồ Chí Minh.
    return "ho chi minh" if text in ("tphcm", "hcm", "sai gon") else text


def build_province_lookups() -> tuple[dict[str, str], dict[str, str]]:
    to_new: dict[str, str] = {}
    to_original: dict[str, str] = {}
    for province_new, aliases in PROVINCE_GROUPS.items():
        for alias in set(aliases + [province_new]):
            key = province_key(alias)
            if key in to_new and to_new[key] != province_new:
                raise ValueError(f"Tên tỉnh nhập nhằng: {alias}")
            to_new[key] = province_new
            to_original[key] = alias
    return to_new, to_original


PROVINCE_TO_NEW, _ = build_province_lookups()


def get_original_province(location: object, slug: object) -> tuple[str, str, str, bool]:
    """Ưu tiên phần tỉnh cuối Location, đối chiếu Province; không đoán khi xung đột.

    Trả về (ProvinceOriginal, ProvinceNew, RegionGroup, has_conflict).
    """
    loc = str(location or "").strip()
    loc = re.sub(r"\(\s*mới\s*\)", "", loc, flags=re.IGNORECASE).strip()
    province_tail = loc.split(",")[-1].strip() if loc else ""
    loc_new = PROVINCE_TO_NEW.get(province_key(province_tail))
    slug_new = PROVINCE_TO_NEW.get(province_key(slug))
    conflict = bool(loc_new and slug_new and loc_new != slug_new)
    if loc_new:
        original, new = province_tail, loc_new
    elif slug_new:
        # Khi Location không rõ thì dùng Province slug đã ánh xạ được.
        original, new = str(slug).strip(), slug_new
    else:
        return "Không xác định", "Không xác định", "Không xác định", False
    return original[:100], new, REGION_NEW[new], conflict


def group_value(value: float, definitions: list[tuple[int, str, float, float | None]]) -> str:
    for _, label, lower, upper in definitions:
        if value >= lower and (upper is None or value < upper):
            return label
    raise ValueError(f"Không phân nhóm được: {value}")


def read_source(csv_path: Path) -> pd.DataFrame:
    if not csv_path.is_file():
        raise FileNotFoundError(f"Không tìm thấy CSV gốc: {csv_path}")
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, encoding="utf-8-sig", low_memory=False)
    missing = set(RAW_MAPPING).difference(df.columns)
    extra = set(df.columns).difference(RAW_MAPPING)
    if missing or extra:
        raise ValueError(f"CSV không đúng 28 cột; thiếu={sorted(missing)}; thừa={sorted(extra)}")
    df = df[list(RAW_MAPPING)].copy()
    # 1-based ordinal of records (not text lines, because Description can contain newlines).
    df.insert(0, "RawRowID", np.arange(1, len(df) + 1, dtype=np.int64))
    return df


def transform(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Giữ bản ghi có Scraped At mới nhất của mỗi Listing ID, rồi validate.

    Nếu bản mới nhất lỗi giá/diện tích, không tự động dùng bản cũ hơn.
    Mỗi dòng bị loại ghi một lý do chính (tránh đếm trùng khi đối soát).
    """
    report: dict = {"source_rows": len(raw), "source_columns": len(RAW_MAPPING)}
    rejected: list[pd.DataFrame] = []

    def reject(frame: pd.DataFrame, reason: str) -> None:
        if frame.empty:
            return
        rejected.append(pd.DataFrame({
            "SourceRawRowID": frame["RawRowID"].astype("int64"),
            "ListingIDRaw": frame["Listing ID"].astype(str).str.slice(0, 255),
            "RejectReason": reason,
        }))

    land = raw.loc[raw["Property Type"].str.strip().eq("Đất")].copy()
    report["land_rows"] = len(land)
    report["out_of_scope_non_land"] = len(raw) - len(land)

    number_id = pd.to_numeric(land["Listing ID"].str.strip(), errors="coerce")
    valid_id = (number_id.notna() & np.isfinite(number_id) &
                number_id.gt(0) & number_id.lt(9_000_000_000_000_000) & number_id.eq(np.floor(number_id)))
    reject(land.loc[~valid_id], "INVALID_LISTING_ID")
    land = land.loc[valid_id].copy()
    land["_id"] = number_id.loc[valid_id].astype("int64")
    report["reject_invalid_id"] = int((~valid_id).sum())

    land["_scraped"] = pd.to_datetime(land["Scraped At"], errors="coerce", format="%Y-%m-%d %H:%M:%S")
    land = land.sort_values(["_scraped", "RawRowID"], ascending=[False, False],
                            na_position="last", kind="mergesort")
    duplicate = land.duplicated("_id", keep="first")
    reject(land.loc[duplicate], "DUPLICATE_LISTING_ID")
    report["reject_duplicate_id"] = int(duplicate.sum())
    land = land.loc[~duplicate].copy()

    # Giữ logic loại tin chỉ cho thuê của ETL.py cũ; tiêu đề có cả bán và thuê vẫn giữ.
    title = land["Title"].fillna("").astype(str)
    rental = title.str.contains("cho thuê|cho thue", case=False, regex=True, na=False)
    selling = title.str.contains("bán|ban|cần bán", case=False, regex=True, na=False)
    rental_only = rental & ~selling
    report["out_of_scope_rental"] = int(rental_only.sum())
    land = land.loc[~rental_only].copy()

    land["_price"] = pd.to_numeric(land["Price"].str.strip(), errors="coerce")
    land["_area"] = pd.to_numeric(land["Area"].str.strip(), errors="coerce")
    valid_price = land["_price"].notna() & np.isfinite(land["_price"]) & land["_price"].gt(0) & land["_price"].lt(1_000_000)
    reject(land.loc[~valid_price], "INVALID_PRICE")
    report["reject_invalid_price"] = int((~valid_price).sum())
    land = land.loc[valid_price].copy()

    valid_area = land["_area"].notna() & np.isfinite(land["_area"]) & land["_area"].between(10, 1_000_000, inclusive="both")
    reject(land.loc[~valid_area], "INVALID_AREA")
    report["reject_invalid_area"] = int((~valid_area).sum())
    land = land.loc[valid_area].copy()

    # Kích thước có thể thiếu; chỉ giữ giá trị nguồn hợp lệ, không tự suy ra từ Area.
    width = pd.to_numeric(land["Width"].str.strip(), errors="coerce")
    length = pd.to_numeric(land["Length"].str.strip(), errors="coerce")
    width = width.where(np.isfinite(width) & width.gt(0) & width.le(1000))
    length = length.where(np.isfinite(length) & length.gt(0) & length.le(10000))

    provinces = [get_original_province(l, p) for l, p in zip(land["Location"], land["Province"])]
    loc_orig = [p[0] for p in provinces]
    loc_new = [p[1] for p in provinces]
    loc_region = [p[2] for p in provinces]
    report["province_conflict_count"] = sum(p[3] for p in provinces)
    report["province_conflict_raw_row_ids"] = [
        int(row_id) for row_id, prov in zip(land["RawRowID"], provinces) if prov[3]
    ]
    report["unknown_province_count"] = loc_new.count("Không xác định")
    report["province_new_distinct"] = len(set(loc_new) - {"Không xác định"})

    scraped = land["_scraped"]
    updated = pd.to_datetime(land["Last Updated Date"], errors="coerce", format="%d/%m/%Y %H:%M")
    # Nếu ngày cập nhật không đọc được, khóa thời gian dùng -1, không loại cả tin.
    time_key = updated.dt.year.mul(100).add(updated.dt.month).fillna(-1).astype("int32")
    pos = land["Position"].fillna("").astype(str).str.strip()
    pos = pos.where(pos.isin(["Đường chính", "Trong hẻm"]), "Không xác định")
    price = land["_price"].astype(float)
    area = land["_area"].astype(float)
    price_billions = price / 1000.0
    price_per_m2 = price / area
    cleaned = pd.DataFrame({
        "ListingID": land["_id"].to_numpy(dtype=np.int64),
        "SourceRawRowID": land["RawRowID"].to_numpy(dtype=np.int64),
        "Price": price.round(4).to_numpy(),
        "PriceBillion": price_billions.round(6).to_numpy(),
        "Area": area.round(4).to_numpy(),
        "Width": width.round(4).to_numpy(),
        "Length": length.round(4).to_numpy(),
        "PricePerM2": price_per_m2.round(6).to_numpy(),
        "PriceSegment": [group_value(v, PRICE_GROUPS) for v in price_billions],
        "AreaGroup": [group_value(v, AREA_GROUPS) for v in area],
        "ProvinceOriginal": loc_orig,
        "ProvinceNew": loc_new,
        "RegionGroup": loc_region,
        "Position": pos.to_numpy(),
        "ScrapedAt": scraped.to_numpy(),
        "LastUpdatedAt": updated.to_numpy(),
        "TimeKey": time_key.to_numpy(),
    }, columns=CLEAN_COLUMNS)
    # Đảm bảo decimal > 0 ở DB, tránh 1e-6 bị làm tròn thành 0.
    tiny_ppm2 = cleaned["PricePerM2"] <= 0
    if tiny_ppm2.any():
        invalid_ids = set(cleaned.loc[tiny_ppm2, "SourceRawRowID"])
        reject(land.loc[land["RawRowID"].isin(invalid_ids)], "PRICE_PER_M2_ROUNDS_TO_ZERO")
        cleaned = cleaned.loc[~tiny_ppm2].copy()
    report["reject_zero_price_per_m2"] = int(tiny_ppm2.sum())
    report["clean_rows"] = len(cleaned)
    report["missing_width_clean"] = int(cleaned["Width"].isna().sum())
    report["missing_length_clean"] = int(cleaned["Length"].isna().sum())
    report["unknown_position_clean"] = int(cleaned["Position"].eq("Không xác định").sum())
    report["missing_or_unusable_dates_clean"] = int((cleaned["ScrapedAt"].isna() | cleaned["LastUpdatedAt"].isna()).sum())
    have_dates = cleaned["ScrapedAt"].notna() & cleaned["LastUpdatedAt"].notna()
    age = (cleaned["ScrapedAt"] - cleaned["LastUpdatedAt"]).dt.total_seconds() / 86400
    report["negative_age_days_clean"] = int((have_dates & age.lt(0)).sum())
    report["old_over_30_days_clean"] = int((have_dates & age.gt(30)).sum())
    rejected_df = (pd.concat(rejected, ignore_index=True) if rejected else
                   pd.DataFrame(columns=["SourceRawRowID", "ListingIDRaw", "RejectReason"]))
    report["reject_rows"] = len(rejected_df)
    report["reject_by_reason"] = {str(k): int(v) for k, v in Counter(rejected_df["RejectReason"]).items()}
    report["accounting_ok"] = (
        report["source_rows"] == report["out_of_scope_non_land"] +
        report["out_of_scope_rental"] + report["reject_rows"] + report["clean_rows"]
    )
    if not report["accounting_ok"]:
        raise RuntimeError("Không đối soát được số lượng Raw / ngoài phạm vi / Reject / Clean")
    if cleaned["ListingID"].duplicated().any() or cleaned["SourceRawRowID"].duplicated().any():
        raise RuntimeError("ListingID hoặc SourceRawRowID trùng trong CleanListing")
    return cleaned, rejected_df, report


def write_prepared_files(project: Path, clean: pd.DataFrame, rejects: pd.DataFrame, report: dict) -> None:
    output = project / "data" / "processed"
    output.mkdir(parents=True, exist_ok=True)
    clean_file = output / "VN-real-estate-Apr-Sept-2025-cleaned.csv"
    clean.to_csv(clean_file, index=False, encoding="utf-8-sig", date_format="%Y-%m-%d %H:%M:%S")
    rejects.to_csv(output / "ETL_Rejects.csv", index=False, encoding="utf-8-sig")
    with (output / "ETL_Report.json").open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"[OK] CSV sạch: {clean_file} ({len(clean):,} dòng, {len(clean.columns)} cột)")
    print(f"[OK] Reject: {len(rejects):,} dòng; báo cáo: {output / 'ETL_Report.json'}")


def connect_sql(server: str, sql_user: str | None, sql_password: str | None):
    try:
        import pyodbc
    except ImportError as exc:
        raise RuntimeError("Chưa cài pyodbc: python -m pip install -r etl/requirements.txt") from exc
    available = pyodbc.drivers()
    drivers = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]
    driver = next((d for d in drivers if d in available), None)
    if not driver:
        raise RuntimeError(f"Thiếu Microsoft ODBC Driver 17/18 for SQL Server; đang có: {available}")
    options = [f"DRIVER={{{driver}}}", f"SERVER={server}", "DATABASE=master", "Encrypt=yes", "TrustServerCertificate=yes"]
    if sql_user:
        if not sql_password:
            raise RuntimeError("Có --sql-user thì cần DW_SQL_PASSWORD (biến môi trường).")
        options += [f"UID={sql_user}", f"PWD={sql_password}"]
    else:
        options.append("Trusted_Connection=yes")
    return pyodbc.connect(";".join(options), autocommit=False, timeout=30)


def require_empty_schema(cur) -> None:
    # Không xóa hoặc ghi đè dữ liệu nếu đã có: lần nạp đầu tiên phải là bảng trống.
    expect = {
        "Land_Staging": {"stgRawListing": 29, "CleanListing": 17, "Reject_Log": 5},
        "DW_Land": {"Dim_Time": 6, "Dim_Location": 4, "Dim_Size_Group": 5,
                    "Dim_Price_Segment": 5, "Dim_Land_Profile": 2, "Fact_Listing": 15},
    }
    for db, tables in expect.items():
        for table, ncols in tables.items():
            full = f"[{db}].dbo.[{table}]"
            # Dùng INFORMATION_SCHEMA của từng database để kiểm tra V2.
            columns = cur.execute(
                f"SELECT COUNT(*) FROM [{db}].INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?", table
            ).fetchone()[0]
            if columns != ncols:
                raise RuntimeError(f"Schema {full} sai: {columns} cột, mong đợi {ncols}. Dừng trước khi ghi.")
            count = cur.execute(f"SELECT COUNT_BIG(*) FROM {full}").fetchone()[0]
            if count:
                raise RuntimeError(f"{full} đã có {count:,} dòng. Không ghi đè; hãy kiểm tra dữ liệu cũ.")


def sql_batch_insert(cur, target: str, columns: list[str], rows, batch_size: int = 300) -> int:
    quoted = ", ".join(f"[{column}]" for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    statement = f"INSERT INTO {target} ({quoted}) VALUES ({placeholders})"
    cur.fast_executemany = True
    count = 0
    buffer = []
    for row in rows:
        buffer.append(tuple(row))
        if len(buffer) >= batch_size:
            cur.executemany(statement, buffer)
            count += len(buffer)
            buffer.clear()
    if buffer:
        cur.executemany(statement, buffer)
        count += len(buffer)
    return count


def optional_float(value):
    return None if pd.isna(value) else float(value)


def optional_datetime(value):
    return None if pd.isna(value) else value.to_pydatetime() if hasattr(value, "to_pydatetime") else value


def load_sql(raw: pd.DataFrame, clean: pd.DataFrame, rejects: pd.DataFrame,
             server: str, user: str | None, password: str | None) -> None:
    conn = connect_sql(server, user, password)
    cur = conn.cursor()
    identity_on = False
    try:
        require_empty_schema(cur)
        print("[SQL] Đã xác nhận đúng 9 bảng V2 và tất cả đều trống.")
        cur.execute("SET IDENTITY_INSERT [Land_Staging].dbo.stgRawListing ON")
        identity_on = True
        raw_columns = ["RawRowID"] + list(RAW_MAPPING.values())

        def iter_raw():
            for row in raw.itertuples(index=False, name=None):
                yield (int(row[0]), *(str(x) for x in row[1:]))

        count_raw = sql_batch_insert(cur, "[Land_Staging].dbo.stgRawListing", raw_columns, iter_raw())
        cur.execute("SET IDENTITY_INSERT [Land_Staging].dbo.stgRawListing OFF")
        identity_on = False
        print(f"[SQL] Raw: {count_raw:,}")

        def iter_clean():
            for r in clean.itertuples(index=False):
                yield (
                    int(r.ListingID), int(r.SourceRawRowID), float(r.Price), float(r.PriceBillion),
                    float(r.Area), optional_float(r.Width), optional_float(r.Length),
                    float(r.PricePerM2), r.PriceSegment, r.AreaGroup, r.ProvinceOriginal,
                    r.ProvinceNew, r.RegionGroup, r.Position, optional_datetime(r.ScrapedAt),
                    optional_datetime(r.LastUpdatedAt), int(r.TimeKey),
                )
        count_clean = sql_batch_insert(cur, "[Land_Staging].dbo.CleanListing", CLEAN_COLUMNS, iter_clean())
        print(f"[SQL] Clean: {count_clean:,}")

        count_reject = sql_batch_insert(
            cur, "[Land_Staging].dbo.Reject_Log",
            ["SourceRawRowID", "ListingIDRaw", "RejectReason"],
            ((int(r.SourceRawRowID), str(r.ListingIDRaw), r.RejectReason)
             for r in rejects.itertuples(index=False)),
        )
        print(f"[SQL] Reject_Log: {count_reject:,}")

        # Khóa unknown bảo đảm Fact không mất tham chiếu nếu ngày/tỉnh không xác định.
        cur.execute(
            "INSERT INTO [DW_Land].dbo.Dim_Time "
            "(TimeKey, [Year], [Quarter], [Month], YearMonth, MonthName) "
            "VALUES (-1, 0, 0, 0, 'Unknown', N'Không xác định')"
        )
        valid_months = sorted(set(map(int, clean.loc[clean["TimeKey"].ne(-1), "TimeKey"])))
        times = [(x, x // 100, ((x % 100 - 1) // 3 + 1), x % 100,
                  f"{x // 100:04d}-{x % 100:02d}", f"Tháng {x % 100:02d}") for x in valid_months]
        sql_batch_insert(cur, "[DW_Land].dbo.Dim_Time",
                         ["TimeKey", "Year", "Quarter", "Month", "YearMonth", "MonthName"], times)
        sql_batch_insert(cur, "[DW_Land].dbo.Dim_Size_Group",
                         ["SizeKey", "AreaGroup", "MinArea", "MaxArea", "SortOrder"],
                         ((key, label, lower, upper, key) for key, label, lower, upper in AREA_GROUPS))
        sql_batch_insert(cur, "[DW_Land].dbo.Dim_Price_Segment",
                         ["SegmentKey", "PriceSegment", "MinBillion", "MaxBillion", "SortOrder"],
                         ((key, label, lower, upper, key) for key, label, lower, upper in PRICE_GROUPS))
        locations = sorted(set(zip(clean["ProvinceOriginal"], clean["ProvinceNew"], clean["RegionGroup"])))
        sql_batch_insert(cur, "[DW_Land].dbo.Dim_Location",
                         ["ProvinceOriginal", "ProvinceNew", "RegionGroup"], locations)
        positions = sorted(set(clean["Position"]))
        sql_batch_insert(cur, "[DW_Land].dbo.Dim_Land_Profile", ["Position"], ((p,) for p in positions))
        loc_key = {(a, b, c): int(k) for k, a, b, c in cur.execute(
            "SELECT LocationKey, ProvinceOriginal, ProvinceNew, RegionGroup FROM [DW_Land].dbo.Dim_Location"
        ).fetchall()}
        pos_key = {p: int(k) for k, p in cur.execute(
            "SELECT ProfileKey, Position FROM [DW_Land].dbo.Dim_Land_Profile"
        ).fetchall()}
        size_key = {label: int(k) for k, label in cur.execute(
            "SELECT SizeKey, AreaGroup FROM [DW_Land].dbo.Dim_Size_Group"
        ).fetchall()}
        seg_key = {label: int(k) for k, label in cur.execute(
            "SELECT SegmentKey, PriceSegment FROM [DW_Land].dbo.Dim_Price_Segment"
        ).fetchall()}
        valid_time_keys = {-1, *valid_months}

        def iter_fact():
            for r in clean.itertuples(index=False):
                time = int(r.TimeKey)
                if time not in valid_time_keys:
                    raise ValueError(f"Không lookup được TimeKey={time}")
                return_fields = (
                    int(r.ListingID), time,
                    loc_key[(r.ProvinceOriginal, r.ProvinceNew, r.RegionGroup)],
                    size_key[r.AreaGroup], seg_key[r.PriceSegment], pos_key[r.Position],
                    float(r.Price), float(r.PriceBillion), float(r.Area),
                    optional_float(r.Width), optional_float(r.Length), float(r.PricePerM2),
                    optional_datetime(r.ScrapedAt), optional_datetime(r.LastUpdatedAt),
                )
                yield return_fields

        fact_columns = [
            "ListingID", "TimeKey", "LocationKey", "SizeKey", "SegmentKey", "ProfileKey",
            "Price", "PriceBillion", "Area", "Width", "Length", "PricePerM2", "ScrapedAt", "LastUpdatedAt",
        ]
        count_fact = sql_batch_insert(cur, "[DW_Land].dbo.Fact_Listing", fact_columns, iter_fact())
        expected = (len(raw), len(clean), len(rejects), len(clean))
        if (count_raw, count_clean, count_reject, count_fact) != expected:
            raise RuntimeError(f"Đếm số dòng nạp không khớp: {(count_raw, count_clean, count_reject, count_fact)} != {expected}")
        for db, table, target in [
            ("Land_Staging", "stgRawListing", len(raw)), ("Land_Staging", "CleanListing", len(clean)),
            ("Land_Staging", "Reject_Log", len(rejects)), ("DW_Land", "Fact_Listing", len(clean)),
        ]:
            actual = cur.execute(f"SELECT COUNT_BIG(*) FROM [{db}].dbo.[{table}]").fetchone()[0]
            if actual != target:
                raise RuntimeError(f"Sai số dòng {db}.{table}: {actual} != {target}")
        conn.commit()
        print(f"[OK] Đã COMMIT SQL Server: Fact_Listing={count_fact:,}. Đã đối soát số dòng.")
    except Exception:
        conn.rollback()
        print("[SQL] Có lỗi: đã ROLLBACK transaction, không coi là nạp thành công.", file=sys.stderr)
        raise
    finally:
        if identity_on:
            try:
                cur.execute("SET IDENTITY_INSERT [Land_Staging].dbo.stgRawListing OFF")
            except Exception:
                pass
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="ETL tin đăng Đất CSV -> SQL Server (không SSIS)")
    parser.add_argument("--mode", choices=["prepare", "load"], default="prepare",
                        help="prepare: làm sạch + xuất CSV; load: làm sạch rồi nạp vào SQL Server")
    parser.add_argument("--sql-server", help="Tên SQL Server, ví dụ .\\SQLEXPRESS (bắt buộc nếu load)")
    parser.add_argument("--sql-user", help="SQL Authentication (bỏ trống để dùng Windows Authentication)")
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    source = project / "data" / "raw" / "VN-real-estate-Apr-Sept-2025.csv"
    print(f"[ETL] Dự án: {project}\n[ETL] CSV gốc: {source}")
    raw = read_source(source)
    clean, rejects, report = transform(raw)
    write_prepared_files(project, clean, rejects, report)
    print("[ETL] Đối soát:", json.dumps({k: report[k] for k in [
        "source_rows", "out_of_scope_non_land", "out_of_scope_rental", "reject_rows", "clean_rows", "accounting_ok",
    ]}, ensure_ascii=False))
    if args.mode == "load":
        if not args.sql_server:
            parser.error("Chế độ --mode load cần --sql-server 'Tên\\Instance' ")
        import os
        load_sql(raw, clean, rejects, args.sql_server, args.sql_user, os.getenv("DW_SQL_PASSWORD"))
    else:
        print("[ETL] Chế độ prepare hoàn tất. Chưa kết nối hoặc ghi vào SQL Server.")


if __name__ == "__main__":
    main()
