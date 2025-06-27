# Assigner.py (clean patched version)
import pandas as pd
from dateutil.parser import parse
import re

# === Constants ===
SERVICE_TYPE_COMPANIES = {
    "curb to curb": ["SilverDC", "CURB", "Old Dominion", "Yellow Cab", "Action Cab",
                     "Silver Cab", "Regency", "RegencyPG"],
    "door to door (ambulatory only)": ["UZURV", "SilverRides", 'WeDriveU'],
    "door to door (wheelchair only)": ["Silver CAB WAV", "RegencyWAV"],
    "door to door (both)": ["UZURV Non-Dedicated", "Navarre Non-Dedicated", "KTS Non-Dedicated",
                            "WeDriveUDC", "DMV Transport",
                            "TransitGroup", "TransitGroupPG"],
    "wheelchair lift": ["Falcon", "SarahCarCare"]
}

# === Helpers ===
def classify_space_type(space_type: str, company_type: str) -> str:
    space_type = space_type.lower().strip()
    company_type = company_type.lower().strip()

    if company_type == "curb to curb":
        return "ambulatory" if space_type.startswith("am") else "none"
    elif company_type == "door to door (ambulatory only)":
        return "ambulatory" if space_type.startswith("am") else "none"
    elif company_type == "door to door (wheelchair only)":
        return "wheelchair" if space_type.startswith("wc") else "none"
    elif company_type == "door to door (both)":
        return "wheelchair" if space_type.startswith("wc") else (
            "ambulatory" if space_type.startswith("am") else "none"
        )
    elif company_type == "wheelchair lift":
        return "wheelchair" if space_type.startswith("wc") else "none"
    return "none"

def get_service_type_for_company(company_name):
    for service_type, companies in SERVICE_TYPE_COMPANIES.items():
        if company_name in companies:
            return service_type
    raise ValueError(f"Company '{company_name}' not found in mapping.")

def is_door_to_door(company):
    return any(
        key.startswith("door to door")
        for key, val in SERVICE_TYPE_COMPANIES.items()
        if company in val
    )

def load_excluded_comments_by_ride_type(filepath):
    df = pd.read_csv(filepath).fillna("")
    excluded_comments = {}
    for column in df.columns:
        ride_type = column.strip().lower()
        keywords = set(val.strip().lower() for val in df[column] if isinstance(val, str) and val.strip())
        excluded_comments[ride_type] = keywords
    return excluded_comments

# === Data Loaders ===
def load_and_preprocess_rides(filepath):
    df = pd.read_csv(filepath, encoding='latin1')
    df.columns = df.columns.str.lower().str.strip()

    # Now you can safely access 'drop_off_comments'
    if "drop_off_comments" not in df.columns:
        df["drop_off_comments"] = ""

    if "distance" not in df.columns:
        for col in df.columns:
            if "distance" in col:
                df.rename(columns={col: "distance"}, inplace=True)

    if 'neg_time' in df.columns:
        df['pickup time'] = pd.to_datetime(df['neg_time'].astype(str).str.strip(), format="%H:%M", errors="coerce")

    df["space type"] = df["_spacetype"].str.lower().str.strip()
    df["aid needed"] = df["mobaids"].fillna("none").str.lower().str.strip()
    df["customer id"] = df["trapeze client id"].astype(str).str.split(".").str[0]
    df["zip code"] = df["pickup zipcode"].astype(str).str.strip().str.split(".").str[0]
    df["pickup city"] = df["pickup city"].fillna("").str.strip().str.lower()

    df["distance"] = pd.to_numeric(df["distance"], errors='coerce')
    return df

def load_capacity_matrix(filepath):
    df = pd.read_csv(filepath, skiprows=1)
    df.columns = [str(c).strip() for c in df.columns]
    df.rename(columns={df.columns[0]: 'Company'}, inplace=True)
    for col in df.columns[2:]:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def load_constraints(csv_path):
    df = pd.read_csv(csv_path).fillna("")
    excluded_ids = set(str(i).split(".")[0] for i in df['Uber Exclude'].tolist() if i)
    door_only_ids = set(str(i).split(".")[0] for i in df['Door-to-Door Only'].tolist() if i)
    return excluded_ids, door_only_ids

def load_company_city_permissions(filepath):
    df = pd.read_csv(filepath, header=None)
    company_city_map = {}

    for i, row in df.iterrows():
        company = str(row[0]).strip()
        if not company:
            continue
        cities = set(
            str(city).strip().lower()
            for city in row.iloc[2:22]
            if pd.notna(city) and str(city).strip()
        )
        company_city_map[company] = cities

    return company_city_map



# === Filter Function ===
def filter_rides(df, company, hour_time, ride_count, assigned_ids,
                 excluded_ids, door_only_ids, company_zip_map,
                 comment_exclusions):
    print(f"\n--- Filtering for {company} at {hour_time.strftime('%H:%M')} ---")

    try:
        allowed_type = get_service_type_for_company(company)
    except ValueError as e:
        print(f"error: {e}")
        return pd.DataFrame()

    print(f"Allowed service type: {allowed_type}")
    allowed_cities = company_zip_map.get(company, set())
    print(f"Allowed zips or regions: {allowed_cities}")

    df = df[df["pickup time"].dt.hour == hour_time.hour].copy()
    print(f"Step 1: After time filter: {df.shape}")

    df = df[~df.apply(lambda row: (row["customer id"], hour_time.hour) in assigned_ids, axis=1)]
    print(f"Step 2: After assigned_ids filter: {df.shape}")

    def allow_uber_rider(row):
        if row["wants_uber"] != 1:
            return True
        if allowed_type == "curb to curb":
            if "am" in str(row["space type"]).lower() and pd.notna(row["distance"]) and row["distance"] <= 5:
                return True
        return False

    df = df[df.apply(allow_uber_rider, axis=1)]
    print(f"Step 3: After wants_uber filter: {df.shape}")

    df = df[~df["customer id"].isin(excluded_ids)]
    print(f"Step 4: After excluded_ids filter: {df.shape}")

    if not is_door_to_door(company):
        df = df[~df["customer id"].isin(door_only_ids)]
    print(f"Step 5: After door_only filter: {df.shape}")

    print(f"Space types seen: {df['space type'].unique().tolist()}")

    df = df[df.apply(lambda row: classify_space_type(row["space type"], allowed_type) != "none", axis=1)]
    print(f"Step 6: After space type filter: {df.shape}")

    excluded_aid_needs = {"scott bld", "birches", "dementia", "down syndrome", "father", "guard", "gaurd", "inova",
                          "iona", "metro access van only", "naval support", "no cell", "no number", "no phone",
                          'metro access only', "scotch building", "scott's building", "security", "soctt building", "macs van only", "macs only", 'macs vehicle only',
                          "metro access vehicles only", 'metro access vehicle only', 'requesting metro access only', 'request metro access only',
                          'requesting metro access van only', 'requesting metro access vehicle only', 'macs van only',
                          'request macs van only', 'request macs vehicle only', 'requesting macs only', 'requesting macs vehicle only', 'macs sedan only', 'request macs sedan only', 'requesting macs sedan only',
                          'request macs vans only', 'macs vans only', 'request macs sedan', 'requesting macs sedan', 'metro access w/ lift', 'metro access with lift',
                          'macs w/ lift', 'macs with lift', 'macs lift', "requests van for macs rides", '// metro access only', "// metro access vehicle only", '// macs only',
                          '// macs van only', '// metro access van only', '// metro access sedan only', '// macs sedan only', '//metro access only', '//macs only', '//metro access vehicle only',
                          '//metro access van only', '//macs van only', '//metro access sedan only', '//macs sedan only', 'metro access or uber only',
                          'macs vehicle or uber only', '//metro access or uber only', '//macs or uber only', 'only macs', 'requesting metro access vehicule',
                          '[macs only]', 'metro access vans only', 'door to door macs vehicle only', 'fe door to door macs vehicle only.', 'fe door to door macs vehicle only',
                          'macs w/lift van only', 'macs cars or sedans only', 'macs van or car only', 'metro access cars or sedans only',
                          'metro access van or car only', 'macs w/lift van only', 'van wiht lift', 'please metro access', 'metro access with the lift', 'macs ride only', 'metro access ride only', 'no ph.', 'F/E , NO CAB ', 'VN LFT - CAN', 'metro only pls',
                          'metro only', 'asking for metro access vehicle', 'no uber, metro access vehicle.', 'appointment time asking for a Metro Access transport', 'request metro access', 'f/e metro van lft',
                          'f/e/client request macs vehicle', 'metro van only', 'metro access car only', 'metro access car, or van only', 'metro access  car or van only',
                          'please send a metro access vehicle', 'send a metro access vehicle, only,'
                          }

    def has_disqualifying_aid(comment):
        comment = str(comment).lower()
        return any(keyword in comment for keyword in excluded_aid_needs)

    df = df[~df["pick-up comment"].fillna("").apply(has_disqualifying_aid)]
    df = df[~df["drop-off comments"].fillna("").apply(has_disqualifying_aid)]
    print(f"Step 7: After aid exclusion filter: {df.shape}")

    ride_keywords = comment_exclusions.get(allowed_type.lower(), set())

    def has_excluded_phrase(comment):
        if not isinstance(comment, str):
            return False
        comment = comment.lower()
        return any(phrase in comment for phrase in ride_keywords)

    df = df[~df["pick-up comment"].fillna("").apply(has_excluded_phrase)]
    df = df[~df["drop-off comments"].fillna("").apply(has_excluded_phrase)]
    print(f"Step 8: After ride-type comment exclusion filter: {df.shape}")

    def is_allowed_city(row):
        pickup_city = str(row.get("pickup city", "")).strip().lower()
        return pickup_city in allowed_cities

    df = df[df.apply(is_allowed_city, axis=1)]
    print(f"Step 9: After city filter: {df.shape}")

    if "distance" not in df.columns:
        print("Missing 'distance' column")
        return pd.DataFrame()
    df = df[df["distance"].notna()]

    max_distance = None
    if allowed_type == 'curb to curb':
        max_distance = 15
    elif allowed_type == 'door to door (ambulatory only)':
        max_distance = 20 if company in {'UZURV'} else 10

    elif allowed_type == 'door to door (wheelchair only)':
        max_distance = 20
    elif allowed_type == 'door to door (both)':
        max_distance = 15 if company in {'TransitGroup', 'TransitGroupPG'} else 10
    elif allowed_type == 'wheelchair lift':
        max_distance = 10

    if max_distance is not None:
        df = df[df["distance"] <= max_distance]
    print(f"Step 11: After max distance ({max_distance}) filter: {df.shape}")

    if allowed_type == "door to door (both)":
        wc_rides = df[df["space type"].str.contains("wc", na=False)]
        am_rides = df[~df.index.isin(wc_rides.index)]
        wc_rides = wc_rides.sort_values(by="distance", ascending=False)
        am_rides = am_rides.sort_values(by="distance", ascending=True)
        result = pd.concat([wc_rides, am_rides], ignore_index=True).head(ride_count)
    else:
        if allowed_type == "curb to curb":
            df = df.sort_values(by="distance", ascending=True)
        elif "wheelchair" in allowed_type:
            df = df.sort_values(by="distance", ascending=False)
        result = df.head(ride_count)

    print(f" Final eligible rides: {result.shape}")
    return result

# === Assignment Function ===
def assign_all_rides(rides_df, capacity_df, excluded_ids, door_only_ids, company_city_map, comment_exclusions):
    assigned_ids = set()
    assignments = []
    expected_columns = rides_df.columns.tolist() + ["assigned company"]

    capacity_df.rename(columns={capacity_df.columns[0]: "Company"}, inplace=True)
    print("=== Starting ride assignment ===")
    hour_pattern = re.compile(r"^\d{1,2}:\d{2}$")
    hour_columns = [col for col in capacity_df.columns if hour_pattern.match(str(col).strip())]

    for _, row in capacity_df.iterrows():
        company = str(row["Company"]).strip()
        if not company or company.lower().startswith("provider"):
            continue

        for hour_str in hour_columns:
            try:
                hour_dt = parse(hour_str.strip())
                ride_count = int(row[hour_str])
            except:
                continue
            if ride_count <= 0:
                continue

            eligible = filter_rides(
                rides_df, company, hour_dt, ride_count,
                assigned_ids, excluded_ids, door_only_ids,
                company_city_map, comment_exclusions
            )

            if eligible.empty:
                continue

            eligible = eligible.copy()
            eligible["assigned company"] = company
            assigned_ids.update((cid, hour_dt.hour) for cid in eligible["customer id"])
            assignments.append(eligible)

    if assignments:
        result = pd.concat(assignments, ignore_index=True)

        company_name_mapping = {
            "Old Dominion": "Old Dominion",
            "Yellow Cab": "Yellow Cab",
            "Action Cab": "Action Taxi",
            "CURB": "CURB",
            "Silver Cab": "Silver Cab",
            "Silver CAB WAV": "SilverWAV",
            "Regency": "Regency",
            "RegencyWAV": "RegencyWAV",
            "RegencyPG": "RegencyPG",
            "UZURV": "UZURV",
            "Navarre Non-Dedicated": "NAVARRE",
            "TransitGroup": "TransitGroup",
            "WeDriveU": "WeDriveU",
            "KTS Non-Dedicated": "KTSND",
            "WeDriveUDC": "WeDriveUDC",
            "DMV Transport": "DMV",
            "TransitGroupPG": "TransitGroupPG",
            "UZURV Non-Dedicated": "UZURV-ND",
            "SilverDC": "SilverDC"
        }

        result["assigned company"] = result["assigned company"].map(company_name_mapping).fillna(
            result["assigned company"])
        return result
    return pd.DataFrame(columns=expected_columns)

