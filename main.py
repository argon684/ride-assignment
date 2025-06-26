

from assigner import (
    load_and_preprocess_rides,
    load_capacity_matrix,
    load_constraints,
    #load_company_zip_permissions,
    assign_all_rides
)

# === File paths ===
rides_csv_path = "sample-data.csv"
capacity_csv_path = "company-limitations.csv"
constraint_csv_path = "exclusions.csv"
zip_permission_path = "company-locations.csv"
output_csv_path = "assigned-rides-output.csv"

# === Load input files ===
rides_df = load_and_preprocess_rides(rides_csv_path)
capacity_df = load_capacity_matrix(capacity_csv_path)
excluded_ids, door_only_ids = load_constraints(constraint_csv_path)
company_zip_map = load_company_zip_permissions(zip_permission_path)

# === Assign rides ===
assigned_df = assign_all_rides(
    rides_df, capacity_df, excluded_ids, door_only_ids, company_zip_map
)

# === Output result ===
assigned_df.to_csv(output_csv_path, index=False)
print(f"Assigned {len(assigned_df)} rides. Output saved to: {output_csv_path}")
