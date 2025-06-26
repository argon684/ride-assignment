from flask import Flask, render_template, request, send_file
import pandas as pd
import re
import os
from assigner import (
    load_and_preprocess_rides,
    load_capacity_matrix,
    load_constraints,
    load_company_city_permissions,
    load_excluded_comments_by_ride_type,
    assign_all_rides
)

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'output'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/assign', methods=['POST'])
def assign():
    # Get uploaded ride file
    ride_file = request.files['ride_csv']
    ride_path = os.path.join(UPLOAD_FOLDER, 'uploaded_rides.csv')
    ride_file.save(ride_path)

    # Load static constraint files
    capacity_path = 'company-limitations.csv'
    exclusions_path = 'exclusions.csv'
    #zip_permission_path = 'company-locations.csv'
    comments_path = 'excluded-comments.csv'

    # Load data
    rides_df = load_and_preprocess_rides(ride_path)
    capacity_df = load_capacity_matrix(capacity_path)
    excluded_ids, door_only_ids = load_constraints(exclusions_path)
    company_city_map = load_company_city_permissions("pickup-cities.csv")


    comment_exclusions = load_excluded_comments_by_ride_type(comments_path)

    comment_df = pd.read_csv("excluded-comments.csv", skiprows=1)
    comment_excludes = {}
    for col in comment_df.columns:
        ride_type = col.lower().replace("exclude", "").strip()
        keywords = comment_df[col].dropna().astype(str).str.lower().str.strip().tolist()
        comment_excludes[ride_type] = set(keywords)


    # Assign rides
    assigned_df = assign_all_rides(
        rides_df, capacity_df, excluded_ids, door_only_ids, company_city_map, comment_exclusions
    )

    if 'pickup time' in assigned_df.columns:
        assigned_df = assigned_df.drop(columns=['pickup time'])

    # Save result
    output_path = os.path.join(OUTPUT_FOLDER, 'assigned-rides-output.csv')
    assigned_df.to_csv(output_path, index=False)

    return render_template('result.html', result_path='download')

@app.route('/download')
def download():
    path = os.path.join(OUTPUT_FOLDER, 'assigned-rides-output.csv')
    return send_file(path, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)


