from flask import Flask, request, jsonify
import pandas as pd
from googlesearch import search
import tempfile
import os

app = Flask(__name__)

def read_data(file_path):
    # Detect file extension and load data accordingly
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith(('.xls', '.xlsx')):
        return pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file format")

@app.route('/update_links', methods=['POST'])
def update_links():
    data = request.json
    file_path = data['file_path']
    keyword_column = data['keyword_column']
    
    # Load the data file
    try:
        df = read_data(file_path)
    except ValueError as e:
        return jsonify(success=False, message=str(e))
    
    # Add 'Updated Link' column if not present
    if 'Updated Link' not in df.columns:
        df['Updated Link'] = None
    
    # Iterate over each row in the dataframe to update links
    for index, row in df.iterrows():
        keyword = row[keyword_column]
        query = f"{keyword} site:example.com"  # Customize your query
        links = [j for j in search(query, num=3, stop=3)]
        
        if links:
            df.at[index, 'Updated Link'] = ', '.join(links)
    
    # Save the updated file
    temp_file_path = tempfile.mktemp(suffix=file_path[-4:])
    if file_path.endswith('.csv'):
        df.to_csv(temp_file_track, index=False)
    elif file_path.endswith(('.xls', '.xlsx')):
        df.to_excel(temp_file_path, index=False)

    os.replace(temp_file_path, file_path)  # Atomic operation

    return jsonify(success=True, message="Links updated successfully.")

if __name__ == "__main__":
    app.run(debug=True)
