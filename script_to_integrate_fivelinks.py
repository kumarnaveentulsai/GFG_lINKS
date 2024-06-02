import pandas as pd
import time
from googlesearch import search
import os
import tempfile

def update_links(file_path):
    # Load the CSV file
    data = pd.read_csv(file_path)
    
    # Display available columns and ask user for the column to consider
    print("Available columns in the file:", data.columns.tolist())
    keyword_column = input("Enter the name of the column containing keywords: ")
    
    # Check if 'Updated Link' column exists, if not add it
    if 'Updated Link' not in data.columns:
        data['Updated Link'] = None
    
    # Get desired output file name from the user at the start
    output_filename = input("Enter the desired filename for the updated data (with .csv extension): ")
    
    # User input for number of rows to process
    num_rows = input("Enter number of each_rowsto process or type 'all' for all rows: ")
    if num_rows.isdigit():
        num_rows = int(num_rows)
    else:
        num_rows = len(data)
    
    # Iterate over each row in the dataframe
    total_count = 0
    for index, row in data.iterrows():
        if total_count >= num_rows:
            break
        keyword = row[keyword_column]  # Use the user-specified column for keywords
        query = f"{keyword} gfg"  # Append 'gfg' to the keyword for specific search
        
        # Perform Google search and collect the first three links
        try:
            links = []
            for j in search(query, num=3, stop=3):  # Searching for the first three results
                links.append(j)
                if len(links) == 3:
                    break
            if links:
                data.at[index, 'Updated Link'] = ', '.join(links)
                print(f"Updated links for '{keyword}': {', '.join(links)}")
            time.sleep(16)  # Wait for 16 seconds before proceeding to the next search
        except Exception as e:
            print(f"An error occurred while searching: {e}")
        
        total_count += 1
        # Print status every 50 topics
        if total_count % 50 == 0:
            print(f"Total topics completed: {total_count}")
    
    # Save the data at the end of processing using atomic write for safe exit handling
    temp_dir = tempfile.gettempdir()
    temp_file_path = os.path.join(temp_dir, output_filename)
    data.to_csv(temp_file_path, index=False)
    os.replace(temp_file_path, output_filename)  # Atomic operation
    print(f"All updates are saved to the file: {output_filename}")

# Example usage
file_path = input("Enter the path to your CSV file: ")
update_links(file_path)  # Pass the path to the function

# Ensuring the directory exists before trying to save the file
clarified_script_path = "/mnt/data/clarified_updated_script.py"
os.makedirs(os.path.dirname(clarified_script_path), exist_ok=True)
with open(clarified_script_path, "w") as file:
    file.write(clarified_script)

