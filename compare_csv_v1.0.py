import pandas as pd
import sys

def compare_csv_files(old_csv, new_csv, output_csv, key_column):
    # Load the CSV files
    old_data = pd.read_csv(old_csv)
    new_data = pd.read_csv(new_csv)
    
    # Find missing entries
    old_keys = set(old_data[key_column])
    new_keys = set(new_data[key_column])
    
    missing_in_new = old_keys - new_keys
    missing_in_old = new_keys - old_keys
    
    # Extract mismatched entries
    common_keys = old_keys & new_keys
    mismatched_entries = []
    
    for key in common_keys:
        old_row = old_data[old_data[key_column] == key]
        new_row = new_data[new_data[key_column] == key]
        if not old_row.equals(new_row):
            mismatched_entries.append({'Key': key, 'Old Data': old_row.to_dict('records'), 'New Data': new_row.to_dict('records')})
    
    # Save results
    with open(output_csv, 'w') as f:
        f.write("Missing in New:\n")
        for key in missing_in_new:
            f.write(f"{key}\n")
        f.write("\nMissing in Old:\n")
        for key in missing_in_old:
            f.write(f"{key}\n")
        f.write("\nMismatched Entries:\n")
        for entry in mismatched_entries:
            f.write(f"{entry}\n")
    
    print(f"Comparison complete. Results saved to {output_csv}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python compare_csv.py <old_csv> <new_csv> <output_csv>")
        sys.exit(1)
    
    old_csv = sys.argv[1]
    new_csv = sys.argv[2]
    output_csv = sys.argv[3]
    
    # Specify the key column (adjust as needed)
    key_column = "Pokemon Name"  # Replace with the column name that uniquely identifies rows
    
    compare_csv_files(old_csv, new_csv, output_csv, key_column)
