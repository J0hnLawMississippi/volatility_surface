#!/usr/bin/env python3

import os
import pandas as pd
import numpy as np

def check_h5_file(file_path, key):
    """Check an .h5 file for data statistics and missing values, assuming it contains a Pandas DataFrame."""
    try:
        # Load the DataFrame from the .h5 file
        with pd.HDFStore(file_path, 'r') as store:
            data = store[key]  # Load the DataFrame using the provided key
        
        # Calculate statistics
        stats = {
            'num_rows': data.shape[0],
            'num_cols': data.shape[1],
            'columns': list(data.columns),
            'dtypes': {col: str(data[col].dtype) for col in data.columns},
            'missing_values': data.isna().sum().to_dict(),
            'min_values': data.min(numeric_only=True).to_dict(),
            'max_values': data.max(numeric_only=True).to_dict(),
            'mean_values': data.mean(numeric_only=True).to_dict(),
            'std_values': data.std(numeric_only=True).to_dict()
        }
        return stats
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

def process_folder(folder_path, output_csv):
    """Process all .h5 files in a folder and save results to a CSV."""
    results = []
    
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.h5'):
            file_path = os.path.join(folder_path, file_name)
            key = os.path.splitext(file_name)[0]  # Use the filename without .h5 as the key
            stats = check_h5_file(file_path, key)
            
            if stats:
                # Flatten the stats dictionary for CSV output
                for col in stats['columns']:
                    row = {
                        'file_name': file_name,
                        'column_name': col,
                        'num_rows': stats['num_rows'],
                        'num_cols': stats['num_cols'],
                        'dtype': stats['dtypes'][col],
                        'missing_values': stats['missing_values'][col],
                        'min_value': stats['min_values'].get(col, np.nan),
                        'max_value': stats['max_values'].get(col, np.nan),
                        'mean_value': stats['mean_values'].get(col, np.nan),
                        'std_value': stats['std_values'].get(col, np.nan)
                    }
                    results.append(row)
    
    # Convert results to a DataFrame and save to CSV
    df = pd.DataFrame(results)
    df.to_csv(output_csv, index=False)
    print(f"Results saved to {output_csv}")


# Example usage
folder_path = '/home/trademachine/optionbot/vol_surf'  # Replace with your folder path
output_csv = 'h5_file_stats.csv'
process_folder(folder_path, output_csv)
