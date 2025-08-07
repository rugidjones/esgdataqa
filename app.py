# ==============================================================================
# File: app.py (Python Backend)
# ==============================================================================
# This is the core Python script that runs the data analysis as a web service.

import pandas as pd
import xlsxwriter
import warnings
import numpy as np
from datetime import datetime, timedelta
import os
import io
from flask import Flask, request, jsonify, send_file
import sys
import subprocess

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.filterwarnings("ignore", "invalid value encountered in subtract", RuntimeWarning)

# We need to ensure openpyxl and xlsxwriter are available in the deployment environment
try:
    import openpyxl
except ImportError:
    print("openpyxl not found, attempting to install...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl"])
    import openpyxl

try:
    import xlsxwriter
except ImportError:
    print("xlsxwriter not found, attempting to install...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "xlsxwriter"])
    import xlsxwriter

app = Flask(__name__)

# The false positive list is now handled by the upload, so we don't need a file on the server.
FALSE_POSITIVE_LIST = []

def get_false_positive_list(fp_file):
    """
    Reads the false positive list from an uploaded text file.
    """
    try:
        # Decode the file contents and read line by line
        fp_list = [int(line.strip()) for line in fp_file.read().decode('utf-8').splitlines() if line.strip()]
        return fp_list
    except Exception as e:
        print(f"Error reading false positive file: {e}")
        return []

def analyze_data_core(df, fp_list):
    """
    Performs the core data analysis logic and returns a BytesIO object
    containing the multi-tabbed Excel file.
    """
    try:
        print("Starting data analysis...")
        
        # -------------------- Your original logic starts here --------------------

        # --- Renaming columns ---
        column_mapping = {
            'Property Name': 'Property Name', 'Conservice Id': 'Conservice ID or Yoda Prop Code',
            'Location Bill Id': 'Location Bill ID', 'Account Number': 'Account Number',
            'Control Number': 'Control Number', 'Legal Vendor Name': 'Provider Name',
            'Service Type': 'Utility', 'Meter Number': 'Meter Number',
            'Add\'l Meter Name': 'Unique Meter ID', 'Start Date': 'Start Date',
            'End Date': 'End Date', 'Use': 'Usage', 'Cost': 'Cost',
            'Documentation': 'Document'
        }
        df.rename(columns=column_mapping, inplace=True)

        if 'Account Number' in df.columns:
            initial_rows = len(df)
            df = df[df['Account Number'].astype(str) != '~NA~'].copy()
            filtered_rows = initial_rows - len(df)
            if filtered_rows > 0:
                print(f"   - Filtered out {filtered_rows} rows with '~NA~' in 'Account Number'.")

        essential_columns = ['Meter Number', 'Start Date', 'End Date', 'Usage', 'Cost', 'Service Address']
        missing_columns = [col for col in essential_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing essential columns: {', '.join(missing_columns)}")

        for col in ['Gross Square Footage', 'Common Area SF']:
            if col not in df.columns:
                print(f"Warning: '{col}' column not found in source file.")

        df['Start Date'] = pd.to_datetime(df['Start Date'])
        df['End Date'] = pd.to_datetime(df['End Date'])

        if 'Created Date' in df.columns:
            df['Created Date'] = pd.to_datetime(df['Created Date'])
        if 'Last Modified Date' in df.columns:
            df['Last Modified Date'] = pd.to_datetime(df['Last Modified Date'])
        else:
            df['Last Modified Date'] = pd.NaT

        if 'Sold' in df.columns:
            df['Sold'] = pd.to_datetime(df['Sold'], errors='coerce')
        else:
            df['Sold'] = pd.NaT
            print("Warning: 'Sold' column not found in source file. Skipping 'Bills After Sale Date' check.")

        df['Usage'] = pd.to_numeric(df['Usage'], errors='coerce')
        df['Cost'] = pd.to_numeric(df['Cost'], errors='coerce')
        df = df.dropna(subset=['Usage', 'Cost'])
        df = df.sort_values(by=['Meter Number', 'Start Date'])

        def clean_text(val):
            if pd.isna(val): return 'MISSING_VALUE_FOR_DUPLICATE_CHECK'
            if isinstance(val, str):
                return (val.strip().lower().replace('\xa0', ' ').replace('\u200b', '').replace('\n', ' ').replace('\t', ' ').strip())
            return val

        duplicate_subset = ['Meter Number', 'Start Date', 'End Date', 'Usage', 'Cost', 'Service Address']
        df_clean = df.copy()
        for col in duplicate_subset:
            if col in df_clean.columns:
                if df_clean[col].dtype == 'object':
                    df_clean[col] = df_clean[col].apply(clean_text)
                elif pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                    df_clean[col] = df_clean[col].dt.floor('D')
                elif pd.api.types.is_numeric_dtype(df_clean[col]):
                    df_clean[col] = np.round(df_clean[col], 3)
        df_clean[duplicate_subset] = df_clean[duplicate_subset].fillna('MISSING_VALUE_FOR_DUPLICATE_CHECK')
        actual_duplicate_subset = [col for col in duplicate_subset if col in df_clean.columns]

        if actual_duplicate_subset:
            df['Duplicate'] = df_clean.duplicated(subset=actual_duplicate_subset, keep=False)
        else:
            df['Duplicate'] = False
            print("Warning: No valid columns found for duplicate detection. 'Duplicate' column set to False for all rows.")

        usage_mean = df['Usage'].dropna().mean(); usage_std = df['Usage'].dropna().std()
        df['Usage Z Score'] = (df['Usage'] - usage_mean) / usage_std if usage_std != 0 else np.nan
        cost_mean = df['Cost'].dropna().mean(); cost_std = df['Cost'].dropna().std()
        df['Cost Z Score'] = (df['Cost'] - cost_mean) / cost_std if cost_std != 0 else np.nan
        df['Usage MEAN'], df['Usage Standard'] = usage_mean, usage_std
        df['Cost Mean'], df['Cost Standard'] = cost_mean, cost_std
        df['Use_color'] = ''; df.loc[df['Usage Z Score'].abs() > 3.0, 'Use_color'] = 'red'
        df.loc[(df['Usage Z Score'] < 0) & (df['Usage Z Score'].abs() <= 3.0), 'Use_color'] = 'yellow'
        df['Cost_color'] = ''; df.loc[df['Cost Z Score'].abs() > 3.0, 'Cost_color'] = 'red'
        df.loc[(df['Cost Z Score'] < 0) & (df['Cost Z Score'].abs() <= 3.0), 'Cost_color'] = 'yellow'

        df['Gap'] = False; df['Gap_Dates'] = ''
        for meter_number in df['Meter Number'].unique():
            meter_data = df[df['Meter Number'] == meter_number].sort_values('Start Date')
            for i in range(1, len(meter_data)):
                previous_end = meter_data.iloc[i-1]['End Date']
                current_start = meter_data.iloc[i]['Start Date']
                if current_start > previous_end + pd.Timedelta(days=1):
                    df.loc[meter_data.index[i-1:i+1], 'Gap'] = True
                    df.loc[meter_data.index[i], 'Gap_Dates'] = f"{previous_end.date()} to {current_start.date()}"

        inactive_cutoff_date = pd.Timestamp(datetime.today() - timedelta(days=60))
        last_dates = df.groupby('Meter Number')['End Date'].max()
        inactive_meters = last_dates[last_dates < inactive_cutoff_date].index
        df['Meter_Inactive'] = df['Meter Number'].isin(inactive_meters)

        no_recent_data_cutoff = pd.Timestamp(datetime.today() - timedelta(days=90)) 
        latest_end_dates_per_meter = df.groupby('Meter Number')['End Date'].max()
        stale_data_meters = latest_end_dates_per_meter[latest_end_dates_per_meter < no_recent_data_cutoff].index
        df['No_Recent_Data_Flag'] = df['Meter Number'].isin(stale_data_meters)

        if 'Gross Square Footage' in df.columns:
            df['Cost_per_SF'] = df['Cost'] / df['Gross Square Footage']
            df['Usage_per_SF'] = df['Usage'] / df['Gross Square Footage']
            df['Gross Square Footage'] = pd.to_numeric(df['Gross Square Footage'], errors='coerce').replace(0, np.nan)
            
            cost_sf_mean, cost_sf_std = df['Cost_per_SF'].dropna().mean(), df['Cost_per_SF'].dropna().std()
            df['Cost_per_SF_zscore'] = (df['Cost_per_SF'] - cost_sf_mean) / cost_sf_std if cost_sf_std != 0 else np.nan
            usage_sf_mean, usage_sf_std = df['Usage_per_SF'].dropna().mean(), df['Usage_per_SF'].dropna().std()
            df['Usage_per_SF_zscore'] = (df['Usage_per_SF'] - usage_sf_mean) / usage_sf_std if usage_sf_std != 0 else np.nan
            
            df['Inspect_Cost_per_SF'] = ''; df.loc[df['Cost_per_SF_zscore'].abs() > 3.0, 'Inspect_Cost_per_SF'] = 'red'
            df['Inspect_Usage_per_SF'] = ''; df.loc[df['Usage_per_SF_zscore'].abs() > 3.0, 'Inspect_Usage_per_SF'] = 'red'
        else:
            df['Cost_per_SF'] = np.nan; df['Usage_per_SF'] = np.nan
            df['Cost_per_SF_zscore'] = np.nan; df['Usage_per_SF_zscore'] = np.nan
            df['Inspect_Cost_per_SF'] = ''; df['Inspect_Usage_per_SF'] = ''
            
        df = df.replace([np.inf, -np.inf], np.nan)

        df['Rate'] = df['Cost'] / df['Usage']; df['Rate'] = df['Rate'].replace([np.inf, -np.inf], np.nan)
        rate_mean = df['Rate'].dropna().mean(); rate_std = df['Rate'].dropna().std()
        df['Rate Z Score'] = (df['Rate'] - rate_mean) / rate_std if rate_std != 0 else np.nan
        df['Inspect_Rate'] = ''; df.loc[df['Rate Z Score'].abs() > 3.0, 'Inspect_Rate'] = 'red'

        if 'Created Date' in df.columns and 'Last Modified Date' in df.columns:
            df['Recent_Modification'] = (df['Created Date'] == df['Last Modified Date'])
        else:
            df['Recent_Modification'] = False

        df['Use_Zero_Cost_NonZero'] = (df['Usage'] == 0) & (df['Cost'] != 0)

        if 'HCF' in df.columns and df['HCF'].notna().any():
            df['HCF'] = pd.to_numeric(df['HCF'], errors='coerce')
            df['HCF_to_Gallons'] = df['HCF'] * 748
            df['HCF_Conversion_Match'] = (df['Usage'] - df['HCF_to_Gallons']).abs() <= 100
        else:
            df['HCF_to_Gallons'] = np.nan; df['HCF_Conversion_Match'] = np.nan

        df['Zero_Between_Positive'] = False
        for meter_number in df['Meter Number'].unique():
            meter_data = df[df['Meter Number'] == meter_number].sort_values('Start Date').reset_index()
            for i in range(1, len(meter_data) - 1):
                if meter_data.loc[i-1, 'Usage'] > 0 and meter_data.loc[i, 'Usage'] == 0 and meter_data.loc[i+1, 'Usage'] > 0 and meter_data.loc[i, 'Start Date'] > meter_data.loc[i-1, 'End Date']:
                    idxs = [meter_data.loc[i - 1, 'index'], meter_data.loc[i, 'index'], meter_data.loc[i + 1, 'index']]
                    df.loc[idxs, 'Zero_Between_Positive'] = True
            
        df['Is_Anomaly'] = (df['Usage Z Score'].abs() > 3.0) | (df['Cost Z Score'].abs() > 3.0) | (df['Usage'] == 0)
        df['Consecutive_Anomalies_Count'] = df.groupby('Meter Number')['Is_Anomaly'].transform(
            lambda x: x.mask(~x).groupby((x != x.shift()).cumsum()).cumcount() + 1
        ).fillna(0).astype(int)
        df['Consistently_Anomalous_Meter'] = df['Consecutive_Anomalies_Count'] >= 2
        df.drop(columns=['Is_Anomaly'], errors='ignore', inplace=True)

        df['Negative_Usage'] = df['Usage'] < 0
        df['Zero_Cost_Positive_Usage'] = (df['Cost'] == 0) & (df['Usage'] > 0)

        RECENTLY_UPDATED_DAYS_THRESHOLD = 30
        if 'Last Modified Date' in df.columns:
            df['Recently_Updated'] = (df['Last Modified Date'] > (datetime.today() - timedelta(days=RECENTLY_UPDATED_DAYS_THRESHOLD)))
        else:
            df['Recently_Updated'] = False

        RECENTLY_CREATED_DAYS_THRESHOLD = 30
        if 'Created Date' in df.columns:
            df['Recently_Created'] = (df['Created Date'] > (datetime.today() - timedelta(days=RECENTLY_CREATED_DAYS_THRESHOLD)))
        else:
            df['Recently_Created'] = False

        df['New_Bill_Usage_Anomaly'] = (df['Recently_Created'] == True) & (df['Usage Z Score'].abs() > 3.0)
        df['Bill_After_Sold_Date'] = False
        if 'Sold' in df.columns and not df['Sold'].isnull().all():
            valid_dates_mask = df['End Date'].notna() & df['Sold'].notna()
            df.loc[valid_dates_mask, 'Bill_After_Sold_Date'] = df.loc[valid_dates_mask, 'End Date'] > df.loc[valid_dates_mask, 'Sold']

        df['is_false_positive'] = False
        df.loc[df['Location Bill ID'].isin(fp_list), 'is_false_positive'] = True

        core_identifying_columns = [
            'Property Name', 'Location Bill ID', 'Control Number', 'Conservice ID or Yoda Prop Code', 'Provider Name',
            'Utility', 'Account Number', 'Meter Number', 'Unique Meter ID', 'Start Date', 'End Date',
            'Usage', 'Cost', 'Service Address', 'Document'
        ]
        primary_flags = [
            'Duplicate', 'Gap', 'Gap_Dates', 'Consecutive_Anomalies_Count', 'Consistently_Anomalous_Meter',
            'Inspect_Usage_per_SF', 'Inspect_Rate', 'Recent_Modification', 'Recently_Updated', 'Recently_Created',
            'Use_Zero_Cost_NonZero', 'Negative_Usage', 'Zero_Cost_Positive_Usage', 'Bill_After_Sold_Date',
            'New_Bill_Usage_Anomaly', 'Meter_Inactive', 'No_Recent_Data_Flag', 'HCF_Conversion_Match',
            'is_false_positive', 'Use_color', 'Zero_Between_Positive'
        ]
        calculated_statistical_columns = [
            'Rate', 'Billing_Period_Days', 'Usage MEAN', 'Usage Standard', 'Usage Z Score', 'Rate Z Score',
            'Gross Square Footage', 'Common Area SF', 'Created Date', 'Last Modified Date', 'Area Covered', 'Sold',
            'Usage_per_SF', 'Usage_per_SF_zscore', 'HCF', 'HCF_to_Gallons',
            'Cost Mean', 'Cost Standard', 'Cost Z Score', 'Cost_per_SF', 'Cost_per_SF_zscore', 'Inspect_Cost_per_SF', 'Cost_color'
        ]
        master_column_order = core_identifying_columns + primary_flags + calculated_statistical_columns
        df = df.reindex(columns=master_column_order, fill_value=np.nan)
        
        df_filtered_for_tabs = df[df['is_false_positive'] == False].copy()

        # The rest of the Excel writing logic is the same...
        # ...
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Sheet1', index=False)
            workbook = writer.book; worksheet = writer.sheets['Sheet1']
            green_format = workbook.add_format({'bg_color': '#C6EFCE'}); light_red_format = workbook.add_format({'bg_color': '#FFCCCC'}); blue_format = workbook.add_format({'bg_color': 'blue'})

            for row_num, (_, row) in enumerate(df.iterrows(), start=1):
                if row.get('Zero_Between_Positive', False): worksheet.set_row(row_num, None, green_format)
                if row.get('No_Recent_Data_Flag', False):
                    meter_col_idx = df.columns.get_loc('Meter Number')
                    worksheet.write(row_num, meter_col_idx, row['Meter Number'], light_red_format)
            
            usage_col_idx = df.columns.get_loc('Usage') if 'Usage' in df.columns else -1
            cost_col_idx = df.columns.get_loc('Cost') if 'Cost' in df.columns else -1
            if usage_col_idx != -1:
                worksheet.conditional_format(1, usage_col_idx, len(df), usage_col_idx, {'type': 'cell', 'criteria': '==', 'value': 0, 'format': blue_format})
            if cost_col_idx != -1:
                worksheet.conditional_format(1, cost_col_idx, len(df), cost_col_idx, {'type': 'cell', 'criteria': '>', 'value': 0, 'format': blue_format})
            worksheet.autofit()

            specific_anomaly_tabs = {
                'Recently Modified Bills': df_filtered_for_tabs[(df_filtered_for_tabs['Recently_Updated'] == True)].copy(),
                'High Value Anomalies': df_filtered_for_tabs[((df_filtered_for_tabs['Usage Z Score'].abs() > 3.0) | (df_filtered_for_tabs['Inspect_Usage_per_SF'] == 'red'))].copy(),
                'Negative Usage Records': df_filtered_for_tabs[(df_filtered_for_tabs['Negative_Usage'] == True)].copy(),
                'Rate Anomalies': df_filtered_for_tabs[(df_filtered_for_tabs['Inspect_Rate'] == 'red')].copy(),
                'Zero Cost Positive Usage': df_filtered_for_tabs[(df_filtered_for_tabs['Zero_Cost_Positive_Usage'] == True)].copy(),
                'Bills After Sale Date': df_filtered_for_tabs[(df_filtered_for_tabs['Bill_After_Sold_Date'] == True)].copy(),
                'Zero_Between_Positive': df_filtered_for_tabs[(df_filtered_for_tabs['Zero_Between_Positive'] == True)].copy(),
                'No Recent Data Meters': df_filtered_for_tabs[(df_filtered_for_tabs['No_Recent_Data_Flag'] == True)].copy(),
                'New Bill Anomalies': df_filtered_for_tabs[(df_filtered_for_tabs['New_Bill_Usage_Anomaly'] == True)].copy(),
                'Duplicate Records': df_filtered_for_tabs[(df_filtered_for_tabs['Duplicate'] == True)].copy(),
                'Gap Records': df_filtered_for_tabs[(df_filtered_for_tabs['Gap'] == True)].copy(),
            }
            if 'HCF_Conversion_Match' in df_filtered_for_tabs.columns and (df_filtered_for_tabs['HCF_Conversion_Match'] == False).any():
                specific_anomaly_tabs['HCF Mismatch'] = df_filtered_for_tabs[((df_filtered_for_tabs['HCF_Conversion_Match'] == False) & df_filtered_for_tabs['HCF'].notna())].copy()

            for tab_name, tab_df in specific_anomaly_tabs.items():
                if not tab_df.empty:
                    tab_df.to_excel(writer, sheet_name=tab_name, index=False)
                    writer.sheets[tab_name].autofit()
                    print(f"   - '{tab_name}' tab created with {len(tab_df)} records.")
                else:
                    print(f"   - '{tab_name}' tab not created: No records found.")
        
        output.seek(0)
        return output

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

@app.route('/', methods=['GET'])
def index():
    return send_file('index.html')

@app.route('/analyze', methods=['POST'])
def analyze():
    # Check for file uploads
    if 'raw_data_file' not in request.files:
        return jsonify({"error": "No raw data file provided"}), 400
    
    raw_data_file = request.files['raw_data_file']
    
    # Read the data file into a pandas DataFrame
    try:
        raw_data_stream = io.BytesIO(raw_data_file.read())
        df = pd.read_excel(raw_data_stream, sheet_name='Raw_Data_Table_S2', engine='openpyxl')
    except Exception as e:
        return jsonify({"error": f"Error reading raw data file: {e}"}), 400

    # Get the false positive list if a file was uploaded
    fp_file = request.files.get('fp_file')
    fp_list = []
    if fp_file and fp_file.filename != '':
        try:
            fp_list = [int(line.strip()) for line in fp_file.read().decode('utf-8').splitlines() if line.strip()]
        except Exception as e:
            return jsonify({"error": f"Error reading false positive file: {e}"}), 400

    # Perform the analysis
    output_stream = analyze_data_core(df, fp_list)
    
    if output_stream is None:
        return jsonify({"error": "Analysis failed. Please check your data."}), 500
    
    return send_file(output_stream, as_attachment=True, download_name='cleaned_data.xlsx')

if __name__ == '__main__':
    # For local development
    # Render will use gunicorn and a different entrypoint
    app.run(host='0.0.0.0', port=5000, debug=True)
```html
