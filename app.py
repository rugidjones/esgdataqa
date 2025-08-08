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
import logging

# Configure logging for production environment
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

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
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB upload limit

def get_false_positive_list(fp_file):
    """
    Reads the false positive list from an uploaded text file.
    """
    try:
        # Decode the file contents and read line by line
        fp_list = [int(line.strip()) for line in fp_file.read().decode('utf-8').splitlines() if line.strip()]
        return fp_list
    except Exception as e:
        logging.error(f"Error reading false positive file: {e}")
        return []

def analyze_data_and_generate_report(df_generator, fp_list):
    """
    Performs the core data analysis logic by processing chunks from a generator.
    Returns a BytesIO object containing the multi-tabbed Excel file.
    """
    print("Starting chunk-based analysis...")
    
    # Store all processed chunks and anomaly data in lists to be combined later
    processed_chunks = []
    
    # Dictionaries to hold data for each anomaly tab
    anomaly_data = {
        'Recently Modified Bills': [], 'High Value Anomalies': [], 'Negative Usage Records': [],
        'Rate Anomalies': [], 'Zero Cost Positive Usage': [], 'Bills After Sale Date': [],
        'Zero_Between_Positive': [], 'No Recent Data Meters': [], 'New Bill Anomalies': [],
        'HCF Mismatch': [], 'Duplicate Records': [], 'Gap Records': []
    }
    
    # Dictionaries to store meter-specific info for continuity between chunks
    meter_info = {}

    def process_chunk(chunk_df):
        # This function contains the core logic that was in analyze_data_core before
        
        # --- Renaming and cleaning ---
        column_mapping = {
            'Property Name': 'Property Name', 'Conservice Id': 'Conservice ID or Yoda Prop Code',
            'Location Bill Id': 'Location Bill ID', 'Account Number': 'Account Number',
            'Control Number': 'Control Number', 'Legal Vendor Name': 'Provider Name',
            'Service Type': 'Utility', 'Meter Number': 'Meter Number',
            'Add\'l Meter Name': 'Unique Meter ID', 'Start Date': 'Start Date',
            'End Date': 'End Date', 'Use': 'Usage', 'Cost': 'Cost',
            'Documentation': 'Document'
        }
        chunk_df.rename(columns=column_mapping, inplace=True)

        if 'Account Number' in chunk_df.columns:
            chunk_df = chunk_df[chunk_df['Account Number'].astype(str) != '~NA~'].copy()

        essential_columns = ['Meter Number', 'Start Date', 'End Date', 'Usage', 'Cost', 'Service Address']
        missing_columns = [col for col in essential_columns if col not in chunk_df.columns]
        if missing_columns:
            raise ValueError(f"Missing essential columns: {', '.join(missing_columns)}")

        for col in ['Gross Square Footage', 'Common Area SF']:
            if col not in chunk_df.columns:
                print(f"Warning: '{col}' column not found in source file.")

        chunk_df['Start Date'] = pd.to_datetime(chunk_df['Start Date'])
        chunk_df['End Date'] = pd.to_datetime(chunk_df['End Date'])

        if 'Created Date' in chunk_df.columns:
            chunk_df['Created Date'] = pd.to_datetime(chunk_df['Created Date'])
        if 'Last Modified Date' in chunk_df.columns:
            chunk_df['Last Modified Date'] = pd.to_datetime(chunk_df['Last Modified Date'])
        else:
            chunk_df['Last Modified Date'] = pd.NaT

        if 'Sold' in chunk_df.columns:
            chunk_df['Sold'] = pd.to_datetime(chunk_df['Sold'], errors='coerce')
        else:
            chunk_df['Sold'] = pd.NaT

        chunk_df['Usage'] = pd.to_numeric(chunk_df['Usage'], errors='coerce')
        chunk_df['Cost'] = pd.to_numeric(chunk_df['Cost'], errors='coerce')
        chunk_df = chunk_df.dropna(subset=['Usage', 'Cost'])
        chunk_df = chunk_df.sort_values(by=['Meter Number', 'Start Date'])

        def clean_text(val):
            if pd.isna(val): return 'MISSING_VALUE_FOR_DUPLICATE_CHECK'
            if isinstance(val, str):
                return (val.strip().lower().replace('\xa0', ' ').replace('\u200b', '').replace('\n', ' ').replace('\t', ' ').strip())
            return val

        duplicate_subset = ['Meter Number', 'Start Date', 'End Date', 'Usage', 'Cost', 'Service Address']
        df_clean = chunk_df.copy()
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
            chunk_df['Duplicate'] = df_clean.duplicated(subset=actual_duplicate_subset, keep=False)
        else:
            chunk_df['Duplicate'] = False

        usage_mean = chunk_df['Usage'].dropna().mean(); usage_std = chunk_df['Usage'].dropna().std()
        chunk_df['Usage Z Score'] = (chunk_df['Usage'] - usage_mean) / usage_std if usage_std != 0 else np.nan
        cost_mean = chunk_df['Cost'].dropna().mean(); cost_std = chunk_df['Cost'].dropna().std()
        chunk_df['Cost Z Score'] = (chunk_df['Cost'] - cost_mean) / cost_std if cost_std != 0 else np.nan
        chunk_df['Usage MEAN'], chunk_df['Usage Standard'] = usage_mean, usage_std
        chunk_df['Cost Mean'], chunk_df['Cost Standard'] = cost_mean, cost_std
        chunk_df['Use_color'] = ''; chunk_df.loc[chunk_df['Usage Z Score'].abs() > 3.0, 'Use_color'] = 'red'
        chunk_df.loc[(chunk_df['Usage Z Score'] < 0) & (chunk_df['Usage Z Score'].abs() <= 3.0), 'Use_color'] = 'yellow'
        chunk_df['Cost_color'] = ''; chunk_df.loc[chunk_df['Cost Z Score'].abs() > 3.0, 'Cost_color'] = 'red'
        chunk_df.loc[(chunk_df['Cost Z Score'] < 0) & (chunk_df['Cost Z Score'].abs() <= 3.0), 'Cost_color'] = 'yellow'

        chunk_df['Gap'] = False; chunk_df['Gap_Dates'] = ''
        for meter_number in chunk_df['Meter Number'].unique():
            meter_data = chunk_df[chunk_df['Meter Number'] == meter_number].sort_values('Start Date')
            for i in range(1, len(meter_data)):
                previous_end = meter_data.iloc[i-1]['End Date']
                current_start = meter_data.iloc[i]['Start Date']
                if current_start > previous_end + pd.Timedelta(days=1):
                    chunk_df.loc[meter_data.index[i-1:i+1], 'Gap'] = True
                    chunk_df.loc[meter_data.index[i], 'Gap_Dates'] = f"{previous_end.date()} to {current_start.date()}"

        inactive_cutoff_date = pd.Timestamp(datetime.today() - timedelta(days=60))
        last_dates = chunk_df.groupby('Meter Number')['End Date'].max()
        inactive_meters = last_dates[last_dates < inactive_cutoff_date].index
        chunk_df['Meter_Inactive'] = chunk_df['Meter Number'].isin(inactive_meters)

        no_recent_data_cutoff = pd.Timestamp(datetime.today() - timedelta(days=90)) 
        latest_end_dates_per_meter = chunk_df.groupby('Meter Number')['End Date'].max()
        stale_data_meters = latest_end_dates_per_meter[latest_end_dates_per_meter < no_recent_data_cutoff].index
        chunk_df['No_Recent_Data_Flag'] = chunk_df['Meter Number'].isin(stale_data_meters)

        if 'Gross Square Footage' in chunk_df.columns:
            chunk_df['Cost_per_SF'] = chunk_df['Cost'] / chunk_df['Gross Square Footage']
            chunk_df['Usage_per_SF'] = chunk_df['Usage'] / chunk_df['Gross Square Footage']
            chunk_df['Gross Square Footage'] = pd.to_numeric(chunk_df['Gross Square Footage'], errors='coerce').replace(0, np.nan)
            
            cost_sf_mean, cost_sf_std = chunk_df['Cost_per_SF'].dropna().mean(), chunk_df['Cost_per_SF'].dropna().std()
            chunk_df['Cost_per_SF_zscore'] = (chunk_df['Cost_per_SF'] - cost_sf_mean) / cost_sf_std if cost_sf_std != 0 else np.nan
            usage_sf_mean, usage_sf_std = chunk_df['Usage_per_SF'].dropna().mean(), chunk_df['Usage_per_SF'].dropna().std()
            chunk_df['Usage_per_SF_zscore'] = (chunk_df['Usage_per_SF'] - usage_sf_mean) / usage_sf_std if usage_sf_std != 0 else np.nan
            
            chunk_df['Inspect_Cost_per_SF'] = ''; chunk_df.loc[chunk_df['Cost_per_SF_zscore'].abs() > 3.0, 'Inspect_Cost_per_SF'] = 'red'
            chunk_df['Inspect_Usage_per_SF'] = ''; chunk_df.loc[chunk_df['Usage_per_SF_zscore'].abs() > 3.0, 'Inspect_Usage_per_SF'] = 'red'
        else:
            chunk_df['Cost_per_SF'] = np.nan; chunk_df['Usage_per_SF'] = np.nan
            chunk_df['Cost_per_SF_zscore'] = np.nan; chunk_df['Usage_per_SF_zscore'] = np.nan
            chunk_df['Inspect_Cost_per_SF'] = ''; chunk_df['Inspect_Usage_per_SF'] = ''
            
        chunk_df = chunk_df.replace([np.inf, -np.inf], np.nan)

        chunk_df['Rate'] = chunk_df['Cost'] / chunk_df['Usage']; chunk_df['Rate'] = chunk_df['Rate'].replace([np.inf, -np.inf], np.nan)
        rate_mean = chunk_df['Rate'].dropna().mean(); rate_std = chunk_df['Rate'].dropna().std()
        chunk_df['Rate Z Score'] = (chunk_df['Rate'] - rate_mean) / rate_std if rate_std != 0 else np.nan
        chunk_df['Inspect_Rate'] = ''; chunk_df.loc[chunk_df['Rate Z Score'].abs() > 3.0, 'Inspect_Rate'] = 'red'

        if 'Created Date' in chunk_df.columns and 'Last Modified Date' in chunk_df.columns:
            chunk_df['Recent_Modification'] = (chunk_df['Created Date'] == chunk_df['Last Modified Date'])
        else:
            chunk_df['Recent_Modification'] = False

        chunk_df['Use_Zero_Cost_NonZero'] = (chunk_df['Usage'] == 0) & (chunk_df['Cost'] != 0)

        if 'HCF' in chunk_df.columns and chunk_df['HCF'].notna().any():
            chunk_df['HCF'] = pd.to_numeric(chunk_df['HCF'], errors='coerce')
            chunk_df['HCF_to_Gallons'] = chunk_df['HCF'] * 748
            chunk_df['HCF_Conversion_Match'] = (chunk_df['Usage'] - chunk_df['HCF_to_Gallons']).abs() <= 100
        else:
            chunk_df['HCF_to_Gallons'] = np.nan; chunk_df['HCF_Conversion_Match'] = np.nan

        chunk_df['Zero_Between_Positive'] = False
        for meter_number in chunk_df['Meter Number'].unique():
            meter_data = chunk_df[chunk_df['Meter Number'] == meter_number].sort_values('Start Date').reset_index()
            for i in range(1, len(meter_data) - 1):
                if meter_data.loc[i-1, 'Usage'] > 0 and meter_data.loc[i, 'Usage'] == 0 and meter_data.loc[i+1, 'Usage'] > 0 and meter_data.loc[i, 'Start Date'] > meter_data.loc[i-1, 'End Date']:
                    idxs = [meter_data.loc[i - 1, 'index'], meter_data.loc[i, 'index'], meter_data.loc[i + 1, 'index']]
                    chunk_df.loc[idxs, 'Zero_Between_Positive'] = True
            
        chunk_df['Is_Anomaly'] = (chunk_df['Usage Z Score'].abs() > 3.0) | (chunk_df['Cost Z Score'].abs() > 3.0) | (chunk_df['Usage'] == 0)
        chunk_df['Consecutive_Anomalies_Count'] = chunk_df.groupby('Meter Number')['Is_Anomaly'].transform(
            lambda x: x.mask(~x).groupby((x != x.shift()).cumsum()).cumcount() + 1
        ).fillna(0).astype(int)
        chunk_df['Consistently_Anomalous_Meter'] = chunk_df['Consecutive_Anomalies_Count'] >= 2
        chunk_df.drop(columns=['Is_Anomaly'], errors='ignore', inplace=True)

        chunk_df['Negative_Usage'] = chunk_df['Usage'] < 0
        chunk_df['Zero_Cost_Positive_Usage'] = (chunk_df['Cost'] == 0) & (chunk_df['Usage'] > 0)

        RECENTLY_UPDATED_DAYS_THRESHOLD = 30
        if 'Last Modified Date' in chunk_df.columns:
            chunk_df['Recently_Updated'] = (chunk_df['Last Modified Date'] > (datetime.today() - timedelta(days=RECENTLY_UPDATED_DAYS_THRESHOLD)))
        else:
            chunk_df['Recently_Updated'] = False

        RECENTLY_CREATED_DAYS_THRESHOLD = 30
        if 'Created Date' in chunk_df.columns:
            chunk_df['Recently_Created'] = (chunk_df['Created Date'] > (datetime.today() - timedelta(days=RECENTLY_CREATED_DAYS_THRESHOLD)))
        else:
            chunk_df['Recently_Created'] = False

        chunk_df['New_Bill_Usage_Anomaly'] = (chunk_df['Recently_Created'] == True) & (chunk_df['Usage Z Score'].abs() > 3.0)
        chunk_df['Bill_After_Sold_Date'] = False
        if 'Sold' in chunk_df.columns and not chunk_df['Sold'].isnull().all():
            valid_dates_mask = chunk_df['End Date'].notna() & chunk_df['Sold'].notna()
            chunk_df.loc[valid_dates_mask, 'Bill_After_Sold_Date'] = chunk_df.loc[valid_dates_mask, 'End Date'] > chunk_df.loc[valid_dates_mask, 'Sold']

        chunk_df['is_false_positive'] = False
        chunk_df.loc[chunk_df['Location Bill ID'].isin(fp_list), 'is_false_positive'] = True

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
        chunk_df = chunk_df.reindex(columns=master_column_order, fill_value=np.nan)
        
        chunk_df_filtered_for_tabs = chunk_df[chunk_df['is_false_positive'] == False].copy()

        # Collect anomaly data from the chunk
        if not chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['Recently_Updated'] == True].empty:
            anomaly_data['Recently Modified Bills'].append(chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['Recently_Updated'] == True])
        if not chunk_df_filtered_for_tabs[((chunk_df_filtered_for_tabs['Usage Z Score'].abs() > 3.0) | (chunk_df_filtered_for_tabs['Inspect_Usage_per_SF'] == 'red'))].empty:
            anomaly_data['High Value Anomalies'].append(chunk_df_filtered_for_tabs[((chunk_df_filtered_for_tabs['Usage Z Score'].abs() > 3.0) | (chunk_df_filtered_for_tabs['Inspect_Usage_per_SF'] == 'red'))])
        if not chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['Negative_Usage'] == True].empty:
            anomaly_data['Negative Usage Records'].append(chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['Negative_Usage'] == True])
        if not chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['Inspect_Rate'] == 'red'].empty:
            anomaly_data['Rate Anomalies'].append(chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['Inspect_Rate'] == 'red'])
        if not chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['Zero_Cost_Positive_Usage'] == True].empty:
            anomaly_data['Zero Cost Positive Usage'].append(chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['Zero_Cost_Positive_Usage'] == True])
        if not chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['Bill_After_Sold_Date'] == True].empty:
            anomaly_data['Bills After Sale Date'].append(chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['Bill_After_Sold_Date'] == True])
        if not chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['Zero_Between_Positive'] == True].empty:
            anomaly_data['Zero_Between_Positive'].append(chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['Zero_Between_Positive'] == True])
        if not chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['No_Recent_Data_Flag'] == True].empty:
            anomaly_data['No Recent Data Meters'].append(chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['No_Recent_Data_Flag'] == True])
        if not chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['New_Bill_Usage_Anomaly'] == True].empty:
            anomaly_data['New Bill Anomalies'].append(chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['New_Bill_Usage_Anomaly'] == True])
        if not chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['Duplicate'] == True].empty:
            anomaly_data['Duplicate Records'].append(chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['Duplicate'] == True])
        if not chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['Gap'] == True].empty:
            anomaly_data['Gap Records'].append(chunk_df_filtered_for_tabs[chunk_df_filtered_for_tabs['Gap'] == True])
        if 'HCF_Conversion_Match' in chunk_df_filtered_for_tabs.columns and not chunk_df_filtered_for_tabs[((chunk_df_filtered_for_tabs['HCF_Conversion_Match'] == False) & chunk_df_filtered_for_tabs['HCF'].notna())].empty:
            anomaly_data['HCF Mismatch'].append(chunk_df_filtered_for_tabs[((chunk_df_filtered_for_tabs['HCF_Conversion_Match'] == False) & chunk_df_filtered_for_tabs['HCF'].notna())])
            
        processed_chunks.append(chunk_df)

        return processed_chunks, anomaly_data
    except Exception as e:
        print(f"An error occurred in a chunk: {e}")
        return None, None

@app.route('/', methods=['GET'])
def index():
    return send_file('index.html')

@app.route('/analyze', methods=['POST'])
def analyze():
    # Check for file uploads
    if 'raw_data_file' not in request.files:
        return jsonify({"error": "No raw data file provided"}), 400
    
    raw_data_file = request.files['raw_data_file']
    
    try:
        # Read the Excel file in chunks
        excel_file = pd.ExcelFile(raw_data_file, engine='openpyxl')
        sheet_names = [name for name in excel_file.sheet_names if "raw_data_table" in name.lower()]
        if not sheet_names:
            return jsonify({"error": "No sheet named 'Raw_Data_Table' found. Please check your sheet names."}), 400
        sheet_name = sheet_names[0]
        
        # This generator will yield chunks of the dataframe
        df_generator = pd.read_excel(raw_data_file, sheet_name=sheet_name, chunksize=1000, engine='openpyxl')

        # Get the false positive list if a file was uploaded
        fp_file = request.files.get('fp_file')
        fp_list = []
        if fp_file and fp_file.filename != '':
            fp_list = get_false_positive_list(fp_file)

        # Process each chunk
        processed_chunks = []
        anomaly_data = {
            'Recently Modified Bills': [], 'High Value Anomalies': [], 'Negative Usage Records': [],
            'Rate Anomalies': [], 'Zero Cost Positive Usage': [], 'Bills After Sale Date': [],
            'Zero_Between_Positive': [], 'No Recent Data Meters': [], 'New Bill Anomalies': [],
            'HCF Mismatch': [], 'Duplicate Records': [], 'Gap Records': []
        }

        # A flag to track if any error occurred during chunk processing
        analysis_successful = True

        for chunk_df in df_generator:
            try:
                # Apply the core logic to the current chunk
                # Note: df_generator is already a DataFrame
                chunk_df_processed = chunk_df.copy() # Avoid modifying the chunk directly
                
                # --- APPLY CORE LOGIC (as in the analyze_data_core function before) ---
                
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
                chunk_df_processed.rename(columns=column_mapping, inplace=True)

                if 'Account Number' in chunk_df_processed.columns:
                    chunk_df_processed = chunk_df_processed[chunk_df_processed['Account Number'].astype(str) != '~NA~'].copy()

                essential_columns = ['Meter Number', 'Start Date', 'End Date', 'Usage', 'Cost', 'Service Address']
                missing_columns = [col for col in essential_columns if col not in chunk_df_processed.columns]
                if missing_columns:
                    raise ValueError(f"Missing essential columns: {', '.join(missing_columns)}")

                for col in ['Gross Square Footage', 'Common Area SF']:
                    if col not in chunk_df_processed.columns:
                        logging.warning(f"'{col}' column not found in source file.")

                chunk_df_processed['Start Date'] = pd.to_datetime(chunk_df_processed['Start Date'])
                chunk_df_processed['End Date'] = pd.to_datetime(chunk_df_processed['End Date'])

                if 'Created Date' in chunk_df_processed.columns:
                    chunk_df_processed['Created Date'] = pd.to_datetime(chunk_df_processed['Created Date'])
                if 'Last Modified Date' in chunk_df_processed.columns:
                    chunk_df_processed['Last Modified Date'] = pd.to_datetime(chunk_df_processed['Last Modified Date'])
                else:
                    chunk_df_processed['Last Modified Date'] = pd.NaT

                if 'Sold' in chunk_df_processed.columns:
                    chunk_df_processed['Sold'] = pd.to_datetime(chunk_df_processed['Sold'], errors='coerce')
                else:
                    chunk_df_processed['Sold'] = pd.NaT

                chunk_df_processed['Usage'] = pd.to_numeric(chunk_df_processed['Usage'], errors='coerce')
                chunk_df_processed['Cost'] = pd.to_numeric(chunk_df_processed['Cost'], errors='coerce')
                chunk_df_processed = chunk_df_processed.dropna(subset=['Usage', 'Cost'])
                chunk_df_processed = chunk_df_processed.sort_values(by=['Meter Number', 'Start Date'])
                
                # All other processing logic, including Z-scores, flags, etc.
                # This needs to be applied to the chunk_df_processed

                # ... (All of your data processing logic, moved here) ...

                # Now, collect anomaly data from this chunk
                df_filtered_for_tabs = chunk_df_processed[chunk_df_processed['is_false_positive'] == False].copy()

                for tab_name, data_list in anomaly_data.items():
                    if tab_name == 'Recently Modified Bills' and not df_filtered_for_tabs[(df_filtered_for_tabs['Recently_Updated'] == True)].empty:
                        data_list.append(df_filtered_for_tabs[(df_filtered_for_tabs['Recently_Updated'] == True)].copy())
                    elif tab_name == 'High Value Anomalies' and not df_filtered_for_tabs[((df_filtered_for_tabs['Usage Z Score'].abs() > 3.0) | (df_filtered_for_tabs['Inspect_Usage_per_SF'] == 'red'))].empty:
                         data_list.append(df_filtered_for_tabs[((df_filtered_for_tabs['Usage Z Score'].abs() > 3.0) | (df_filtered_for_tabs['Inspect_Usage_per_SF'] == 'red'))].copy())
                    # ... add similar logic for all other tabs ...

                processed_chunks.append(chunk_df_processed)

            except Exception as e:
                logging.error(f"Error processing a data chunk: {e}")
                analysis_successful = False
                break

        if not analysis_successful:
            return None

        # Concatenate all processed chunks and anomaly dataframes
        final_df = pd.concat(processed_chunks, ignore_index=True)
        final_anomaly_data = {
            name: pd.concat(data_list, ignore_index=True) if data_list else pd.DataFrame()
            for name, data_list in anomaly_data.items()
        }

        # Save to output stream
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, sheet_name='Sheet1', index=False)
            for name, df_tab in final_anomaly_data.items():
                if not df_tab.empty:
                    df_tab.to_excel(writer, sheet_name=name, index=False)

        output.seek(0)
        return output
    except Exception as e:
        logging.error(f"Analysis error: {e}")
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
        excel_file = pd.ExcelFile(raw_data_file, engine='openpyxl')
        sheet_names = [name for name in excel_file.sheet_names if "raw_data_table" in name.lower()]
        
        if not sheet_names:
            return jsonify({"error": "No sheet named 'Raw_Data_Table' found. Please check your sheet names."}), 400
        
        sheet_name = sheet_names[0]
        # Read the Excel file in chunks
        df_generator = pd.read_excel(raw_data_file, sheet_name=sheet_name, chunksize=5000, engine='openpyxl')

    except Exception as e:
        return jsonify({"error": f"Error reading raw data file: {e}"}), 400

    fp_file = request.files.get('fp_file')
    fp_list = []
    if fp_file and fp_file.filename != '':
        try:
            fp_list = [int(line.strip()) for line in fp_file.read().decode('utf-8').splitlines() if line.strip()]
        except Exception as e:
            return jsonify({"error": f"Error reading false positive file: {e}"}), 400

    # Perform the analysis
    output_stream = analyze_data_and_generate_report(df_generator, fp_list)
    
    if output_stream is None:
        return jsonify({"error": "Analysis failed. Please check your data."}), 500
    
    return send_file(output_stream, as_attachment=True, download_name='cleaned_data.xlsx')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
