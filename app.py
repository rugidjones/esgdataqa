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

def get_false_positive_list(client_name):
    """
    Reads the false positive list from an uploaded text file for a given client.
    This version is adapted for the web app's file handling.
    """
    file_name = f"false_positives_{client_name}.txt"
    if os.path.exists(file_name):
        with open(file_name, 'r') as f:
            fp_list = [int(line.strip()) for line in f if line.strip()]
        print(f"Loaded {len(fp_list)} false positives for '{client_name}'.")
        return fp_list
    else:
        print(f"Warning: No false positive file found for '{client_name}'. No filters will be applied.")
        return []

def analyze_data_core(df, client_name, fp_list):
    """
    Performs the core data analysis logic.
    This function is a refactoring of your original script,
    designed to be called by the web API.
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

        cleaned_file_path = 'cleaned_data.xlsx'
        with pd.ExcelWriter(cleaned_file_path, engine='xlsxwriter') as writer:
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
        
        print(f"\nCleaned data saved to {cleaned_file_path}")
        return df 

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

def generate_summary_plots(df):
    print("\n5. Generating a visual summary of the findings...")
    hcf_mismatch_count = 0
    if 'HCF_Conversion_Match' in df.columns:
        hcf_mismatch_count = df['HCF_Conversion_Match'].eq(False).sum()

    df_filtered = df[df['is_false_positive'] == False]
    issue_counts = {
        'Duplicates': df_filtered['Duplicate'].sum(),
        'Gaps': df_filtered['Gap'].sum(),
        'Zero-Usage Between Positives': df_filtered['Zero_Between_Positive'].sum(),
        'Zero Usage Non-Zero Cost': df_filtered['Use_Zero_Cost_NonZero'].sum(),
        'High Value Anomalies': (df_filtered['Usage Z Score'].abs() > 3.0).sum() +
                                (df_filtered['Inspect_Usage_per_SF'] == 'red').sum(),
        'Rate Anomalies': (df_filtered['Inspect_Rate'] == 'red').sum(),
        'Negative Usage': df_filtered['Negative_Usage'].sum(), 
        'Bills After Sale Date': df_filtered['Bill_After_Sold_Date'].sum(),
        'New Bill Anomalies': (df_filtered['New_Bill_Usage_Anomaly'] == True).sum(),
        'Recently Modified Bills': (df_filtered['Recently_Updated'] == True).sum(),
        'HCF Mismatch': (df_filtered['HCF_Conversion_Match'] == False).sum() if 'HCF_Conversion_Match' in df_filtered.columns else 0,
        'No Recent Data': df_filtered['No_Recent_Data_Flag'].sum()
    }
    
    issues_df = pd.DataFrame(issue_counts.items(), columns=['Issue', 'Count'])
    issues_df = issues_df[issues_df['Count'] > 0].sort_values(by='Count', ascending=False)
    if issues_df.empty:
        print("No major data quality issues were found! 🎉"); return

    plt.style.use('seaborn-v0_8-whitegrid')
    plt.figure(figsize=(14, 7))
    sns.barplot(x='Count', y='Issue', hue='Issue', data=issues_df, palette='viridis', orient='h', legend=False)
    plt.title('Summary of Top Data Quality Issues Found', fontsize=18, fontweight='bold', pad=20)
    plt.xlabel('Number of Records Affected', fontsize=12); plt.ylabel('Data Quality Issue', fontsize=12)
    plt.xticks(fontsize=10); plt.yticks(fontsize=10)
    
    for index, row in issues_df.iterrows():
        plt.text(row.Count, index, f' {int(row.Count)}', color='black', ha="left", va="center")
    plt.tight_layout(); plt.show()


if __name__ == "__main__":
    print("🌟 Welcome to the Automated Utility Bill Data Quality Analyzer!")
    print("This tool will perform a series of data checks and provide a detailed report.")
    
    # Clean up the output file from any previous runs
    cleaned_output_file = 'cleaned_data.xlsx'
    if os.path.exists(cleaned_output_file):
        try:
            os.remove(cleaned_output_file)
            print("\nPrevious 'cleaned_data.xlsx' file has been removed for a fresh run.")
        except Exception as e:
            print(f"Warning: Could not remove previous '{cleaned_output_file}': {e}")
            
    try:
        print("\nStep 1: Please upload your main data Excel file (e.g., Raw_Data_Table_S2.xlsx).")
        uploaded_data = files.upload()
        if not uploaded_data:
            print("No main data file was uploaded. Please upload a file to continue.")
            exit()
        data_file_name = next(iter(uploaded_data))
        data_file_path = f"/content/{data_file_name}"
        print(f"Successfully uploaded '{data_file_name}'.")

        print(f"\nStep 2: Please upload the false positive text file for client '{CURRENT_CLIENT_NAME}' (e.g., false_positives_{CURRENT_CLIENT_NAME}.txt).")
        print("Note: If a false positive file for this client does not exist, the script will proceed without filtering.")
        uploaded_fp = files.upload()
        
        df_processed = analyze_data(data_file_path, CURRENT_CLIENT_NAME)

        if df_processed is not None:
            generate_summary_plots(df_processed)
            print("\n--- Presentation Ready! ---")
            print("The `cleaned_data.xlsx` file is now ready for download from the Files pane on the left.")
            print("It contains the master data plus separate tabs for each issue identified, filtered for false positives.")
            print("Thank you for using the analyzer!")
        else:
            print("\nAnalysis failed. Please check the input file and try again.")
    except Exception as e:
        print(f"\nAn error occurred during file upload or processing: {str(e)}")
