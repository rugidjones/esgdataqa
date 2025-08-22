# --- INSTALLATION FOR COLAB: Ensures xlsxwriter is available ---
import sys
import subprocess
try:
    import xlsxwriter
except ImportError:
    print("Installing required package: xlsxwriter...")
    # Use !pip in Colab for shell commands
    subprocess.check_call([sys.executable, "-m", "pip", "install", "xlsxwriter"])
    print("Installation complete.")
# --- END INSTALLATION ---

import pandas as pd
import warnings
import numpy as np
from datetime import datetime, timedelta
import os
from google.colab import files # For Colab file upload functionality
import matplotlib.pyplot as plt # For creating plots
import seaborn as sns # For enhancing plot aesthetics

# Suppress pandas RuntimeWarning for calculations with NaNs
warnings.filterwarnings("ignore", "invalid value encountered in subtract", RuntimeWarning)
# Suppress openpyxl default style warning
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
# Suppress the specific xlsxwriter URL warning
warnings.filterwarnings("ignore", category=UserWarning, module="xlsxwriter")


# --- CORE LOGIC FUNCTIONS ---
# This function is now empty as the feature has been temporarily disabled
def get_false_positive_list(uploaded_fp_dict):
    return []

def analyze_data(data_file_path, client_name, generate_full_report):
    """
    Analyzes utility bill data from an Excel file, performs various data quality
    checks, and exports the results to a new Excel file with multiple sheets.

    Returns:
        pd.DataFrame: The final processed DataFrame containing all flags,
                      used for generating the summary plot.
    """
    print("\n--- Starting Data Analysis ---")
    
    try:
        print("1. Reading source data...")
        df = pd.read_excel(data_file_path, sheet_name='Raw_Data_Table_S2')

        print("2. Renaming columns and performing initial data cleaning...")
        column_mapping = {
            'Property Name': 'Property Name',
            'Conservice Id': 'Conservice ID or Yoda Prop Code',
            'Location Bill Id': 'Location Bill ID',
            'Account Number': 'Account Number',
            'Control Number': 'Control Number',
            'Legal Vendor Name': 'Provider Name',
            'Service Type': 'Utility',
            'Meter Number': 'Meter Number',
            'Add\'l Meter Name': 'Unique Meter ID',
            'Start Date': 'Start Date',
            'End Date': 'End Date',
            'Use': 'Usage',
            'Cost': 'Cost',
            'Documentation': 'Document',
            'Service Address': 'Service Address'
        }
        df.rename(columns=column_mapping, inplace=True)

        if 'Account Number' in df.columns:
            initial_rows = len(df)
            df = df[df['Account Number'].astype(str) != '~NA~'].copy()
            filtered_rows = initial_rows - len(df)
            if filtered_rows > 0:
                print(f"   - Filtered out {filtered_rows} rows with '~NA~' in 'Account Number'.")

        essential_columns = ['Meter Number', 'Start Date', 'End Date', 'Usage', 'Cost', 'Service Address', 'Property Name']
        missing_columns = [col for col in essential_columns if col not in df.columns]
        if missing_columns:
            print(f"Error: Missing essential columns: {', '.join(missing_columns)}")
            return None

        for col in ['Gross Square Footage', 'Common Area SF']:
            if col not in df.columns:
                print(f"Warning: '{col}' column not found in source file.")

        df['Start Date'] = pd.to_datetime(df['Start Date']).dt.date
        df['End Date'] = pd.to_datetime(df['End Date']).dt.date

        if 'Created Date' in df.columns:
            df['Created Date'] = pd.to_datetime(df['Created Date']).dt.date
        if 'Last Modified Date' in df.columns:
            df['Last Modified Date'] = pd.to_datetime(df['Last Modified Date']).dt.date
        else:
            df['Last Modified Date'] = pd.NaT

        df['Usage'] = pd.to_numeric(df['Usage'], errors='coerce')
        df['Cost'] = pd.to_numeric(df['Cost'], errors='coerce')
        df = df.dropna(subset=['Usage', 'Cost'])

        df = df.sort_values(by=['Meter Number', 'Start Date'])
        
        df['Meter_First_Seen'] = df.groupby('Meter Number')['Start Date'].transform('min')
        df['Year_First_Seen'] = df['Meter_First_Seen'].astype(str).str[:4]
        
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

        usage_mean = df['Usage'].dropna().mean()
        usage_std = df['Usage'].dropna().std()
        df['Usage Z Score'] = (df['Usage'] - usage_mean) / usage_std if usage_std != 0 else np.nan

        cost_mean = df['Cost'].dropna().mean()
        cost_std = df['Cost'].dropna().std()
        df['Cost Z Score'] = (df['Cost'] - cost_mean) / cost_std if cost_std != 0 else np.nan

        df['Usage MEAN'] = usage_mean
        df['Usage Standard'] = usage_std
        df['Cost Mean'] = cost_mean
        df['Cost Standard'] = cost_std
        
        df['Gap'] = False
        df['Gap_Dates'] = ''

        date_format = '%#m/%#d/%Y' if os.name == 'nt' else '%-m/%-d/%Y'
        for meter_number in df['Meter Number'].unique():
            meter_data = df[df['Meter Number'] == meter_number].sort_values('Start Date')
            for i in range(1, len(meter_data)):
                previous_end = meter_data.iloc[i-1]['End Date']
                current_start = meter_data.iloc[i]['Start Date']
                if current_start > previous_end + pd.Timedelta(days=1):
                    df.loc[meter_data.index[i-1:i+1], 'Gap'] = True
                    df.loc[meter_data.index[i], 'Gap_Dates'] = f"{previous_end.strftime(date_format)} to {current_start.strftime(date_format)}"

        df['Is_Anomaly'] = (df['Usage Z Score'].abs() > 3.0) | \
                             (df['Cost Z Score'].abs() > 3.0) | \
                             (df['Usage'] == 0)

        df['Consecutive_Anomalies_Count'] = df.groupby('Meter Number')['Is_Anomaly'].transform(
            lambda x: x.mask(~x).groupby((x != x.shift()).cumsum()).cumcount() + 1
        ).fillna(0).astype(int)

        df['Consistently_Anomalous_Meter'] = df['Consecutive_Anomalies_Count'] >= 2

        df.drop(columns=['Is_Anomaly'], errors='ignore', inplace=True)

        df['Negative_Usage'] = df['Usage'] < 0

        df['Use_Zero_Cost_NonZero'] = (df['Usage'] == 0) & (df['Cost'] != 0)

        if 'HCF' in df.columns and df['HCF'].notna().any():
            df['HCF'] = pd.to_numeric(df['HCF'], errors='coerce')
            df['HCF_to_Gallons'] = df['HCF'] * 748
            df['HCF_Conversion_Match'] = (df['Usage'] - df['HCF_to_Gallons']).abs() <= 100
        else:
            df['HCF_to_Gallons'] = np.nan
            df['HCF_Conversion_Match'] = np.nan

        df['Zero_Between_Positive'] = False
        for meter_number in df['Meter Number'].unique():
            meter_data = df[df['Meter Number'] == meter_number].sort_values('Start Date').reset_index()
            for i in range(1, len(meter_data) - 1):
                prev_use = meter_data.loc[i - 1, 'Usage']
                curr_use = meter_data.loc[i, 'Usage']
                next_use = meter_data.loc[i + 1, 'Usage']
                prev_end = meter_data.loc[i - 1, 'End Date']
                curr_start = meter_data.loc[i, 'Start Date']
                if prev_use > 0 and curr_use == 0 and next_use > 0 and curr_start > prev_end:
                    idxs = [meter_data.loc[i - 1, 'index'], meter_data.loc[i, 'index'], meter_data.loc[i + 1, 'index']]
                    df.loc[idxs, 'Zero_Between_Positive'] = True
        
        # --- NEW Z-SCORE CALCULATIONS ---
        # Calculate Rate and Rate Z Score
        df['Rate'] = df['Cost'] / df['Usage']
        df['Rate'] = df['Rate'].replace([np.inf, -np.inf], np.nan)
        rate_mean = df['Rate'].dropna().mean()
        rate_std = df['Rate'].dropna().std()
        df['Rate Z Score'] = (df['Rate'] - rate_mean) / rate_std if rate_std != 0 else np.nan

        # Calculate Usage per SF and its Z Score
        if 'Gross Square Footage' in df.columns:
            df['Usage_per_SF'] = df['Usage'] / df['Gross Square Footage']
            usage_sf_mean, usage_sf_std = df['Usage_per_SF'].dropna().mean(), df['Usage_per_SF'].dropna().std()
            df['Usage_per_SF_zscore'] = (df['Usage_per_SF'] - usage_sf_mean) / usage_sf_std if usage_sf_std != 0 else np.nan
        else:
            df['Usage_per_SF'] = np.nan
            df['Usage_per_SF_zscore'] = np.nan
            
        if 'Gross Square Footage' in df.columns:
            df['Cost_per_SF'] = df['Cost'] / df['Gross Square Footage']
            cost_sf_mean, cost_sf_std = df['Cost_per_SF'].dropna().mean(), df['Cost_per_SF'].dropna().std()
            df['Cost_per_SF_zscore'] = (df['Cost_per_SF'] - cost_sf_mean) / cost_sf_std if cost_sf_std != 0 else np.nan
        else:
            df['Cost_per_SF'] = np.nan
            df['Cost_per_SF_zscore'] = np.nan
        
        df['Inspect_Usage_per_SF'] = df['Usage_per_SF_zscore'].abs() > 3.0
        df['Inspect_Rate'] = df['Rate Z Score'].abs() > 3.0
        df['Inspect_Cost_per_SF'] = df['Cost_per_SF_zscore'].abs() > 3.0

        # Create a boolean column for filtering false positives
        df['is_false_positive'] = False

        core_identifying_columns = [
            'Property Name', 'Location Bill ID', 'Control Number', 'Conservice ID or Yoda Prop Code', 'Provider Name',
            'Utility', 'Account Number', 'Meter Number', 'Unique Meter ID', 'Start Date', 'End Date',
            'Usage', 'Cost', 'Service Address', 'Gap_Dates', 'Document'
        ]
        
        rate_columns = ['Rate', 'Rate Z Score', 'Inspect_Rate']
        
        primary_flags = [
            'Duplicate', 'Gap',
            'Consecutive_Anomalies_Count', 'Consistently_Anomalous_Meter',
            'Inspect_Usage_per_SF',
            'Recent_Modification', 'Recently_Updated', 'Recently_Created',
            'Use_Zero_Cost_NonZero', 'Negative_Usage', 'Zero_Usage_Positive_Cost',
            'New_Bill_Usage_Anomaly',
            'HCF_Conversion_Match',
            'is_false_positive', 'Zero_Between_Positive'
        ]

        calculated_statistical_columns = [
            'Usage MEAN', 'Usage Standard',
            'Usage Z Score', 
            'Gross Square Footage', 'Common Area SF', 'Created Date', 'Last Modified Date',
            'Usage_per_SF', 'Usage_per_SF_zscore',
            'HCF', 'HCF_to_Gallons',
            'Cost Mean', 'Cost Standard', 'Cost Z Score', 'Cost_per_SF', 'Cost_per_SF_zscore', 'Inspect_Cost_per_SF',
            'Meter_First_Seen', 'Year_First_Seen'
        ]

        master_column_order = core_identifying_columns + rate_columns + primary_flags + calculated_statistical_columns

        # Re-index the DataFrame with the master column order.
        df = df.reindex(columns=master_column_order, fill_value=np.nan)
        
        print("\nAnalysis complete! Generating report...")
        
        cleaned_file_path = 'cleaned_data.xlsx'
        print("4. Generating output Excel file with formatted tabs...")
        with pd.ExcelWriter(cleaned_file_path, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Main Data', index=False)
            
            specific_anomaly_tabs = {
                'High Value Anomalies': df[((df['Usage Z Score'].abs() > 3.0) | (df['Inspect_Usage_per_SF'] == True) | (df['Inspect_Rate'] == True) | (df['Inspect_Cost_per_SF'] == True))].copy(),
                'Negative Usage Records': df[(df['Negative_Usage'] == True)].copy(),
                'Zero Usage Positive Cost': df[(df['Zero_Usage_Positive_Cost'] == True)].copy(),
                'Zero_Between_Positive': df[(df['Zero_Between_Positive'] == True)].copy(),
                'New Bill Anomalies': df[(df['New_Bill_Usage_Anomaly'] == True)].copy(),
                'HCF Mismatch': df[((df['HCF_Conversion_Match'] == False) & df['HCF'].notna())].copy(),
                'Duplicate Records': df[(df['Duplicate'] == True)].copy(),
                'Gap Records': df[(df['Gap'] == True)].copy(),
            }
            
            for tab_name, tab_df in specific_anomaly_tabs.items():
                if not tab_df.empty:
                    tab_df.to_excel(writer, sheet_name=tab_name, index=False)
                    print(f"- '{tab_name}' tab added to report.")
            
            print(f"\nCleaned data saved to {cleaned_file_path}")
            return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def generate_summary_plots(df):
    print("\n5. Generating a visual summary of the findings...")

    hcf_mismatch_count = 0
    if 'HCF_Conversion_Match' in df.columns:
        hcf_mismatch_count = df['HCF_Conversion_Match'].eq(False).sum()


    issue_counts = {
        'Duplicates': df['Duplicate'].sum(),
        'Gaps': df['Gap'].sum(),
        'Zero-Usage Between Positives': df['Zero_Between_Positive'].sum(),
        'Zero Usage Positive Cost': df['Zero_Usage_Positive_Cost'].sum(),
        'High Value Anomalies': (df['Usage Z Score'].abs() > 3.0).sum() +
                                 (df['Inspect_Usage_per_SF'] == True).sum() +
                                 (df['Inspect_Rate'] == True).sum() +
                                 (df['Inspect_Cost_per_SF'] == True).sum(),
        'Negative Usage': df['Negative_Usage'].sum(),
        'New Bill Anomalies': df['New_Bill_Usage_Anomaly'].sum(),
        'Recently Modified Bills': df['Recently_Updated'].sum(),
        'HCF Mismatch': hcf_mismatch_count,
    }

    issues_df = pd.DataFrame(issue_counts.items(), columns=['Issue', 'Count'])
    issues_df = issues_df[issues_df['Count'] > 0].sort_values(by='Count', ascending=False)

    if issues_df.empty:
        print("No major data quality issues were found! 🎉")
    else:
        fig, ax = plt.subplots(figsize=(14, 7))
        sns.barplot(x='Count', y='Issue', hue='Issue', data=issues_df, palette='viridis', orient='h', legend=False, ax=ax)
        ax.set_title('Summary of Top Data Quality Issues Found', fontsize=18, fontweight='bold', pad=20)
        ax.set_xlabel('Number of Records Affected', fontsize=12)
        ax.set_ylabel('Data Quality Issue', fontsize=12)
        ax.tick_params(axis='x', labelsize=10)
        ax.tick_params(axis='y', labelsize=10)

        for index, row in issues_df.iterrows():
            ax.text(row.Count, index, f' {int(row.Count)}', color='black', ha="left", va="center")

        plt.tight_layout()
        plt.show()


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
        # Prompt user to upload the main data file
        print("\nStep 1: Please upload your Raw_Data_Table_S2.xlsx file.")
        uploaded_data = files.upload()
        data_file_name = next(iter(uploaded_data))
        data_file_path = f"/content/{data_file_name}"
        print(f"Successfully uploaded '{data_file_name}'.")
        
        # We no longer prompt for a false positive file
        fp_list = []

        # Prompt user whether to generate a full report
        generate_full_report_input = input("Generate a complete report? (y/n, default is 'y' for files < 20MB): ").strip().lower()
        generate_full_report = generate_full_report_input in ('y', 'yes', '')

        # Analyze data and conditionally generate reports
        df_processed = analyze_data(data_file_path, generate_full_report)

        if df_processed is not None:
            # Generate and display the summary plots based on user's choice
            if generate_full_report:
                generate_summary_plots(df_processed)
            else:
                print("Summary plot skipped for large files.")

            print("\n--- Presentation Ready! ---")
            print("The `cleaned_data.xlsx` file is now ready for download from the Files pane on the left.")
            print("It contains the master data plus separate tabs for each issue identified.")
            print("Thank you for using the analyzer!")
        else:
            print("\nAnalysis failed. Please check the input file and try again.")

    except Exception as e:
        print(f"\nAn error occurred during file upload or processing: {str(e)}")
