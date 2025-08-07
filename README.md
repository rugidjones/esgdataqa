🌟 Automated Utility Bill Data Quality Analyzer (Web App)
This application is a data quality tool designed to help you quickly identify potential issues in utility billing files. The app provides a clean web interface for uploading data and generates a detailed Excel report with specific tabs for each type of anomaly found.

🚀 How to Use the Web App
Access the URL: Navigate to the live URL of your deployed Render service.

Upload Files:

Upload Raw Data: Click "Choose File" next to the "Upload Raw Data" label and select your Raw_Data_Table_S2.xlsx file.

Upload False Positives (Optional): If you have a client-specific list of known false positives, click "Choose File" next to the "Upload False Positives" label and select your text file (e.g., false_positives_PECO.txt). This will filter out those known issues.

Run Analysis: Click the "Run Analysis" button. The app will process the data and, once complete, automatically download a cleaned_data.xlsx file to your computer.

✨ Key Features & Data Quality Checks
The generated cleaned_data.xlsx report contains multiple tabs to help you prioritize your review:

Sheet1 (Main Data): The full dataset with all the new flags and calculated columns appended.

Recently Modified Bills: A prioritized tab of bills that were modified in the last 30 days and require re-verification.

New Bill Anomalies: Flags new bills (created in the last 30 days) that have an anomalous Usage Z Score, indicating potential data entry errors.

High Value Anomalies: Bills with unusually high Usage or Usage per Square Foot.

Rate Anomalies: Bills where the calculated Rate (Cost / Usage) is an extreme outlier.

Negative Usage Records: Bills with negative Usage values.

Zero Cost Positive Usage: Bills with a Cost of zero but a positive Usage amount.

Bills After Sale Date: Bills whose End Date is after the property's Sold date.

Duplicate Records: Bills that are exact duplicates based on key details.

Gap Records: Bills with a gap in their date history.

HCF Mismatch: Bills with a discrepancy between the reported HCF and Usage values (if HCF data is present).

No Recent Data Meters: Meters that haven't had a bill in the last 90 days.

Zero_Between_Positive: Bills with zero usage that are between two positive-usage bills for the same meter.

📋 Update Log
Version 1.0 (Initial Development)
Core data quality checks and local script created.

Version 2.0 (Render Web App)
The entire local script was refactored into a Flask web application (app.py).

Added a simple web interface (index.html) for user interaction.

The application is now deployable on platforms like Render.

The requirements.txt and Dockerfile were created to support the deployment.

Version 2.1 (Current Version - YYYY-MM-DD)
The frontend UI was improved with clearer file upload labels and a brief description.

The app.py backend was updated to correctly handle and return the multi-tabbed Excel report, fixing the issue where only one sheet was being generated.

📧 Questions or Issues?
Please contact [Your Name/Team Name] if you have any questions, encounter issues, or have suggestions for further improvements.
