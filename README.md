eopp_top_log_analyzer
Author: Eugene Arkhipov

Description
A Python script for normalizing and aggregating level-3 errors from the EOPP system based on logs exported from Graylog.
It identifies the top 70 most frequent error messages and exceptions, making it easier to diagnose and prioritize issues in production environments.

What the Script Does
- Loads a CSV file containing logs (e.g., raw_logs.csv) exported from Graylog.
- Normalizes error messages by removing variable parts (UUIDs, timestamps, IDs) to group similar errors together.
- Counts the frequency of unique normalized error messages and generates a Top-N list (default: 70).
- Performs similar normalization and aggregation for exception traces.
- Saves the results to CSV files inside the output/ folder.
- Helps quickly identify the most common issues in the production environment.

Requirements
- Python 3.7+
- Libraries:
  - pandas
  - re (standard library)

To install the required dependencies:
pip install pandas

How to Use
1. Place the input CSV file (e.g., raw_logs.csv) in the project root.
2. Run the script:
   python eopp_top_50_v1.03.py
3. The results will be saved in the output/ directory:
   - top_70_errors.csv — top error messages by count.
   - top_70_exceptions.csv — top exception traces.

Configuration
- To change the number of displayed errors, edit the constant:
  NUMBER_OF_ERRORS = 70
- If your input file is named differently or stored elsewhere, update the file_path variable in the script accordingly.

Project Structure (generated with help)
/eopp_top_log_analyzer
│
├── eopp_top_50_v1.03.py  — main analysis script
├── raw_logs.csv          — input log file (not included in the repo)
├── output/               — directory for results (created automatically)
└── README.md             — this file

Why This Matters
Error normalization helps consolidate similar incidents by hiding unique noise (like IDs or timestamps). This makes it easier to spot trends, react faster to recurring issues, and triage critical failures effectively.

Contact & Support
Have questions or suggestions?
Feel free to open an issue or reach out directly.
