import requests
import pandas as pd
import re
import os
import sys
from datetime import date, datetime
from io import StringIO

# --- Configuration (TEMPLATE) ---
ETF_TICKER = "KYLD" # <-- CHANGE THIS FOR OTHER ETFs
DOWNLOAD_URL = f"https://web.services.kurvinvest.com/etfdata/{ETF_TICKER}/holdings.csv"
OUTPUT_FILENAME = f"enriched_{ETF_TICKER}.csv"
ARCHIVE_FILENAME_BASE = "holdings.csv"
# --- End Configuration ---

def parse_option_ticker(ticker):
    """
    Parses a complex option ticker string (cleaned of spaces) to extract its components.
    
    Assumes pattern: [ROOT][YYMMDD][C/P][STRIKE_PRICE]
    Example: 'BWXT251219C00195000' -> Date: 251219, Type: C, Strike: 00195000
    
    Returns a tuple: (Expiration_Date_Formatted, Option_Type, Strike_Price_Decimal)
    """
    # Regex pattern: (\d{6}) captures 6-digit date; ([CP]) captures C or P; (\d+) captures strike
    match = re.search(r'(\d{6})([CP])(\d+)', str(ticker))
    
    if match:
        yy_mm_dd = match.group(1)
        option_type = match.group(2)
        strike_raw = match.group(3)
        
        # 1. Format Expiration Date (YYMMDD to YYYY-MM-DD)
        try:
            date_obj = datetime.strptime(yy_mm_dd, '%y%m%d')
            expiration_date = date_obj.strftime('%Y-%m-%d')
        except ValueError:
            expiration_date = None
        
        # 2. Convert Strike Price (dividing by 1000)
        try:
            strike_price = int(strike_raw) / 1000.0
        except ValueError:
            strike_price = None

        return expiration_date, option_type, strike_price
    
    return None, None, None

def classify_holding(row):
    """
    Classifies the holding based on Option Type and Quantity.
    """
    quantity = row['Quantity']
    option_type = row['Put/Call']
    
    if pd.notna(option_type) and quantity < 0:
        if option_type == 'C':
            return 'CC' # Covered Call (Short Call)
        elif option_type == 'P':
            return 'CSP' # Cash-Secured Put (Short Put)
    
    # Non-option holdings are classified as 'Stock'
    if pd.isna(option_type):
        return 'Stock' 
    else:
        return f'Long {option_type}' # Long Call or Long Put

def process_etf_data():
    """
    Downloads, enriches, archives, and saves the final ETF holdings data.
    """
    try:
        # --- DOWNLOAD AND ARCHIVE ---
        print(f"Attempting to download file for {ETF_TICKER} from: {DOWNLOAD_URL}")
        response = requests.get(DOWNLOAD_URL, stream=True)
        response.raise_for_status() # Check for bad status codes
        print("Download successful.")
        
        # Read content for pandas
        file_content_text = response.content.decode('utf-8')

        # 1. Save the dated archive copy
        today = date.today().strftime("%Y-%m-%d")
        dated_filename = f"{today}_{ETF_TICKER}_{ARCHIVE_FILENAME_BASE}"
        with open(dated_filename, 'w', encoding='utf-8') as f:
            f.write(file_content_text)
        print(f"Saved dated archive copy as: {dated_filename}")

        # 2. Load data directly from memory into Pandas
        df = pd.read_csv(StringIO(file_content_text))
        
        if 'Ticker' not in df.columns or 'Quantity' not in df.columns or 'Description' not in df.columns:
             print("Error: The CSV must contain 'Ticker', 'Quantity', and 'Description' columns.")
             sys.exit(1)
             
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0)
        
        # --- ENRICHMENT STEPS ---
        print("Starting data enrichment...")
        
        # 3. Clean Ticker: Remove all spaces
        df['Ticker'] = df['Ticker'].astype(str).str.replace(' ', '')
        
        # 4. Filter out cash, treasury, and other non-security lines
        cash_keywords = ['TREASURY', 'CASH', 'SWAP', 'REPURCHASE', 'RECEIVABLE', 'DEPOSIT', 'FUTURES', 'CONTRACT', 'MMKT']
        mask = ~df['Description'].astype(str).str.upper().str.contains('|'.join(cash_keywords), na=False)
        df = df[mask].reset_index(drop=True)
        print(f"Filtered to {len(df)} security lines.")

        # 5. Apply the parsing function and expand results
        parsed_results = df['Ticker'].apply(parse_option_ticker)
        df[['Expiration', 'Put/Call', 'Strike']] = pd.DataFrame(parsed_results.tolist(), index=df.index)
        
        # 6. Apply the classification logic
        df['Classification'] = df.apply(classify_holding, axis=1)
        
        # 7. Add ETF column
        df['ETF'] = ETF_TICKER

        # 8. Final Column Reordering
        target_start_cols = ['ETF', 'Ticker', 'Put/Call', 'Strike', 'Expiration', 'Classification']
        other_cols = [col for col in df.columns.tolist() if col not in target_start_cols]
        final_cols = [col for col in target_start_cols if col in df.columns.tolist()] + other_cols
        df = df[final_cols]
        
        # --- SAVE FINAL OUTPUT ---
        print(f"Saving enriched data to: {OUTPUT_FILENAME}")
        df.to_csv(OUTPUT_FILENAME, index=False)
        print(f"Process complete! Check '{dated_filename}' (archive) and '{OUTPUT_FILENAME}' (enriched).")

    except requests.exceptions.RequestException as e:
        print(f"A download error occurred: {e}")
        sys.exit(1)
    except ImportError:
        print("\n--- REQUIRED LIBRARY MISSING ---")
        print("The 'pandas' and 'requests' libraries are required.")
        print("Please install them using: \n\n    pip install requests pandas")
        print("----------------------------------")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    process_etf_data()
