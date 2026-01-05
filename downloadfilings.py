import os
from pathlib import Path
from sec_edgar_downloader import Downloader
from bs4 import BeautifulSoup

# --- GLOBAL CONFIGURATION ---
# Putting these here fixes the NameError and makes them easy to change
EMAIL = "your.email@domain.com"
TICKERS = ["MSFT", "AAPL", "NVDA", "TSLA", "GOOGL", "AVGO"]
FORMS = ["10-K", "10-Q"]
BASE_DIR = "sec-edgar-filings"
OUTPUT_DIR = "momentum_7_txt_uploads"

def download_and_convert_to_txt():
    # Ensure the upload folder exists
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    dl = Downloader("Momentum Phinance", EMAIL)

    for ticker in TICKERS:
        for form in FORMS:
            print(f"Fetching {form} for {ticker}...")
            dl.get(form, ticker, limit=1, download_details=True)

            ticker_path = Path(BASE_DIR) / ticker / form
            
            if ticker_path.exists():
                for filing_folder in ticker_path.iterdir():
                    if filing_folder.is_dir():
                        # SEC download creates HTML by default
                        for html_file in filing_folder.glob("*.html"):
                            with open(html_file, 'r', encoding='utf-8') as f:
                                soup = BeautifulSoup(f.read(), "html.parser")
                                
                                # Preserve spacing for financial tables
                                raw_text = soup.get_text(separator='\n\n', strip=True)
                                
                                new_name = f"{ticker}_{form}.txt"
                                dest_path = os.path.join(OUTPUT_DIR, new_name)
                                
                                with open(dest_path, 'w', encoding='utf-8') as out_f:
                                    out_f.write(raw_text)
                                print(f"  ✓ Converted to Text: {new_name}")
            else:
                print(f"  ! Skipping {ticker} {form} (no data found)")

if __name__ == "__main__":
    download_and_convert_to_txt()
    # Now this print works because OUTPUT_DIR is global!
    print(f"\nWinning! Drop the files from '{OUTPUT_DIR}' into NotebookLM.")
