import os
from sec_edgar_downloader import Downloader

def fetch_momentum_filings():
    # The SEC requires a User-Agent that includes your name/company and email
    # Replace the placeholders with your actual info to avoid being throttled
    dl = Downloader("Momentum Phinance", "your.email@domain.com")
    
    # Your current Momentum 7 list
    tickers = ["MSFT", "AAPL", "NVDA", "GOOGL", "TSLA"]
    
    for ticker in tickers:
        print(f"--- Processing {ticker} ---")
        
        # 1. Fetch the latest 10-K (Annual Report)
        # limit=1 ensures you only get the most recent one
        dl.get("10-K", ticker, limit=1, download_details=True)
        
        # 2. Fetch the latest 10-Q (Quarterly Report)
        dl.get("10-Q", ticker, limit=1, download_details=True)
        
        # 3. Optional: Fetch recent 8-Ks (Material Events/Earnings Press Releases)
        # This is where you often find the "hype" vs. "reality" management commentary
        dl.get("8-K", ticker, limit=2, download_details=True)

if __name__ == "__main__":
    fetch_momentum_filings()
    print("\nDownload Complete. Check your current directory for a 'sec-edgar-filings' folder.")
