import requests
import csv
import os
import yfinance as yf
from datetime import datetime
import pandas as pd

# ================= CONFIGURATION =================
CSV_URL = "https://www.cboe.com/available_weeklys/get_csv_download/"

# Creates a 'data' folder in the same directory where you run the script
OUTPUT_DIR = "data" 

# Set to True to calculate real IV from option chains (Slower, ~1 sec per stock)
# Set to False to use Beta/Fast checks only
FETCH_REAL_IV = True 
# =================================================

def ensure_output_dir():
    """Creates the folder to store downloaded files if it doesn't exist."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory: {os.path.abspath(OUTPUT_DIR)}")

def download_weeklys():
    headers = {'User-Agent': 'Mozilla/5.0'}
    print(f"Downloading CBOE Data...")
    try:
        response = requests.get(CSV_URL, headers=headers)
        response.raise_for_status()
        
        today_str = datetime.now().strftime("%Y-%m-%d")
        filepath = os.path.join(OUTPUT_DIR, f"raw_weeklys_{today_str}.csv")
        
        with open(filepath, 'wb') as f:
            f.write(response.content)
            
        return filepath
    except Exception as e:
        print(f"Error downloading: {e}")
        return None

def parse_csv_to_data(filepath):
    data_map = {}
    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 2: continue
                col0, col1 = row[0].strip().upper(), row[1].strip()
                
                # Basic filters to skip headers and dates
                if not col0 or "AVAILABLE WEEKLYS" in col0 or "TICKER" in col0: continue
                # Skip rows where the name looks like a date (e.g. 11/28/25)
                if "/" in col1 and len(col1) <= 10 and any(c.isdigit() for c in col1): continue
                
                data_map[col0] = col1
    except Exception as e:
        print(f"Error parsing CSV: {e}")
        
    return data_map

def get_wheel_metrics(ticker_symbol):
    """
    Fetches metrics specifically for the Wheel Strategy, including 50 & 200 SMAs.
    """
    metrics = {
        "Price": 0, "IV": "N/A", "Volume": 0, 
        "SMA50": 0, "SMA200": 0, "Trend": "SIDEWAYS", 
        "PriceToSales": 0, "ForwardPE": 0, "Earnings": "N/A"
    }
    
    try:
        tick = yf.Ticker(ticker_symbol)
        
        # 1. Basic Info & Valuation
        info = tick.info
        price = info.get('currentPrice', 0)
        metrics["Price"] = price
        metrics["Volume"] = info.get('averageVolume', 0)
        metrics["PriceToSales"] = info.get('priceToSalesTrailing12Months', 0)
        metrics["ForwardPE"] = info.get('forwardPE', 0)
        
        # 2. Trend (Price vs 50 & 200 SMAs)
        # Fetching history to calculate 50-day SMA, as yfinance info only provides 200-day
        hist = tick.history(period="1y")
        
        sma200 = info.get('twoHundredDayAverage', 0)
        metrics["SMA200"] = sma200
        
        sma50 = 0
        if not hist.empty and len(hist) >= 50:
            # Calculate 50-day Simple Moving Average (SMA)
            sma50 = hist['Close'].iloc[-50:].mean() # A simple approx. for 50-day SMA
            metrics["SMA50"] = round(sma50, 2)
            
        # Determine Trend based on stacking (Price > 50 > 200 or Price < 50 < 200)
        # Check that all values are non-zero before comparison
        if price > 0 and sma50 > 0 and sma200 > 0:
            if price > sma50 and sma50 > sma200:
                metrics["Trend"] = "UP" # Bullish: Price > SMA50 > SMA200
            elif price < sma50 and sma50 < sma200:
                metrics["Trend"] = "DOWN" # Bearish: Price < SMA50 < SMA200
            else:
                metrics["Trend"] = "SIDEWAYS"
        
        # 3. Earnings Date
        try:
            cal = tick.calendar
            if cal is not None and not cal.empty:
                # Calendar structure varies, usually 'Earnings Date' is the key
                dates = cal.get('Earnings Date', [])
                if dates:
                    # Format as YYYY-MM-DD
                    metrics["Earnings"] = dates[0].strftime('%Y-%m-%d')
        except:
            pass

        # 4. Implied Volatility (The Heavy Lifting)
        if FETCH_REAL_IV:
            try:
                opts = tick.options
                if opts:
                    # Look at next expiration
                    chain = tick.option_chain(opts[0])
                    calls = chain.calls
                    # Find ATM option (strike closest to current price)
                    idx = (calls['strike'] - price).abs().idxmin()
                    iv_raw = calls.loc[idx, 'impliedVolatility']
                    metrics["IV"] = round(iv_raw * 100, 1) # Store as percentage (e.g. 55.2)
            except:
                pass
                
    except Exception:
        # If yfinance fails entirely for a ticker, return defaults
        pass
        
    return metrics

def save_local_files(data_map, date_str):
    # We save two files: 
    # 1. Dated file (for history)
    # 2. "Latest" file (for easy opening)
    archive_filename = f"weeklys_enriched_{date_str}.csv"
    latest_filename = "weeklys_latest.csv"
    
    archive_path = os.path.join(OUTPUT_DIR, archive_filename)
    latest_path = os.path.join(OUTPUT_DIR, latest_filename)
    
    print(f"\nAnalyzing {len(data_map)} tickers for Wheel metrics...")
    
    rows = []
    total = len(data_map)
    count = 0
    
    # Process tickers
    for ticker, name in sorted(data_map.items()):
        count += 1
        print(f"Processing {count}/{total}: {ticker}...", end='\r')
        
        m = get_wheel_metrics(ticker)
        
        rows.append({
            "Ticker": ticker,
            "Name": name,
            "Price": m["Price"],
            "IV %": m["IV"],
            "Trend": m["Trend"],      # UP/DOWN/SIDEWAYS based on stacking
            "SMA 50": round(m["SMA50"], 2) if m["SMA50"] else "N/A",
            "SMA 200": round(m["SMA200"], 2) if m["SMA200"] else "N/A",
            "P/S": round(m["PriceToSales"], 2) if m["PriceToSales"] else "N/A",
            "Fwd P/E": round(m["ForwardPE"], 2) if m["ForwardPE"] else "N/A",
            "Avg Vol (M)": round(m["Volume"] / 1_000_000, 2), # In Millions
            "Earnings": m["Earnings"]
        })

    # Write to disk
    for filepath in [latest_path, archive_path]:
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                # Updated fieldnames to include new SMA columns
                fieldnames = ["Ticker", "Name", "Price", "IV %", "Trend", "SMA 50", "SMA 200", "P/S", "Fwd P/E", "Avg Vol (M)", "Earnings"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            print(f"\nSaved: {filepath}")
        except Exception as e:
            print(f"Error saving {filepath}: {e}")

if __name__ == "__main__":
    ensure_output_dir()
    
    raw_file = download_weeklys()
    
    if raw_file:
        # Extract date from filename (e.g. raw_weeklys_2025-11-27.csv -> 2025-11-27)
        date_str = os.path.basename(raw_file).replace('raw_weeklys_', '').replace('.csv', '')
        
        data = parse_csv_to_data(raw_file)
        
        if data:
            save_local_files(data, date_str)
        else:
            print("No data found in parsed CSV.")
