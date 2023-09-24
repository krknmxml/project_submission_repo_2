import os
import sys
import shutil
import pandas as pd
import yfinance as yf
import time

# TO-DO
# command line options
# multiprocess fetch


# Get ticker symbols
def get_symbols_data() -> pd.DataFrame:
    """
    Downloads traded symbol information from www.nasdawtrader.com.

    Return: pandas DataFrame containing traded symbols
    """
    # fetch data
    res = pd.read_csv("http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt", sep='|')
    
    # Remove test data records
    res = res[res['Test Issue'] == 'N']
    return res


# Check if directory is writeable
def check_dir_writable(path: str) -> bool:
    """
    Checks if path is directory and writable.

    Param: path: str - The path to check

    Returns: bool - True if writable, False otherwise
    """
    if not os.path.isdir(path) and not os.access(path, os.W_OK):
        return False
    return True


# Clean up directory path and rectrease
def clean_up_and_create_directory(path: str) -> None:
    """
    Remove path and re-create it as a directory providing clean up utility.

    Param: path: str - The path to clean up.

    Returns: None, we assume that the function is called after the parent path was checked for writability.
    """
    if os.path.exists(path):
        if os.path.isfile(path):
            os.remove(path)
        else:
            shutil.rmtree(path)
    os.makedirs(path)


# Fetch financial data from Yahoo
def fetch_symbol_data(symbol: str) -> pd.DataFrame:
    """
    Downloads symbol max available data rows from Yahoo finance.

    Params: symbol: str - the financial symbol like AAPL for Apple Inc.

    Returns: Pandas DataFrame with the information. If the symbol does not exists the datafram will be empty.
    """
    res = yf.download(symbol, period="max")
    return res


# the main function
def main() -> None:
    # Time the execution
    _start_main = time.time()

    # Fetch Nasdaq traded symbols and drop NaN values
    ndt = get_symbols_data()
    ndt.dropna(subset = ["NASDAQ Symbol", "ETF"], inplace=True)
    print('total number of symbols traded = {}'.format(len(ndt.index)))

    cwd = os.getcwd()
    etfs_path, stocks_path = cwd + "/etfs", cwd + "/stocks"

    # Clean up previous dataset if exists
    clean_up_and_create_directory(etfs_path)
    clean_up_and_create_directory(stocks_path)

    downloaded = 0
    for index, row in ndt.iterrows():
        symbol = row["NASDAQ Symbol"]
        print("\nFetching: {}".format(symbol))
        
        # Time the fetch duration
        _start = time.time()
        res = fetch_symbol_data(symbol)
        _end = time.time()
        _dur = _end - _start
        
        # check the fetch result
        if len(res.index) == 0:
            print(" ... Fetch FAILED... took {} seconds".format(_dur))
            
            # Drop row from the DataFrame since we could not fetch data
            ndt.drop(index=index, axis=0, inplace=True)
            
            continue
        else:
            print(" OK , took {} seconds".format(_dur))
            downloaded += 1

        # Save the DataFrame into csv for future use
        if row["ETF"] == "Y":
            res.to_csv("{}/{}.csv".format(etfs_path, symbol))
        else:
            res.to_csv("{}/{}.csv".format(stocks_path, symbol))

    # Print some basic information about the execution before we exit
    _end_main = time.time()
    _dur_main = _end_main - _start_main
    print("\nFetched {} symbol(s) from {} symbols. Overall time took {} seconds".format(downloaded, len(ndt.index), _dur_main))

    # Save ndt as reference for Symbol information 
    ndt.to_csv("{}/{}.csv".format(cwd, "ndt_reference"))

    # We exit normally
    sys.exit(0)


# General __main__ guard 
if __name__ == '__main__':
    # change cwd to the script location
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    # check if current working directory is writable and exit if not
    if not check_dir_writable(os.getcwd()):
        print("{} does not exist or not writable".format(os.getcwd))
        sys.exit(1)
    
    main()
