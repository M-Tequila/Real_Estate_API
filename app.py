import time
import pandas as pd
from cachetools import TTLCache

# Create a cache with a Time-to-Live (TTL) of 5 minutes
cache = TTLCache(maxsize=100, ttl=300)

def load_data(csv_path):
    # Check if data is already cached
    if 'data' in cache:
        return cache['data']
    
    # Load data from CSV and store it in cache
    df = pd.read_csv(csv_path)
    cache['data'] = df
    return df

# Use the function
if __name__ == '__main__':
    csv_path = 'path/to/your/file.csv'
    data = load_data(csv_path)
    print(data)