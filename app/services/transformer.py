import pandas as pd
import os
from datetime import datetime

def process_and_save(data, resource_name):
    if not data:
        return None
    
    # Flatten nested JSON structures
    df = pd.json_normalize(data)
    
    # Create data directory if not exists
    if not os.path.exists('data'):
        os.makedirs('data')
        
    filename = f"data/{resource_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    return filename