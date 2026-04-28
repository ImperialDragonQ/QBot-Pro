"""
QBot Pro - Data Storage System
Stores market data permanently in SQLite database
"""

import sqlite3
import pandas as pd
from datetime import datetime
import os

class DataStorage:
    def __init__(self, db_path='./data/qbot_data.db'):
        """
        Initialize database connection
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self._ensure_directory_exists()
        self._initialize_database()
        
    def _ensure_directory_exists(self):
        """Create data directory if it doesn't exist"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
    def _initialize_database(self):
        """Create tables if they don't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create ES ticks table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS es_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                price REAL NOT NULL,
                size INTEGER NOT NULL,
                flags INTEGER,
                sequence INTEGER,
                ts_recv DATETIME
            )
        ''')
        
        # Create NQ ticks table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS nq_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                price REAL NOT NULL,
                size INTEGER NOT NULL,
                flags INTEGER,
                sequence INTEGER,
                ts_recv DATETIME
            )
        ''')
        
        # Create index for faster queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_es_timestamp ON es_ticks(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_nq_timestamp ON nq_ticks(timestamp)')
        
        conn.commit()
        conn.close()
        
        print(f"✅ Database initialized at: {self.db_path}")
    
    def store_tick_data(self, instrument, df):
        """
        Store tick data DataFrame to database
        
        Args:
            instrument: 'ES' or 'NQ'
            df: Pandas DataFrame with tick data
        """
        if instrument.upper() not in ['ES', 'NQ']:
            raise ValueError("Instrument must be 'ES' or 'NQ'")
            
        table_name = f"{instrument.lower()}_ticks"
        
        conn = sqlite3.connect(self.db_path)
        
        # Store data
        df.to_sql(table_name, conn, if_exists='append', index=False)
        
        rows_added = len(df)
        conn.commit()
        conn.close()
        
        print(f"✅ Stored {rows_added:,} {instrument} ticks in database")
    
    def get_data_range(self, instrument, start_date, end_date):
        """
        Retrieve data for date range
        
        Returns:
            Pandas DataFrame with tick data
        """
        table_name = f"{instrument.lower()}_ticks"
        
        conn = sqlite3.connect(self.db_path)
        
        query = f'''
            SELECT * FROM {table_name}
            WHERE timestamp >= '{start_date}' AND timestamp <= '{end_date}'
            ORDER BY timestamp ASC
        '''
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
    
    def resample_ohlcv(self, df, timeframe='5m'):
        """
        Resample tick data to OHLCV candles
        
        Args:
            df: Tick data DataFrame
            timeframe: Resample period ('1m', '5m', '15m', '1h')
            
        Returns:
            OHLCV DataFrame
        """
        if len(df) == 0:
            return pd.DataFrame()
            
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        
        ohlcv = df['price'].resample(timeframe).ohlc()
        ohlcv['volume'] = df['size'].resample(timeframe).sum()
        
        ohlcv.columns = ['open', 'high', 'low', 'close', 'volume']
        ohlcv.reset_index(inplace=True)
        
        return ohlcv
    
    def count_records(self, instrument):
        """Count total records for instrument"""
        table_name = f"{instrument.lower()}_ticks"
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
        count = cursor.fetchone()[0]
        conn.close()
        
        return count
    
    def get_date_range(self, instrument):
        """Get min/max dates in database"""
        table_name = f"{instrument.lower()}_ticks"
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f'SELECT MIN(timestamp), MAX(timestamp) FROM {table_name}')
        result = cursor.fetchone()
        conn.close()
        
        if result[0] is None:
            return "No data", "No data"
            
        return result[0], result[1]
    
    def get_status(self):
        """Get overall database status"""
        status = {
            'database_path': self.db_path,
            'es_records': self.count_records('ES'),
            'nq_records': self.count_records('NQ'),
            'es_date_range': self.get_date_range('ES'),
            'nq_date_range': self.get_date_range('NQ'),
        }
        return status


if __name__ == '__main__':
    # Test the database system
    print("=" * 60)
    print("Testing QBot Data Storage System")
    print("=" * 60)
    
    storage = DataStorage()
    status = storage.get_status()
    
    print(f"\nDatabase Status:")
    print(f"  Location: {status['database_path']}")
    print(f"  ES Records: {status['es_records']:,}")
    print(f"  NQ Records: {status['nq_records']:,}")
    print(f"  ES Date Range: {status['es_date_range']}")
    print(f"  NQ Date Range: {status['nq_date_range']}")
    
    print("\n✅ Data Storage System ready!")