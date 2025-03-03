import sqlite3
from datetime import datetime
import pandas as pd
import os
import hashlib
import logging

class DatabaseManager:
    def __init__(self, db_path="tess_downloads.db"):
        self.db_path = db_path
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.init_db()
    
    def init_db(self):
        """Initialize the database with required tables"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute('''CREATE TABLE IF NOT EXISTS downloads
                    (file_id TEXT,
                     sector INTEGER,
                     pipeline TEXT,
                     download_attempts INTEGER DEFAULT 0,
                     last_attempt_time TIMESTAMP,
                     status TEXT,
                     error_message TEXT,
                     file_path TEXT,
                     file_size INTEGER,
                     checksum TEXT,
                     PRIMARY KEY (file_id, sector, pipeline))''')
        
        conn.commit()
        conn.close()

    def update_status(self, file_id, status, error_message=None):
        """Update the status of a file download"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute('''UPDATE downloads 
                    SET status = ?, 
                        last_attempt_time = ?,
                        download_attempts = download_attempts + 1,
                        error_message = ?
                    WHERE file_id = ?''', 
                    (status, datetime.now(), error_message, file_id))
        
        conn.commit()
        conn.close()

    def add_file(self, file_id, sector, pipeline, file_path):
        """Add a new file to track"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute('''INSERT OR IGNORE INTO downloads 
                    (file_id, sector, pipeline, status, file_path)
                    VALUES (?, ?, ?, 'pending', ?)''',
                    (file_id, sector, pipeline, file_path))
        
        conn.commit()
        conn.close()

    def get_failed_downloads(self):
        """Get list of failed downloads"""
        conn = sqlite3.connect(self.db_path)
        query = '''SELECT * FROM downloads WHERE status = 'failed' '''
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def get_pending_downloads(self):
        """Get list of pending downloads"""
        conn = sqlite3.connect(self.db_path)
        query = '''SELECT * FROM downloads WHERE status = 'pending' '''
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def get_download_stats(self):
        """Get download statistics"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute('''SELECT status, COUNT(*) FROM downloads GROUP BY status''')
        stats = dict(c.fetchall())
        
        conn.close()
        return stats

    def update_file_info(self, file_path, file_size, checksum):
        """Update file information in database"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute('''UPDATE downloads 
                    SET file_size = ?,
                        checksum = ?
                    WHERE file_path = ?''',
                    (file_size, checksum, file_path))
        
        conn.commit()
        conn.close()

    def get_failed_details(self):
        """Get details of failed downloads"""
        conn = sqlite3.connect(self.db_path)
        query = '''
            SELECT file_id, sector, pipeline, error_message, last_attempt_time 
            FROM downloads 
            WHERE status = 'failed'
            ORDER BY last_attempt_time DESC
        '''
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def print_failure_summary(self):
        """Print a summary of failed downloads"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute('''
            SELECT error_message, COUNT(*) as count
            FROM downloads
            WHERE status = 'failed'
            GROUP BY error_message
        ''')
        
        logger = logging.getLogger(__name__)
        logger.warning("\nFailure Summary:")
        for error, count in c.fetchall():
            logger.warning(f"{count} files failed with error: {error}")
        
        conn.close()