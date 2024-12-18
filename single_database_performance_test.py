import mysql.connector
import time
import random
import string
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

class SingleDatabasePerformanceTest:
    def __init__(self, database_config):
        self.database_config = database_config
        self.test_database = 'single_db_performance_test'

    def _get_connection(self):
        try:
            conn = mysql.connector.connect(
                host=self.database_config['host'],
                port=self.database_config['port'],
                user=self.database_config['user'],
                password=self.database_config['password']
            )
            return conn
        except Exception as e:
            print(f"Kết nối không thành công: {e}")
            return None

    def setup_test_database(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Tạo database và bảng
            cursor.execute(f"DROP DATABASE IF EXISTS {self.test_database}")
            cursor.execute(f"CREATE DATABASE {self.test_database}")
            cursor.execute(f"USE {self.test_database}")
            cursor.execute("""
                CREATE TABLE performance_test (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    data VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            print("✅ Đã tạo database và bảng thử nghiệm")
        except Exception as e:
            print(f"❌ Lỗi setup database: {e}")
        finally:
            cursor.close()
            conn.close()

    def insert_select_test(self, num_inserts=1000, select_multiplier=20):
        conn = self._get_connection()
        cursor = conn.cursor()

        start_time = time.time()

        try:
            cursor.execute(f"USE {self.test_database}")
            
            # Chèn dữ liệu
            for _ in range(num_inserts):
                random_data = ''.join(random.choices(string.ascii_letters, k=50))
                cursor.execute("INSERT INTO performance_test (data) VALUES (%s)", (random_data,))
            
            conn.commit()
            insert_time = time.time()
            print(f"✍️ Ghi {num_inserts} bản ghi: {insert_time - start_time:.4f} giây")

            # Thực hiện select
            select_start_time = time.time()
            for _ in range(num_inserts * select_multiplier):
                cursor.execute("SELECT * FROM performance_test ORDER BY RAND() LIMIT 1")
                cursor.fetchall()
            
            select_end_time = time.time()

            # In kết quả
            print(f"📖 Chi tiết thực thi:")
            print(f"   - Số lượng select: {num_inserts * select_multiplier}")
            print(f"   - Thời gian đọc: {select_end_time - select_start_time:.4f} giây")

            return {
                'insert_time': insert_time - start_time,
                'read_time': select_end_time - select_start_time,
                'read_count': num_inserts * select_multiplier
            }

        except Exception as e:
            print(f"❌ Lỗi thực hiện test: {e}")
        finally:
            cursor.close()
            conn.close()

def main():
    database_config = {
        'host': 'localhost',
        'port': 3308,  
        'user': 'root',
        'password': '123456'
    }
    
    test = SingleDatabasePerformanceTest(database_config)
    
    print("🚀 Bắt đầu kiểm tra hiệu năng database")
    
    test.setup_test_database()
    test.insert_select_test(num_inserts=10000, select_multiplier=20)

if __name__ == "__main__":
    main()