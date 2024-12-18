import mysql.connector
import time
import random
import string
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

class DatabaseReplicationTest:
    def __init__(self, master_config, slave_configs):
        self.master_config = master_config
        self.slave_configs = slave_configs
        self.test_database = 'replication_test_db'

    def _get_connection(self, config, is_read_only=False):
        try:
            conn = mysql.connector.connect(
                host=config['host'],
                port=config['port'],
                user=config['user'],
                password=config['password']
            )
            if is_read_only:
                conn.read_only = True
            return conn
        except Exception as e:
            print(f"Kết nối không thành công: {e}")
            return None

    def setup_test_database(self):
        master_conn = self._get_connection(self.master_config)
        cursor = master_conn.cursor()
        
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
            master_conn.commit()
            print("✅ Đã tạo database và bảng thử nghiệm")
        except Exception as e:
            print(f"❌ Lỗi setup database: {e}")
        finally:
            cursor.close()
            master_conn.close()

    def insert_select_test(self, num_inserts=1000, select_multiplier=10):
        master_conn = self._get_connection(self.master_config)
        master_cursor = master_conn.cursor()

        start_time = time.time()

        try:
            master_cursor.execute(f"USE {self.test_database}")
            
            # Chèn dữ liệu
            for _ in range(num_inserts):
                random_data = ''.join(random.choices(string.ascii_letters, k=50))
                master_cursor.execute("INSERT INTO performance_test (data) VALUES (%s)", (random_data,))
            
            master_conn.commit()
            insert_time = time.time()
            print(f"✍️ Ghi {num_inserts} bản ghi: {insert_time - start_time:.4f} giây")

            # Kiểm tra slave
            def slave_select_test(slave_config):
                try:
                    slave_conn = self._get_connection(slave_config, is_read_only=True)
                    slave_conn.database = self.test_database
                    slave_cursor = slave_conn.cursor()

                    slave_start_time = time.time()
                    
                    # Thực hiện select với số lượng gấp 10 lần số insert
                    for _ in range(num_inserts * select_multiplier):
                        slave_cursor.execute("SELECT * FROM performance_test ORDER BY RAND() LIMIT 1")
                        slave_cursor.fetchall()

                    slave_end_time = time.time()

                    return {
                        'host': slave_config['host'],
                        'port': slave_config['port'],
                        'insert_time': insert_time,
                        'read_time': slave_end_time - slave_start_time,
                        'read_count': num_inserts * select_multiplier
                    }

                except Exception as e:
                    print(f"❌ Lỗi đọc dữ liệu ở {slave_config['host']}: {e}")
                    return None

            results = []
            with ThreadPoolExecutor(max_workers=len(self.slave_configs)) as executor:
                futures = [executor.submit(slave_select_test, slave_config) for slave_config in self.slave_configs]
                
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        results.append(result)

            # In kết quả
            for result in results:
                print(f"📖 Slave {result['host']}:{result['port']}:")
                print(f"   - Số lượng select: {result['read_count']}")
                print(f"   - Thời gian đọc: {result['read_time']:.4f} giây")

            return results

        except Exception as e:
            print(f"❌ Lỗi thực hiện test: {e}")
        finally:
            master_cursor.close()
            master_conn.close()

def main():
    master_config = {
        'host': 'localhost',
        'port': 3308,
        'user': 'root',
        'password': '123456'
    }
    
    slave_configs = [
        {
            'host': 'localhost',
            'port': 3309,
            'user': 'root',
            'password': '123456'
        },
        {
            'host': 'localhost',
            'port': 3310,
            'user': 'root',
            'password': '123456'
        }
    ]
    
    test = DatabaseReplicationTest(master_config, slave_configs)
    
    print("🚀 Bắt đầu kiểm tra hiệu năng replication")
    
    test.setup_test_database()
    test.insert_select_test(num_inserts=1000, select_multiplier=10)

if __name__ == "__main__":
    main()
