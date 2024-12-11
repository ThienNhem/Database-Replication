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

    def write_test(self, num_records=1000):
        master_conn = self._get_connection(self.master_config)
        cursor = master_conn.cursor()
        
        start_time = time.time()
        
        try:
            cursor.execute(f"USE {self.test_database}")
            
            # Chèn dữ liệu
            for _ in range(num_records):
                random_data = ''.join(random.choices(string.ascii_letters, k=50))
                cursor.execute("INSERT INTO performance_test (data) VALUES (%s)", (random_data,))
            
            master_conn.commit()
            end_time = time.time()
            
            print(f"✍️ Ghi {num_records} bản ghi: {end_time - start_time:.4f} giây")
            return end_time - start_time
        
        except Exception as e:
            print(f"❌ Lỗi ghi dữ liệu: {e}")
        finally:
            cursor.close()
            master_conn.close()

    def read_test(self, num_reads=1000):
        # Kiểm tra độ trễ replication ở các slave
        results = []
        
        def test_slave(slave_config):
            try:
                slave_conn = self._get_connection(slave_config, is_read_only=True)
                slave_conn.database = self.test_database
                cursor = slave_conn.cursor()
                
                start_time = time.time()
                for _ in range(num_reads):
                    cursor.execute("SELECT * FROM performance_test ORDER BY RAND() LIMIT 10")
                    cursor.fetchall()
                
                end_time = time.time()
                
                return {
                    'host': slave_config['host'],
                    'port': slave_config['port'],
                    'time': end_time - start_time
                }
            
            except Exception as e:
                print(f"❌ Lỗi đọc dữ liệu ở {slave_config['host']}: {e}")
                return None
        
        with ThreadPoolExecutor(max_workers=len(self.slave_configs)) as executor:
            futures = [executor.submit(test_slave, slave_config) for slave_config in self.slave_configs]
            
            for future in as_completed(futures):
                result = future.result()
                if result:
                    results.append(result)
        
        for result in results:
            print(f"📖 Đọc từ {result['host']}:{result['port']}: {result['time']:.4f} giây")
        
        return results

    def replication_lag_test(self):
        master_conn = self._get_connection(self.master_config)
        master_conn.database = self.test_database
        master_cursor = master_conn.cursor()
        
        # Chèn một bản ghi và kiểm tra thời gian xuất hiện ở slave
        random_data = ''.join(random.choices(string.ascii_letters, k=50))
        
        # Ghi vào master
        master_cursor.execute("INSERT INTO performance_test (data) VALUES (%s)", (random_data,))
        master_conn.commit()
        last_insert_id = master_cursor.lastrowid
        insert_time = time.time()
        
        # Kiểm tra slave
        lag_results = []
        for slave_config in self.slave_configs:
            try:
                slave_conn = self._get_connection(slave_config, is_read_only=True)
                slave_conn.database = self.test_database
                slave_cursor = slave_conn.cursor()
                
                # Chờ và kiểm tra
                for _ in range(10):  # Thử 10 lần
                    slave_cursor.execute("SELECT * FROM performance_test WHERE id = %s", (last_insert_id,))
                    result = slave_cursor.fetchone()
                    
                    if result:
                        replication_time = time.time()
                        lag = replication_time - insert_time
                        lag_results.append({
                            'host': slave_config['host'],
                            'port': slave_config['port'],
                            'lag': lag
                        })
                        break
                    
                    time.sleep(0.1)  # Chờ 100ms giữa các lần thử
                
            except Exception as e:
                print(f"❌ Lỗi kiểm tra độ trễ ở {slave_config['host']}: {e}")
        
        for result in lag_results:
            print(f"⏱️ Độ trễ ở {result['host']}:{result['port']}: {result['lag']:.4f} giây")
        
        return lag_results

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
    test.write_test(num_records=5000)
    test.read_test(num_reads=1000)
    test.replication_lag_test()

if __name__ == "__main__":
    main()