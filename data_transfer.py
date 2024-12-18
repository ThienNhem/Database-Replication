import os
import subprocess
import mysql.connector
import time

class DatabaseMigration:
    def __init__(self, source_config, master_config):
        self.source_config = source_config
        self.master_config = master_config
        self.dump_file = 'database_dump.sql'

    def dump_source_database(self):
        try:
            print("🚀 Đang dump dữ liệu từ database nguồn...")
            cmd = [
                "mysqldump",
                f"--host={self.source_config['host']}",
                f"--port={self.source_config['port']}",
                f"--user={self.source_config['user']}",
                f"--password={self.source_config['password']}",
                self.source_config['database']
            ]
            with open(self.dump_file, 'w') as f:
                subprocess.run(cmd, stdout=f, check=True)
            print("✅ Dump dữ liệu thành công!")
        except subprocess.CalledProcessError as e:
            print(f"❌ Lỗi dump dữ liệu: {e}")
            exit(1)

    def adjust_collation(self):
        try:
            print("🚀 Đang chỉnh sửa collation trong file dump...")
            with open(self.dump_file, 'r', encoding='utf-8') as file:
                content = file.read()
            content = content.replace('utf8mb4_0900_ai_ci', 'utf8mb4_general_ci')
            with open(self.dump_file, 'w', encoding='utf-8') as file:
                file.write(content)
            print("✅ Đã thay đổi collation thành công!")
        except Exception as e:
            print(f"❌ Lỗi chỉnh sửa collation: {e}")
            exit(1)

    def import_to_master(self):
        try:
            print("🚀 Đang import dữ liệu vào master...")
            conn = mysql.connector.connect(
                host=self.master_config['host'],
                port=self.master_config['port'],
                user=self.master_config['user'],
                password=self.master_config['password']
            )
            cursor = conn.cursor()

            # Tạo database trên master nếu chưa có
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.master_config['database']}")
            conn.commit()

            # Import dữ liệu
            cmd = [
                "mysql",
                f"--host={self.master_config['host']}",
                f"--port={self.master_config['port']}",
                f"--user={self.master_config['user']}",
                f"--password={self.master_config['password']}",
                self.master_config['database']
            ]
            with open(self.dump_file, 'r') as f:
                subprocess.run(cmd, stdin=f, check=True)

            print("✅ Import dữ liệu thành công vào master!")
        except Exception as e:
            print(f"❌ Lỗi import dữ liệu: {e}")
            exit(1)

    def setup_replication(self, slave_configs):
        try:
            print("🚀 Đang thiết lập replication cho các slave...")
            master_conn = mysql.connector.connect(
                host=self.master_config['host'],
                port=self.master_config['port'],
                user=self.master_config['user'],
                password=self.master_config['password']
            )
            master_cursor = master_conn.cursor(dictionary=True)
            
            # Lấy thông tin binary log từ master
            master_cursor.execute("SHOW MASTER STATUS")
            master_status = master_cursor.fetchone()
            log_file = master_status['File']
            log_pos = master_status['Position']

            for slave_config in slave_configs:
                slave_conn = mysql.connector.connect(
                    host=slave_config['host'],
                    port=slave_config['port'],
                    user=slave_config['user'],
                    password=slave_config['password']
                )
                slave_cursor = slave_conn.cursor()
                
                # Thiết lập replication
                change_master_cmd = f"""
                    CHANGE MASTER TO
                    MASTER_HOST='{self.master_config['host']}',
                    MASTER_USER='replication_user',
                    MASTER_PASSWORD='replication_password',
                    MASTER_LOG_FILE='{log_file}',
                    MASTER_LOG_POS={log_pos};
                """
                slave_cursor.execute("STOP SLAVE")
                slave_cursor.execute(change_master_cmd)
                slave_cursor.execute("START SLAVE")
                slave_conn.commit()

                print(f"✅ Đã thiết lập replication cho slave {slave_config['host']}:{slave_config['port']}")

        except Exception as e:
            print(f"❌ Lỗi thiết lập replication: {e}")
            exit(1)

def main():
    source_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '123456',
        'database': 'source_database'
    }

    master_config = {
        'host': 'localhost',
        'port': 3308,
        'user': 'root',
        'password': '123456',
        'database': 'replication_database'
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

    migration = DatabaseMigration(source_config, master_config)
    migration.dump_source_database()
    migration.adjust_collation()  
    migration.import_to_master()
    migration.setup_replication(slave_configs)

    print("🎉 Hoàn thành quá trình chuyển dữ liệu và thiết lập replication!")

if __name__ == "__main__":
    main()
