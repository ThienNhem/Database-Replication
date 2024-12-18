import mysql.connector
import pandas as pd
import os
from sqlalchemy import create_engine
import time
import logging

class DatabaseMigrator:
    def __init__(self, source_config, target_master_config):
        """
        Khởi tạo migrator với cấu hình database nguồn và database đích
        
        :param source_config: Dict chứa thông tin kết nối database nguồn
        :param target_master_config: Dict chứa thông tin kết nối database master đích
        """
        self.source_config = source_config
        self.target_master_config = target_master_config
        
        # Cấu hình logging
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s: %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _get_source_connection(self):
        """Tạo kết nối đến database nguồn"""

        
        try:
            connection = mysql.connector.connect(
                host=self.source_config['host'],
                port=self.source_config['port'],
                user=self.source_config['user'],
                password=self.source_config['password'],
                database=self.source_config['database']
            )
            return connection
        except Exception as e:
            self.logger.error(f"Lỗi kết nối database nguồn: {e}")
            raise

    def _get_target_connection(self):
        """Tạo kết nối đến database master đích"""
        try:
            connection = mysql.connector.connect(
                host=self.target_master_config['host'],
                port=self.target_master_config['port'],
                user=self.target_master_config['user'],
                password=self.target_master_config['password']
            )
            return connection
        except Exception as e:
            self.logger.error(f"Lỗi kết nối database đích: {e}")
            raise

    def get_all_tables(self, connection):
        """
        Lấy danh sách tất cả các bảng trong database
        
        :param connection: Kết nối database
        :return: Danh sách tên bảng
        """
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        return tables

    def migrate_schema(self, source_db, target_db):
        """
        Copy schema của các bảng từ database nguồn sang database đích
        
        :param source_db: Tên database nguồn
        :param target_db: Tên database đích
        """
        source_conn = self._get_source_connection()
        target_conn = self._get_target_connection()
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor()

        try:
            # Tạo database đích nếu chưa tồn tại
            target_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {target_db}")
            target_cursor.execute(f"USE {target_db}")

            # Lấy danh sách bảng
            source_cursor.execute(f"USE {source_db}")
            tables = self.get_all_tables(source_conn)

            for table in tables:
                # Lấy cấu trúc bảng
                source_cursor.execute(f"SHOW CREATE TABLE {table}")
                create_table_stmt = source_cursor.fetchone()['Create Table']

                # Thực thi câu lệnh tạo bảng trên database đích
                target_cursor.execute(create_table_stmt)
                self.logger.info(f"✅ Đã copy schema cho bảng: {table}")

            target_conn.commit()
        except Exception as e:
            self.logger.error(f"Lỗi migrate schema: {e}")
            target_conn.rollback()
        finally:
            source_cursor.close()
            target_cursor.close()
            source_conn.close()
            target_conn.close()

    def migrate_data(self, source_db, target_db, chunk_size=10000):
        """
        Di chuyển dữ liệu từng phần để tránh overload
        
        :param source_db: Tên database nguồn
        :param target_db: Tên database đích
        :param chunk_size: Số lượng bản ghi di chuyển mỗi lần
        """
        # Tạo SQLAlchemy engine để hỗ trợ migrate dữ liệu hiệu quả
        source_engine = create_engine(
            f"mysql+mysqlconnector://{self.source_config['user']}:{self.source_config['password']}@{self.source_config['host']}:{self.source_config['port']}/{source_db}"
        )
        target_engine = create_engine(
            f"mysql+mysqlconnector://{self.target_master_config['user']}:{self.target_master_config['password']}@{self.target_master_config['host']}:{self.target_master_config['port']}/{target_db}"
        )

        try:
            # Lấy danh sách bảng
            source_conn = source_engine.connect()
            tables = self.get_all_tables(source_engine.raw_connection())

            for table in tables:
                self.logger.info(f"🚀 Bắt đầu migrate bảng: {table}")
                
                # Đếm tổng số bản ghi để chia chunk
                count_query = f"SELECT COUNT(*) as total FROM {table}"
                total_records = pd.read_sql(count_query, source_engine).iloc[0]['total']
                
                start_time = time.time()
                
                # Migrate từng chunk
                for offset in range(0, total_records, chunk_size):
                    query = f"SELECT * FROM {table} LIMIT {chunk_size} OFFSET {offset}"
                    df = pd.read_sql(query, source_engine)
                    
                    # Ghi vào database đích
                    df.to_sql(table, target_engine, if_exists='append', index=False)
                
                end_time = time.time()
                self.logger.info(f"✅ Hoàn thành migrate bảng {table}: {total_records} bản ghi, {end_time - start_time:.2f} giây")

        except Exception as e:
            self.logger.error(f"Lỗi migrate dữ liệu: {e}")
        finally:
            source_conn.close()

def main():
    # Cấu hình database nguồn (thay đổi theo môi trường của bạn)
    source_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '123456',
        'database': 'sample_migration_db'
    }

    # Cấu hình database master đích (sử dụng cấu hình từ docker-compose)
    target_master_config = {
        'host': 'localhost',
        'port': 3308,  
        'user': 'root',
        'password': '123456'
    }

    # Tên database nguồn và đích
    source_db = 'sample_migration_db'
    target_db = 'migrated_database'

    migrator = DatabaseMigrator(source_config, target_master_config)
    
    print("🔄 Bắt đầu quá trình migration...")
    
    # Migrate schema trước
    migrator.migrate_schema(source_db, target_db)
    
    # Migrate dữ liệu
    migrator.migrate_data(source_db, target_db)
    
    print("✅ Migration hoàn tất!")

if __name__ == "__main__":
    main()