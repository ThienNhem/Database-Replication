import mysql.connector
import random
import string

def setup_source_database():
    config = {
        'host': 'localhost',
        'port': 3306,  
        'user': 'root',
        'password': '123456'
    }
    
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    
    try:
        # Tạo database và bảng mẫu
        cursor.execute("DROP DATABASE IF EXISTS source_db")
        cursor.execute("CREATE DATABASE source_db")
        cursor.execute("USE source_db")
        cursor.execute("""
            CREATE TABLE sample_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                value INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Chèn dữ liệu mẫu
        for _ in range(1000):  # Tạo 1000 bản ghi
            name = ''.join(random.choices(string.ascii_letters, k=10))
            value = random.randint(1, 100)
            cursor.execute("INSERT INTO sample_data (name, value) VALUES (%s, %s)", (name, value))
        
        conn.commit()
        print("✅ Đã tạo database và thêm dữ liệu mẫu.")
    except Exception as e:
        print(f"❌ Lỗi khi tạo database nguồn: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    setup_source_database()
