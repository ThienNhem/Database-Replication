import mysql.connector
import random
import faker
import datetime

class DatabaseSetup:
    def __init__(self, host='localhost', port=3306, user='root', password='123456'):
        """
        Khởi tạo kết nối database
        
        :param host: Địa chỉ host database
        :param port: Cổng kết nối
        :param user: Tên người dùng
        :param password: Mật khẩu
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.fake = faker.Faker('vi_VN')  

    def _get_connection(self, database=None):
        """
        Tạo kết nối đến database
        
        :param database: Tên database (optional)
        :return: Kết nối database
        """
        try:
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=database
            )
            return conn
        except Exception as e:
            print(f"Lỗi kết nối: {e}")
            return None

    def create_database(self, db_name='sample_migration_db'):
        """
        Tạo database mới
        
        :param db_name: Tên database
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Xóa database cũ nếu tồn tại
            cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
            
            # Tạo database mới
            cursor.execute(f"CREATE DATABASE {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            print(f"✅ Tạo database {db_name} thành công")
        except Exception as e:
            print(f"❌ Lỗi tạo database: {e}")
        finally:
            cursor.close()
            conn.close()

    def create_tables(self, db_name='sample_migration_db'):
        """
        Tạo các bảng mẫu
        
        :param db_name: Tên database
        """
        conn = self._get_connection(db_name)
        cursor = conn.cursor()
        
        try:
            # Bảng nhân viên
            cursor.execute("""
            CREATE TABLE employees (
                id INT AUTO_INCREMENT PRIMARY KEY,
                full_name VARCHAR(100),
                email VARCHAR(100),
                phone VARCHAR(20),
                department VARCHAR(50),
                salary DECIMAL(10,2),
                hire_date DATE
            )
            """)
            
            # Bảng sản phẩm
            cursor.execute("""
            CREATE TABLE products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                product_name VARCHAR(100),
                category VARCHAR(50),
                price DECIMAL(10,2),
                stock_quantity INT
            )
            """)
            
            # Bảng đơn hàng
            cursor.execute("""
            CREATE TABLE orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                employee_id INT,
                product_id INT,
                quantity INT,
                total_price DECIMAL(10,2),
                order_date DATETIME,
                FOREIGN KEY (employee_id) REFERENCES employees(id),
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
            """)
            
            conn.commit()
            print("✅ Tạo các bảng thành công")
        except Exception as e:
            print(f"❌ Lỗi tạo bảng: {e}")
        finally:
            cursor.close()
            conn.close()

    def generate_mock_data(self, db_name='sample_migration_db', num_employees=100, num_products=50, num_orders=500):
        """
        Sinh dữ liệu mẫu
        
        :param db_name: Tên database
        :param num_employees: Số lượng nhân viên
        :param num_products: Số lượng sản phẩm
        :param num_orders: Số lượng đơn hàng
        """
        conn = self._get_connection(db_name)
        cursor = conn.cursor()

        departments = ['Kỹ thuật', 'Kinh doanh', 'Nhân sự', 'Marketing', 'Tài chính']
        product_categories = ['Điện tử', 'Quần áo', 'Sách', 'Thực phẩm', 'Đồ gia dụng']

        try:
            # Sinh dữ liệu nhân viên
            employees = []
            for _ in range(num_employees):
                full_name = self.fake.name()
                email = self.fake.email()
                phone = self.fake.phone_number()
                department = random.choice(departments)
                salary = round(random.uniform(5000000, 50000000), 2)
                hire_date = self.fake.date_between(start_date='-5y', end_date='today')
                
                cursor.execute("""
                INSERT INTO employees 
                (full_name, email, phone, department, salary, hire_date) 
                VALUES (%s, %s, %s, %s, %s, %s)
                """, (full_name, email, phone, department, salary, hire_date))
                employees.append(cursor.lastrowid)

            # Sinh dữ liệu sản phẩm
            products = []
            for _ in range(num_products):
                product_name = self.fake.catch_phrase()
                category = random.choice(product_categories)
                price = round(random.uniform(10000, 5000000), 2)
                stock_quantity = random.randint(10, 1000)
                
                cursor.execute("""
                INSERT INTO products 
                (product_name, category, price, stock_quantity) 
                VALUES (%s, %s, %s, %s)
                """, (product_name, category, price, stock_quantity))
                products.append(cursor.lastrowid)

            # Sinh dữ liệu đơn hàng
            for _ in range(num_orders):
                employee_id = random.choice(employees)
                product_id = random.choice(products)
                quantity = random.randint(1, 10)
                product_price = cursor.execute("SELECT price FROM products WHERE id = %s", (product_id,))
                price = cursor.fetchone()[0]
                total_price = quantity * price
                order_date = self.fake.date_time_between(start_date='-1y', end_date='now')
                
                cursor.execute("""
                INSERT INTO orders 
                (employee_id, product_id, quantity, total_price, order_date) 
                VALUES (%s, %s, %s, %s, %s)
                """, (employee_id, product_id, quantity, total_price, order_date))

            conn.commit()
            print("✅ Sinh dữ liệu mẫu hoàn tất")
            print(f"   - Số nhân viên: {num_employees}")
            print(f"   - Số sản phẩm: {num_products}")
            print(f"   - Số đơn hàng: {num_orders}")
        
        except Exception as e:
            print(f"❌ Lỗi sinh dữ liệu: {e}")
        finally:
            cursor.close()
            conn.close()

def main():
    # Tạo đối tượng DatabaseSetup
    db_setup = DatabaseSetup()
    
    # Tên database
    db_name = 'sample_migration_db'
    
    # Thực hiện các bước
    db_setup.create_database(db_name)
    db_setup.create_tables(db_name)
    db_setup.generate_mock_data(db_name)

if __name__ == "__main__":
    main()
    