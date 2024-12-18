#!/bin/bash

# Thông tin master
MASTER_HOST="localhost"
MASTER_PORT="3308"
MASTER_USER="root"
MASTER_PASSWORD="123456"
MASTER_DB="source_db"

# File dump
DUMP_FILE="source_db_dump.sql"

# Tạo database trên master
echo "⏳ Tạo database trên master..."
mysql -h $MASTER_HOST -P $MASTER_PORT -u$MASTER_USER -p$MASTER_PASSWORD -e "CREATE DATABASE IF NOT EXISTS $MASTER_DB"

# Nhập dữ liệu vào master
echo "⏳ Đang nhập dữ liệu vào master..."
mysql -h $MASTER_HOST -P $MASTER_PORT -u$MASTER_USER -p$MASTER_PASSWORD $MASTER_DB < $DUMP_FILE

if [ $? -eq 0 ]; then
    echo "✅ Dữ liệu đã được nhập vào master."
else
    echo "❌ Lỗi khi nhập dữ liệu!"
    exit 1
fi
