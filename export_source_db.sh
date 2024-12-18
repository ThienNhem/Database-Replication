#!/bin/bash

# Thông tin database nguồn
SOURCE_HOST="localhost"
SOURCE_PORT="3307"
SOURCE_USER="root"
SOURCE_PASSWORD="123456"
SOURCE_DB="source_db"

# File dump
DUMP_FILE="source_db_dump.sql"

# Xuất dữ liệu
echo "⏳ Đang xuất dữ liệu từ database nguồn..."
mysqldump -h $SOURCE_HOST -P $SOURCE_PORT -u$SOURCE_USER -p$SOURCE_PASSWORD $SOURCE_DB > $DUMP_FILE

if [ $? -eq 0 ]; then
    echo "✅ Dữ liệu đã được xuất ra file $DUMP_FILE"
else
    echo "❌ Lỗi khi xuất dữ liệu!"
    exit 1
fi
