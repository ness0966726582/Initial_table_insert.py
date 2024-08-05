import psycopg2
from psycopg2 import sql
import uuid
from datetime import date

# DB 資訊
my_serverIP = "10.231.220.60"
my_DBName = "etlv1"
my_login_userName = "postgres"
my_login_password = ""
my_port = "5432"  # PostgreSQL 默認埠

# 連接到 PostgreSQL 資料庫
conn = psycopg2.connect(
    dbname=my_DBName,
    user=my_login_userName,
    password=my_login_password,
    host=my_serverIP,
    port=my_port
)

# 建立游標
cur = conn.cursor()

# 判斷資料表是否存在的 SQL 語句
check_table_exists_query = '''
SELECT EXISTS (
    SELECT FROM information_schema.tables 
    WHERE table_schema = 'public'
    AND table_name = 'assetsTable'
);
'''

# 執行查詢語句
cur.execute(check_table_exists_query)
table_exists = cur.fetchone()[0]

if table_exists:
    print("資料表已存在，程式結束。")
else:
    # 建立資料表的 SQL 語句
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS assetsTable (
        id UUID PRIMARY KEY,
        asset_code VARCHAR(50) NOT NULL,
        item_name VARCHAR(100) NOT NULL,
        storage_location VARCHAR(100) NOT NULL,
        cost_unit VARCHAR(100) NOT NULL,
        owner VARCHAR(100) NOT NULL,
        gmail_account VARCHAR(100) NOT NULL,
        update_time DATE NOT NULL
    );
    '''

    # 執行建表語句
    cur.execute(create_table_query)
    conn.commit()

    # 預設資料
    default_data = [
        ('11CC000037', 'PC', 'annex', 'IT', 'name1', 'ness_huang@mail.bbiclark.com', date(2024, 8, 5)),
        ('11CC000038', 'PC', 'hotel', 'IT', 'name2', 'ness_huang@mail.bbiclark.com', date(2024, 8, 5)),
        ('11CC000039', 'PC', 'casino', 'IT', 'name3', 'ness_huang@mail.bbiclark.com', date(2024, 8, 5)),
        ('11CC000040', 'PC', 'aqua', 'IT', 'name4', 'ness_huang@mail.bbiclark.com', date(2024, 8, 5)),
    ]

    # 插入預設資料的 SQL 語句
    insert_data_query = '''
    INSERT INTO assetsTable (id, asset_code, item_name, storage_location, cost_unit, owner, gmail_account, update_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    '''

    # 插入資料
    for data in default_data:
        cur.execute(insert_data_query, (str(uuid.uuid4()), *data))

    conn.commit()
    print("資料表建立完成並插入預設資料。")

# 關閉游標和連接
cur.close()
conn.close()
