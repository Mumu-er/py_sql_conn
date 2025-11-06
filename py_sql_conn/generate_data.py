import os
import mysql.connector
from faker import Faker
from random import uniform, randint
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')  # 注意这里使用了 .env 中的变量名
DB_NAME = os.getenv('DB_NAME')

# --- 数据库连接信息 ---
config = {
    'host': DB_HOST,  # "localhost" 或 服务器IP
    'user': DB_USER,  # 具有 INSERT 权限的用户
    'password': DB_PASSWORD,  # 用户密码
    'database': DB_NAME  # 目标数据库
}
if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
    raise ValueError("数据库配置信息 (HOST, USER, PASSWORD, NAME) 缺失。请检查 .env 文件和 dotenv 导入。")

# --- 数据生成配置 ---
NUM_RECORDS = 100
# 限制订单日期在过去一年内
START_DATE = datetime.now() - timedelta(days=365)
END_DATE = datetime.now()
# 确保订单 ID 唯一
used_order_ids = set()

# 初始化 Faker，使用中文区域设置
fake = Faker('zh_CN')

def generate_fake_data(num_records):
    """生成包含 order_date, order_id, money, province 的随机数据"""
    data = []

    # 中国的主要省份、自治区和直辖市
    PROVINCES = [
        "北京", "天津", "上海", "重庆",
        "河北", "山西", "辽宁", "吉林", "黑龙江", "江苏",
        "浙江", "安徽", "福建", "江西", "山东", "河南",
        "湖北", "湖南", "广东", "海南", "四川", "贵州",
        "云南", "陕西", "甘肃", "青海", "台湾",
        "内蒙古", "广西", "西藏", "宁夏", "新疆",
        "香港", "澳门"
    ]

    for _ in range(num_records):
        # 1. order_date (遵守 DATE 格式)
        random_date = fake.date_between(start_date=START_DATE, end_date=END_DATE)

        # 2. order_id (具有唯一性)
        while True:
            # 生成一个唯一的随机 ID (例如: O-20251105-00123)
            # 使用时间戳的一部分和随机数，确保唯一性高
            order_id = f"O-{datetime.now().strftime('%Y%m%d')}-{randint(10000, 99999)}"
            if order_id not in used_order_ids:
                used_order_ids.add(order_id)
                break

        # 3. money (随机金额，DECIMAL(10, 2) 格式)
        random_money = round(uniform(10.00, 5000.99), 2)

        # 4. province (遵守中国省份)
        random_province = fake.random_element(PROVINCES)

        data.append((random_date, order_id, random_money, random_province))

    return data


def insert_data_to_mysql(data):
    """批量插入数据到 MySQL"""

    # 确保表名和列名与您创建的一致
    sql = "INSERT INTO all_data (order_date, order_id, money, province) VALUES (%s, %s, %s, %s)"

    mydb = None
    mycursor = None
    try:
        print("尝试连接数据库...")
        mydb = mysql.connector.connect(**config)
        mycursor = mydb.cursor()

        print(f"正在批量插入 {len(data)} 条数据...")
        # executemany 批量执行插入，效率更高
        mycursor.executemany(sql, data)

        # 提交事务
        mydb.commit()

        print(f"成功插入 {mycursor.rowcount} 条记录到 cast.all_data 表。")

    except mysql.connector.Error as err:
        print(f"插入数据失败: {err}")
        if mydb and mydb.is_connected():
            mydb.rollback()  # 发生错误时回滚事务

    finally:
        # 关闭连接
        if mycursor:
            mycursor.close()
        if mydb and mydb.is_connected():
            mydb.close()
            print("连接已关闭。")


if __name__ == "__main__":
    fake_data = generate_fake_data(NUM_RECORDS)
    insert_data_to_mysql(fake_data)