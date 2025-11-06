from pymysql import Connection
# all_data = "file_address"
conn = Connection(
    host='localhost',
    port=3306,
    user='py_remote',
    password='426312',
    database='cast',
    )
print('Connected to MySQL')
create_table_sql = """
CREATE TABLE all_data (
    order_date DATE,
    order_id VARCHAR(50) PRIMARY KEY,
    money DECIMAL(10, 2),
    province VARCHAR(50)
);
"""

cursor = conn.cursor()
cursor.execute(create_table_sql)
print('Table created successfully')
# conn.select_db("cast")
#     # for record in all_data:
#     #     sql_operation = (f"insert into oders(order_date, order_id, money,province)"
#     #                      f"values ('%s', '%s', '%s', '%s')")
#     #     cursor.execute(sql_operation,(record.date, record.id, record.money, record.province))
#     #
#     # result_file = open("file_address", "w", encoding="utf-8")
#     # cursor.execute("select * from orders")
#     # results:tuple = cursor.fetchall()
#     # for res in results:
#     #     res_dict = {"date":str(res[0]), "order_id":res[1],"money": res[2], "province": res[3]}
#     #     result_file.write(str(res_dict) + "\n")
#     # result_file.close()
# except Exception as e:
#     print(f'Error: {e}')
conn.close()