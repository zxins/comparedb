# -*- coding: utf-8 -*-
import sys
import json
import pymysql
from functools import reduce
from urllib.parse import urlparse


def connect_db(**option):
    """ 连接db """
    db = pymysql.connect(
        host=option['host'],
        port=int(option['port']),
        user=option['username'],
        password=option['password'],
        database=option['database']
    )
    return db



def table_rows_info(option):
    """获取数据库表名及表行数

    :param option: 数据库连接参数
    :return: [{表名: 行数}, ...]
    """
    db = connect_db(**option)
    try:
        cursor = db.cursor()
        cursor.execute("show tables")
        info = {}
        for row in cursor.fetchall():
            table_name = row[0]
            sql = "select count(*) from `{0}`".format(table_name)
            cursor.execute(sql)
            row_count = cursor.fetchone()[0]
            info[str(table_name)] = row_count

        return info

    finally:
        db.close()


def print_diff_info(databases):
    """ 控制台打印db对比信息 """
    table_infos = [db_info['table_info'] for db_info in databases]
    inter = list(reduce(lambda x, y: set(x).intersection(y), table_infos))
    inter.sort()
    diff = list(reduce(lambda x, y: set(x).symmetric_difference(y), table_infos))
    diff.sort()

    print("{0: ^55} {1: ^55}".format(databases[0]['alias'], databases[1]['alias']))
    print("{0:=^100}".format("表名:行数"))
    for row in inter:
        count1 = databases[0]['table_info'].get(row)
        count2 = databases[1]['table_info'].get(row)
        text0 = "" if count1 == count2 else "\033[0;31;999m"
        text1 = "{0: >30}: {1: <12}".format(row, count1)
        text2 = "{0: >42}: {1: <12}".format(row, count2)
        text3 = "" if count1 == count2 else "<--\033[0m"    # 行数不同的用<--指向并标红
        print("{0}{1}{2}{3}".format(text0,text1, text2, text3))

    if diff:
        print("\n{0:=^98}".format("可能缺少的表"))
        for row in diff:
            count1 = databases[0]['table_info'].get(row, 'NULL')
            count2 = databases[1]['table_info'].get(row, 'NULL')
            text0 = "" if count1 == count2 else "\033[0;31;99m"
            text1 = "{0: >30}: {1: <12}".format(row, count1)
            text2 = "{0: >42}: {1: <12}".format(row, count2)
            text3 = "" if count1 == count2 else "<--\033[0m"
            print("{0}{1}{2}{3}".format(text0, text1, text2, text3))

def read_params_from_json():
    """ 从json文件读数据库连接参数 """
    import os
    assert os.path.exists('./params.json'), "请先配置params.json文件"

    with open('./params.json', 'r') as f:
        options = json.load(f)
        return options


def read_params_from_argv():
    """ 从执行命令读取数据库连接参数 """
    options = []
    for i in range(1, 3):
        url = sys.argv[i] if '//' in sys.argv[i] else 'mysql://' + sys.argv[i]  # 防止入参不带//解析不到正确的地址
        parser = urlparse(url)
        option = {
            'host': parser.hostname,
            'port': parser.port,
            'username': parser.username,
            'password': parser.password,
            'database': parser.path.replace('/', ''),
        }
        options.append(option)
    return options


def do(options):
    """ 执行比较db方法 """
    databases = []
    # WARNING: 目前仅支持两个db比较, 需要比较多个时可以尝试引入pandas
    for index in range(2):
        db_info = {'alias': options[index].get('alias', '地址' + str(index+1))}   # alias: db别名
        table_info = table_rows_info(options[index])
        db_info['table_info'] = table_info
        databases.append(db_info)
    print_diff_info(databases)



if len(sys.argv) > 2:
    # 带参运行示例：
    # $ python diff.py  mysql://root:123456@127.0.0.1:3306/test1  mysql://root:123456@127.0.0.1:3306/test2
    options = read_params_from_argv()
else:
    options = read_params_from_json()
do(options)
