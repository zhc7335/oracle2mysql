import sys
import csv
import pandas as pd
import pymysql
import os


class Excel2mysql():
    def __init__(self):
        print('欢迎客官莅临！！！撒花！！！')
        host = input("请输入数据库服务器IP：")
        user = input("请输入您的数据库用户名：")
        pwd = input("请输入您的用户密码：")
        dbname = input("请输入您想连接的数据库名称：")
        self.host = str(host)
        self.user = str(user)
        self.pwd = str(pwd)
        self.dbname = str(dbname)
        try:
            connection = pymysql.connect(self.host, self.user, self.pwd, self.dbname)
            connection.autocommit(1)
            self.cursor = connection.cursor()
            print('恭喜客官！！！数据库连接成功啦！！！')
        except Exception as e:
            print('警告警告！！！数据库连接信息输入错误，请重新输入！！！')
            print(e)

    def create_table_sql(self, df):
        columns = df.columns.tolist()
        types = df.dtypes
        # 添加id 制动递增主键模式
        make_table = []
        for item in columns:
            if 'int' in str(df[item].dtype):
                char = item + ' INT'
            elif 'float' in str(df[item].dtype):
                char = item + ' FLOAT'
            elif 'object' in str(df[item].dtype):
                char = item + ' VARCHAR(255)'
            elif 'datetime' in str(df[item].dtype):
                char = item + ' DATETIME'
            make_table.append(char)
        return ','.join(make_table)

    # csv 格式输入 mysql 中
    def excel2mysql(self):
        path = os.getcwd()
        files = os.listdir(path)
        # dateparse = lambda dates: pd.datetime.strptime(dates, '%Y/%m/%d %H:%M:%S')
        for file in files:
            if file.split('.')[-1] in ['xlsx', 'xls']:
                filename = file.split('.')[0]
                data_xls = pd.io.excel.ExcelFile(file)
                data = {}
                # print(data_xls.sheet_names)
                for name in data_xls.sheet_names:
                    # print(name)
                    df = pd.read_excel(data_xls, sheet_name=name)
                    # data[name] = df
                    # print(df)
                    self.cursor.execute('DROP TABLE IF EXISTS {}'.format(name))
                    self.cursor.execute('CREATE TABLE {}({})'.format(name, self.create_table_sql(df)))
                    print('恭喜客官！！！%s表单创建成功！！！' % name)
                    # 提取数据转list 这里有与pandas时间模式无法写入因此换成str 此时mysql上格式已经设置完成
                    # df['HIREDATE'] = df['HIREDATE'].astype('str')
                    sb = ','.join(['%s' % df.columns[y] for y in range(len(df.columns))])
                    z = ','.join('%s' for y in range(len(df.columns)))
                    values = df.values.tolist()
                    sql = "INSERT INTO {}({}) VALUES (%s)".format(name, sb) % z
                    # print(sql)
                    print('客官稍等，数据正在导入······')
                    # 批量导入数据库，executemany批量操作 插入数据 批量操作比逐个操作速度快很多
                    self.cursor.executemany(sql, values)
                    # cursor.executemany('INSERT INTO {} ({}) VALUES ({})'.format(table_name, sb), values)
                    print("恭喜客官！！！数据导入成功！！！")
