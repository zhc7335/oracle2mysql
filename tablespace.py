import cx_Oracle


class Alterspace():
    def __init__(self):
        host = input("请输入数据库服务器IP：")
        user = input("请输入您的数据库用户名：")
        pwd = input("请输入您的用户密码：")
        dbname = input("请输入您想连接的数据库名称：")
        self.host = str(host)
        self.user = str(user)
        self.pwd = str(pwd)
        self.dbname = str(dbname)
        try:
            moniter = cx_Oracle.makedsn(self.host, 1521, self.dbname)
            connection = cx_Oracle.connect(self.user, self.pwd, moniter)
        except Exception as e:
            print('警告警告！！！数据库连接信息输入错误，请重新输入！！！')
        self.cursor = connection.cursor()
        print('数据库连接成功！！！')

    def queryspace(self):
        try:

            sql = '''
                        SELECT UPPER(F.TABLESPACE_NAME) "表空间名",
D.TOT_GROOTTE_MB "表空间大小(M)",
D.TOT_GROOTTE_MB - F.TOTAL_BYTES "已使用空间(M)",
TO_CHAR(ROUND((D.TOT_GROOTTE_MB - F.TOTAL_BYTES) / D.TOT_GROOTTE_MB * 100,2),'990.99') "使用比",
F.TOTAL_BYTES "空闲空间(M)",
F.MAX_BYTES "最大块(M)"
FROM (SELECT TABLESPACE_NAME,
ROUND(SUM(BYTES) / (1024 * 1024), 2) TOTAL_BYTES,
ROUND(MAX(BYTES) / (1024 * 1024), 2) MAX_BYTES
FROM SYS.DBA_FREE_SPACE
GROUP BY TABLESPACE_NAME) F,
(SELECT DD.TABLESPACE_NAME,
ROUND(SUM(DD.BYTES) / (1024 * 1024), 2) TOT_GROOTTE_MB
FROM SYS.DBA_DATA_FILES DD
GROUP BY DD.TABLESPACE_NAME) D
WHERE D.TABLESPACE_NAME = F.TABLESPACE_NAME
ORDER BY 4 DESC
                                      '''
            self.cursor.execute(sql)
            global results
            results = self.cursor.fetchall()
        except Exception as e:
            print(e)
        return results

    def extendspace(self):
        self.queryspace()
        for sheet in results:
            print(sheet[0] + '表空间的占用率为：' + sheet[3] + '%,\t剩余空闲空间为：' + str(sheet[4]) + 'MB.')
            if float(sheet[3]) > 10 and float(sheet[4]) < 300:
                request = input(f"{sheet[0]}表空间内存占用过高！！!请问是否进行扩容（y/n)：")
                if request == 'y':
                    # 查询需要增加空间的表空间对应的数据文件，然后构造新增数据文件的路径
                    sql1 = f'''SELECT file_name,round(bytes / (1024 * 1024), 0) total_space
FROM dba_data_files where tablespace_name='{sheet[0]}'
ORDER BY tablespace_name'''
                    self.cursor.execute(sql1)
                    result = self.cursor.fetchall()
                    loc = result[0][0]
                    loc = loc.split('/')
                    loc = '/'.join(loc[-len(loc):-1]) + '/'
                    # 计算对应表空间已有的数据文件个数，对新增数据文件进行命名
                    sql2 = f'''select count(*) from dba_data_files where tablespace_name='{sheet[0]}' '''
                    self.cursor.execute(sql2)
                    count = self.cursor.fetchall()
                    # location是新增数据文件的路径,size是新建数据文件的大小
                    global location, size, tablespace_name
                    location = loc + f'{str.lower(sheet[0])}{count[0][0] + 1}' + '.dbf'
                    size = sheet[1] / 10
                    tablespace_name = sheet[0]
                    sql3 = f'''alter tablespace {tablespace_name} add datafile '{location}' size {int(size)+1}m'''
                    self.cursor.execute(sql3)
                    print("扩容成功！！！")
                    print("====================华丽的分割线========================")
                    # print(sql3)
        # return tablespace_name, location, size


ca = Alterspace()
ca.extendspace()
