import cx_Oracle


class Alterspace():
    def __init__(self):
        '''
        初始化连接数据库
        '''
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
            moniter = cx_Oracle.makedsn(self.host, 1521, self.dbname)
            connection = cx_Oracle.connect(self.user, self.pwd, moniter)
        except Exception as e:
            print('警告警告！！！数据库连接信息输入错误，请重新输入！！！')
        self.cursor = connection.cursor()
        print('数据库连接成功！！！')

    def queryspace(self):
        '''
        查询数据库内所有表空间的占用率、可用内存等信息
        '''
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
            # print('警告警告！！！请重试！！！')
            print(e)
        return results

    def extendspace(self):
        '''
        根据queryspace函数查询返回的结果
        对其结果进行遍历
        筛选出需要扩容的表空间
        '''
        self.queryspace()
        global filename, loca, size
        filename = []
        loca = ()
        size = []
        for sheet in results:
            print(sheet[0] + '表空间的占用率为：' + sheet[3] + '%,\t剩余空闲空间为：' + str(sheet[4]) + 'MB.')
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
            location = loc + f'{str.lower(sheet[0])}{count[0][0] + 1}' + '.dbf'
            location = (location,)
            measure = sheet[1] / 10
            if float(sheet[3]) > 80 and float(sheet[4]) < 100:
                filename.append(sheet[0])
                loca = loca + location
                size.append(measure)
        if len(filename)==0:
            print("没有表空间需要扩容！！！")

    def createdatafile(self):
        '''
        根据extendspace函数内筛选出的需要扩容的表空间对其进行扩容
        扩容方式为增加数据问价
        '''
        self.extendspace()
        for i in range(len(filename)):
            sql = f'''alter tablespace {filename[i]} add datafile '{loca[i]}' size {int(size[i])+1}m'''
            self.cursor.execute(sql)
            print(f"{filename[i]}表空间扩容{int(size[i])+1}mb成功！！！\n")


ca = Alterspace()
ca.createdatafile()
