import pymysql
from common.config import mysql_userid, mysql_ip, mysql_passwd, mysql_dataset
from utils.singleton import singleton


@singleton
class DatasetMysql:
    def __init__(self):
        super().__init__()
        self.db_ = pymysql.connect(host=mysql_ip, user=mysql_userid, password=mysql_passwd, database=mysql_dataset)

    def __del__(self):
        self.db_.close()

    def execute(self, sql_txt):
        try:
            with self.db_.cursor() as cursor:
                cursor.execute(sql_txt)
                self.db_.commit()
                cursor.close()
        except Exception as e:
            print(e)
            self.db_.rollback()

    def query(self, sql_txt):
        with self.db_.cursor() as cursor:
            cursor.execute(sql_txt)
            # self.db_.commit()
            data = cursor.fetchall()
            cursor.close()
            return data

    def execute_if_exist(self, code, time, sql_txt):
        if not self.if_exist(code, time):
            self.execute(sql_txt)
            return True
        else:
            # print("it has existed!")
            return False

    def if_exist(self, code, time):
        sql_txt = "select * from security_price where code = \"" + str(code) + "\" and time = \"" + str(
            time) + "\";"
        data = self.query(sql_txt)
        if len(data) > 0:
            return True
        return False
