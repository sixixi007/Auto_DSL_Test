#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import xlrd
import os
import csv
import psycopg2
import time
import datetime
import logging
from logging import handlers


class AutoGenerateBaseCsv(object):
    """自动更新csv"""
    def __init__(self):
        self.excel_table_list = list()
        self.db_excel_data_dict = dict()

    def auto_generate_csv(self):
        """获取excel中所有的表名"""
        book = xlrd.open_workbook("./data.xlsx")
        sheet_name_list = book.sheet_names()
        for sheet_name in sheet_name_list:
            log.logger.info(f"sheet的名字是 {sheet_name}")
            if sheet_name == 'bugs':
                continue
            sheet = book.sheet_by_name(sheet_name)
            # 获取行数和列数
            rows = sheet.nrows
            colums = sheet.ncols
            first_row_list = [sheet.cell(0, c).value for c in range(0, colums)]
            var_index = first_row_list.index("落库变量")
            table_field_list = first_row_list[:var_index]
            for table in table_field_list:
                real_table = '.'.join(table.split('.')[:-1])
                if real_table in self.excel_table_list:
                    continue
                self.excel_table_list.append(real_table)

    def find_key_value_db(self):
        """从数据库中获取base数据"""
        conn = psycopg2.connect(database="hdr_cdss_medtest", user="sixixi", password="908h8usd8f2", host="172.16.0.20",
                                port=5432)
        if 'cases.case_diagnose' in self.excel_table_list:
            if 'cases.case_base' not in self.excel_table_list:
                self.excel_table_list.append('cases.case_base')
        for table in self.excel_table_list:
            db_key_value_list = list()
            tt = table.split('.')
            select_sql_schema_header = """select * from information_schema.columns 
                        where table_schema='{}' and table_name='{}';""".format(tt[0], tt[1])
            select_sql = """select * from {} limit 2;""".format(table)
            print(select_sql)
            cursor = conn.cursor()
            time.sleep(1.5)
            cursor.execute(select_sql_schema_header)
            time.sleep(1.5)
            table_list_db = cursor.fetchall()
            time.sleep(1.5)
            cursor.execute(select_sql)
            time.sleep(1.5)
            db_value_list = cursor.fetchone()
            final_db_value_list = list()
            final_db_key_list = list()
            for key_list in table_list_db:
                if len(key_list) >= 4:
                    final_db_key_list.append(key_list[3])
                else:
                    print("不满足条件的数据库字段:", key_list)
            db_key_value_list.append(final_db_key_list)
            for db_value in db_value_list:
                if not db_value:
                    db_value = 'nan'
                if isinstance(db_value, list):
                    db_value = []
                if isinstance(db_value, dict):
                    db_value = {}
                if db_value == '' or db_value == ' ':
                    db_value = '1'
                # if type(db_value) is datetime.datetime:
                #     db_value = datetime.datetime.strftime(db_value, '%Y-%m-%d %H:%M:%S')
                final_db_value_list.append(db_value)
            db_key_value_list.append(final_db_value_list)
            self.db_excel_data_dict[table] = db_key_value_list
        # 关闭游标
        cursor.close()
        # 关闭数据库连接
        conn.close()

    def save_to_csv(self):
        """将查询到的最新base数据写入csv"""
        if not os.path.exists(new_base_insert_csv):
            os.mkdir(new_base_insert_csv)
        print(self.db_excel_data_dict)
        for k, v in self.db_excel_data_dict.items():
            with open(f'{new_base_insert_csv}/{k}.csv', 'w', encoding='utf-8', newline='') as excel_db_file:
                for all_v in v:
                    if any(all_v):
                        ww = csv.writer(excel_db_file, delimiter='\t')
                        ww.writerow(all_v)



class Logger(object):
    # 日志级别关系映射
    level_relations = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'crit': logging.CRITICAL
    }

    def __init__(self, filename, level='info', when='D', backCount=3,
                 fmt='%(asctime)s: %(message)s'):
        self.logger = logging.getLogger(filename)
        # 设置日志格式
        format_str = logging.Formatter(fmt)
        # 设置日志级别
        self.logger.setLevel(self.level_relations.get(level))
        # 往屏幕上输出
        sh = logging.StreamHandler()
        # 设置屏幕上显示的格式
        sh.setFormatter(format_str)
        # 往文件里写入#指定间隔时间自动生成文件的处理器
        th = handlers.TimedRotatingFileHandler(filename=filename, when=when, backupCount=backCount,
                                               encoding='utf-8')
        # 设置文件里写入的格式
        th.setFormatter(format_str)
        # 把对象加到logger里
        self.logger.addHandler(sh)
        self.logger.addHandler(th)


if __name__ == '__main__':
    log = Logger('summary.log', level='info')
    src_path = './base_insert_data'
    dst_path = './new_insert_data'
    new_base_insert_csv = './new_base_insert_csv'
    visit_record_path = 'visit.visit_record.csv'
    new_visit_path = './new_visit_record'
    command_csv = input("请输入是否要执行更新表的base_csv文件：(y/n)")
    command_csv = command_csv.lower()
    if command_csv == 'y' or command_csv == 'yes':
        auto_g = AutoGenerateBaseCsv()
        auto_g.auto_generate_csv()
        log.logger.info(f'要进行更新的表如下：{auto_g.excel_table_list}')
        auto_g.find_key_value_db()
        auto_g.save_to_csv()
    else:
        log.logger.info("##此次没有做表的base_csv文件的更新操作##")
    log.logger.info("请不要忘记检查下base csv文件的光标是否在下一行,再进行dsl自动化测试")
    continue_test = input("请输入是否继续dsl测试:(y/n)")
    continue_test = continue_test.lower()
    if continue_test == 'y' or 'yes':
        log.logger.info("*******************测试开始*******************")



