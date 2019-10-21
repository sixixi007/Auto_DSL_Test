#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import xlrd
import os
import shutil
import csv
import pandas as pd
import copy
import psycopg2
import time
import requests
import json
import logging
from logging import handlers


class AutoDSL(object):
    def __init__(self):
        self.summary_result_list = list()

    def get_csv_content(self, patient_id):
        """存入CSV文件"""
        book = xlrd.open_workbook("./data.xlsx")
        sheet_name_list = book.sheet_names()
        if not os.path.exists('./new_insert_data'):
            os.mkdir('./new_insert_data')
        for sheet_name in sheet_name_list:
            sheet_summary_dict = dict()
            log.logger.info(f"sheet的名字是 {sheet_name}")
            if sheet_name == 'bugs':
                continue
            sheet = book.sheet_by_name(sheet_name)
            # 获取行数和列数
            rows = sheet.nrows
            colums = sheet.ncols
            # print("colums:", colums)
            # 只获取落库变量前的所有数据库表字段
            first_row_list = [sheet.cell(0, c).value for c in range(1, colums)]
            visit_record_field = sheet.cell(0, 0).value
            # print("visit_record_field:", visit_record_field)
            visit_record_values_list = [int(sheet.cell(r, 0).value) for r in range(1, rows)]
            log.logger.info(f"所用到的visit_id为{visit_record_values_list}")
            # print("first_row_list:", first_row_list)
            var_index = first_row_list.index("落库变量")
            # print("洛库变量:", var_index)
            # 落库变量对应的值
            second_row_list = [sheet.cell(1, c).value for c in range(1, colums)]
            # print("second_row_list:", second_row_list)
            var_table_field_value = second_row_list[var_index]
            log.logger.info(f"落库变量为{var_table_field_value}")
            # 人工判断
            people_index = first_row_list.index("期望值")
            # log.logger.info(f"期望值列号：{people_index}")
            # 获取表名和字段名
            table_field_list = first_row_list[:var_index]
            table_name_list = [".".join(table_name.split(".")[:-1]) for table_name in table_field_list]
            field_name_list = [table_name.split(".")[-1] for table_name in table_field_list]
            # 判断表名和字段名
            set_table_name_list = list(set(table_name_list))
            try:
                all_info_list = self.table_field_value(var_index, sheet_name, table_name_list, field_name_list)
                if len(set_table_name_list) == 1:
                    file = table_name_list[0] + '.csv'
                    # print("file:", file)
                    shutil.copyfile(src_path + "/" + file, dst_path + "/" + file)
                    # 一个sheet 一个表
                    self.same_table_insert(table_name_list, field_name_list, all_info_list, visit_record_values_list,
                                           patient_id, sheet_name)
                    # 如果当前表是正是visit_record表，则不做visit_record表的增加操作
                    if table_name_list[0] != "visit.visit_record":
                        shutil.copyfile(visit_record_path, dst_path + "/" + visit_record_path)
                        self.add_visit_record(patient_id, visit_record_values_list, visit_record_field)
                    if table_name_list[0] == "visit.visit_record":
                        # 如果是visit_record表则将id 改成visit_id
                        self.modify_visit_record_id("visit.visit_record")
                    # 修改case_id的值
                    # 在case_base表中添加case_id 以及patient_id,visit_id
                    if table_name_list[0] == "cases.case_diagnose":
                        shutil.copyfile(f'{src_path}/cases.case_base.csv', f'{dst_path}/cases.case_base.csv')
                        case_id_list = self.modify_case_id()
                        self.add_case_base_id(patient_id, case_id_list, visit_record_values_list)
                    if table_name_list[0] == "lab.lab_report_result":
                        self.add_lab_report_table(sheet_name)
                elif len(set_table_name_list) == 2:
                    for file in set_table_name_list:
                        file_csv = file + '.csv'
                        shutil.copyfile(src_path + "/" + file_csv, dst_path + "/" + file_csv)
                    # 一个sheet 不同的表
                    self.diff_table_insert(all_info_list, patient_id, visit_record_values_list)
                    if "visit.visit_record" not in set_table_name_list:
                        shutil.copyfile(visit_record_path, dst_path + "/" + visit_record_path)
                        self.add_visit_record(patient_id, visit_record_values_list, visit_record_field)
                    if "visit.visit_record" in set_table_name_list:
                        self.modify_visit_record_id("visit.visit_record")

                    if "cases.case_diagnose" in set_table_name_list:
                        shutil.copyfile(f'{src_path}/cases.case_base.csv', f'{dst_path}/cases.case_base.csv')
                        case_id_list = self.modify_case_id()
                        self.add_case_base_id(patient_id, case_id_list, visit_record_values_list)

                # 导入数据库
                self.insert_db()
                # 当插入数据较多的时候，需要sleep
                time.sleep(1)
                # 调用api 验证
                error_index_dict, error_rate = self.call_api_trigger(var_table_field_value,
                                                                     rows, people_index, sheet, sheet_name)
                for k, v in error_index_dict.items():
                    sheet_summary_dict[sheet_name] = [v, error_rate]
                    self.summary_result_list.append(sheet_summary_dict)
                    log.logger.info(f"sheet为:{sheet_name},出错的行:{v},出错率:{error_rate}")
                # 从数据库将数据删除
                self.delete_table_data(set_table_name_list, patient_id)
                # 将csv 文件移入已处理的文件夹中
                rm_file_list = os.listdir(dst_path)
                log.logger.info(f"移出的文件列表为：{rm_file_list}")
                # 移出
                for rm_file in rm_file_list:
                    os.remove("{}/{}".format(dst_path, rm_file))
                log.logger.info(f"============={sheet_name}结束，下一个落库变量测试开始===============")
            except Exception as e:
                print(str(e))
                log.logger.info(f"============={sheet_name}测试异常退出，下一个落库变量测试开始===============")
                self.delete_table_data(set_table_name_list, patient_id)
        with open(f'./summary_result.json', 'w', encoding='utf-8', newline='') as summary_file:
            json.dump(self.summary_result_list, summary_file)
        log.logger.info("*********************测试结束*********************")

    def add_lab_report_table(self, sheet_name):
        """增加lab_report表"""
        shutil.copyfile(src_path + "/" + "lab.lab_report.csv", dst_path + "/" + "lab.lab_report.csv")
        # 读取visit_record 中有几条数据
        visit_all_value_list = self.excel_one_line_to_list(sheet_name, -1)
        # 将visit_record中的patient_id,visit_id 插入到lab_report表中
        lab_report_k, lab_report_v = self.read_csv('lab.lab_report')
        lab_all_value_list = list()
        lab_all_value_list.append(lab_report_k)
        if patient_id == 1000000:
            report_id = 10
        if patient_id == 2000000:
            report_id = 110
        if patient_id == 3000000:
            report_id = 220
        if patient_id == 4000000:
            report_id = 330
        if patient_id == 5000000:
            report_id = 440
        lab_report_v[1] = patient_id
        for visit_record_id in visit_all_value_list:
            lab_report_v[lab_report_k.index("report_id")] = report_id
            report_id += 1
            lab_report_v[lab_report_k.index("visit_id")] = visit_record_id
            lab_report_v_d = copy.deepcopy(lab_report_v)
            lab_all_value_list.append(lab_report_v_d)
        with open(f'{dst_path}/lab.lab_report.csv', 'w', encoding='utf-8', newline='') as lab_report_file:
            for all_v in lab_all_value_list:
                if any(all_v):
                    ww = csv.writer(lab_report_file, delimiter='\t')
                    ww.writerow(all_v)
        # 修改report_result中的id
        self.modify_report_id_diff("lab.lab_report_result.csv")


    def modify_case_id(self):
        """修改case_id"""
        case_diagnose_all_list = list()
        case_id_list = list()
        with open(f'{dst_path}/cases.case_diagnose.csv', 'r', encoding='utf-8', newline='') as case_diagnose_id:
            c = csv.reader(case_diagnose_id, delimiter='\t')
            rows = [row for row in c if any(row)]
            for row in rows:
                if any(row):
                    if rows.index(row) == 0:
                        case_diagnose_all_list.append(row)
                    else:
                        row[2] = row[0]
                        case_id_list.append(row[2])
                        case_diagnose_all_list.append(row)
            with open("{}/{}".format(dst_path, "cases.case_diagnose.csv"), 'w', encoding='utf-8', newline='') as f:
                for all_v in case_diagnose_all_list:
                    if any(all_v):
                        ww = csv.writer(f, delimiter='\t')
                        ww.writerow(all_v)
        return case_id_list

    def add_case_base_id(self, patient_id, case_id_list, visit_record_values_list):
        """添加case_base中数据的条数"""
        table_name = "cases.case_base"
        case_key_list, case_value_list = self.read_csv(table_name)
        case_all_value_list = list()
        visit_id_list = list(set(visit_record_values_list))
        for visit_id in visit_id_list:
            index_case = visit_id_list.index(visit_id)
            case_value_list[0] = case_id_list[index_case]
            case_value_list[1] = patient_id
            case_value_list[2] = visit_id
            case_value_list_d = copy.deepcopy(case_value_list)
            case_all_value_list.append(case_value_list_d)
        with open("{}/{}".format(dst_path, "cases.case_base.csv"), 'a+', encoding='utf-8', newline='') as case_csv_file:
            for all_v in case_all_value_list:
                if any(all_v):
                    w = csv.writer(case_csv_file, delimiter='\t')
                    w.writerow(all_v)
        with open("{}/{}".format(dst_path, "cases.case_base.csv"), 'r', encoding='utf-8', newline='') as case_file:
            r = csv.reader(case_file, delimiter='\t')
            rows = [row for row in r if any(row)]
            del rows[1]
            with open("{}/{}".format(dst_path, "cases.case_base.csv"), 'w', encoding='utf-8', newline='') as f:
                for all_v in rows:
                    if any(all_v):
                        ww = csv.writer(f, delimiter='\t')
                        ww.writerow(all_v)

    def call_api_trigger(self, var_table_field_value, rows, people_index, sheet, sheet_name):
        """调用api获取trigger_id"""
        # 获取patinet_id, visit_id, inpat_id
        file_list = os.listdir(dst_path)
        log.logger.info(f"文件列表是:{file_list}")
        error_index_dict = dict()
        visit_id_list = [str(int(sheet.cell(c, 0).value)) for c in range(1, rows)]
        print("excel中的visit_id_list:", visit_id_list)
        # 获取trigger_id_list
        trigger_id_list = list()
        for visit_id in visit_id_list:
            url = "http://172.16.127.101:37125/api/Trigger"
            data_dict = [{"patientId": patient_id, "visitId": visit_id}]
            if tag:
                data_dict = [{"patientId": patient_id, "visitId": visit_id, "tag": tag}]
                # print("data_dict:",data_dict)
            data = json.dumps(data_dict)
            log.logger.info(f"POST请求参数：{data}")
            header = {"Content-Type": "application/json-patch+json"}
            response = requests.post(url, data=data, headers=header)
            content = response.text
            # print(content)
            content_dict = json.loads(content)
            trigger_id = eval(content_dict.get("data"))[0]
            log.logger.info(f"trigger_id:{trigger_id}")
            trigger_id_list.append(trigger_id)
        # 根据trigger_id_list 去获取落库变量对应的值
        error_index_list, error_rate = self.verify_hope_to_db_value(trigger_id_list, var_table_field_value,
                                                                    rows, people_index, sheet)
        error_index_dict[sheet_name] = error_index_list
        return error_index_dict, error_rate


    def verify_hope_to_db_value(self, trigger_id_list, var_table_field_value, rows, people_index, sheet):
        """根据trigger_id_list 验证true和false"""
        var_db_value_list = list()
        for trigger_id in trigger_id_list:
            # 每个trigger_id 对应的json中的key 对应的value
            var_db_value = self.get_var_db(var_table_field_value, trigger_id)
            var_db_value_list.append(var_db_value)
        log.logger.info(f"数据库中落库变量对应的列表是:{var_db_value_list}")
        # excel中每一行的中的人工判断的值
        people_value_list = list()
        for v in range(1, rows):
            people_value_list.append(sheet.cell(v, people_index + 1).value)
        log.logger.info(f"excel中的落库变量依次是:{people_value_list}")
        correct_count = 0
        error_count = 0
        error_rate = '100%'
        error_index_list = list()
        new_excel_list = []
        try:
            if len(var_db_value_list) == len(people_value_list):
                for i in range(len(var_db_value_list)):
                    # 转换格式
                    people_value = self.convert_type(people_value_list[i])
                    new_excel_list.append(people_value)
                    if var_db_value_list[i] == people_value:
                        correct_count += 1
                        error_rate = '0%'
                    else:
                        error_count += 1
                        error_index_list.append(i + 1)
            else:
                # 如果取数个数不一致,直接全部输出100%错误
                error_index_list = []
                error_rate = '100%'

        except Exception as e:
            print(str(e))
            error_index_list = []
            error_rate = '100%'
        if error_count != 0:
            error_rate = str(((error_count / len(people_value_list)) * 100)) + "%"
        log.logger.info("正确的个数为:%s, 错误的个数为:%s" % (correct_count, error_count))
        log.logger.info("转换格式过的excel中的期望值为:%s" % new_excel_list)
        return error_index_list, error_rate

    def convert_type(self, people_value):
        """格式转换"""
        p_value = people_value
        if isinstance(people_value, float):
            # 浮点类型转换
            if str(people_value).split('.')[-1] == '0':
                p_value = str(int(people_value))
            else:
                p_value = str(people_value)
        elif people_value in ["TRUE", True, "true", "True", 1]:
            p_value = 'true'
        elif people_value in ["FALSE", False, "false", "False", 0]:
            p_value = 'false'
        return p_value

    def get_var_db(self, var_table_field_value, trigger_id):
        """根据trigger_id取数据库中的落库变量的值"""
        var_list = var_table_field_value.split('.')
        # 表名
        var_table_name = '.'.join(var_list[:-2])
        # 字段名
        var_field_name = var_list[-2]
        # key 名
        var_key_name = var_list[-1]
        conn = psycopg2.connect(database="", user="sixixi", password="", host="",
                                port=5432)
        select_sql = """select {} from {} where trigger_id in ('{}');""".format(var_field_name, var_table_name,
                                                                                trigger_id)
        cursor = conn.cursor()
        time.sleep(1.5)
        # print("select_sql:", select_sql)
        cursor.execute(select_sql)
        time.sleep(1.5)
        var_key_value_list = cursor.fetchall()
        # print(var_key_value_list)
        # 获取数据
        var_db_value = ''
        for var_value_tuple in var_key_value_list:
            var_value_dict = var_value_tuple[0]
            if var_key_name in var_value_dict.keys():
                var_db_value = var_value_dict.get(var_key_name)
                log.logger.info(f"数据库中的落库变量查询到的值为:{var_db_value}")
        # 关闭游标
        cursor.close()
        # 关闭数据库连接
        conn.close()
        return var_db_value

    def same_table_insert(self, table_name_list, field_name_list, all_info_list,
                          visit_record_values_list, patient_id, sheet_name):
        """相同表插入数据"""
        # 读取base中的字段 和 值
        base_key_list, base_value_list = self.read_csv(table_name_list[0])
        all_value_list = list()
        for field_name in field_name_list:
            field_name_index = field_name_list.index(field_name)
            visit_id_index = ""
            if field_name_index == 0:
                v_list = all_info_list[field_name_index].get("value")
                key_index = base_key_list.index(field_name)
                visit_id_index = self.manage_patient_id_etc(base_key_list, base_value_list, patient_id)
                for k, v in enumerate(v_list):
                    base_value_list[key_index] = v
                    if isinstance(visit_id_index, int):
                        base_value_list[visit_id_index] = visit_record_values_list[k]
                    base_value_list_d = copy.deepcopy(base_value_list)
                    all_value_list.append(base_value_list_d)
            else:
                self.manage_csv(all_info_list, all_value_list, field_name_index, base_key_list, field_name,
                                visit_id_index,
                                visit_record_values_list)
        # 将all_value_list 写入csv文件中
        # print("all_value_list:", all_value_list)
        with open("./{}/{}.csv".format(dst_path, table_name_list[0]), 'a+', encoding='utf-8',
                  newline='') as csv_file:
            for all_v in all_value_list:
                if any(all_v):
                    w = csv.writer(csv_file, delimiter='\t')
                    w.writerow(all_v)

        # # 修改主键值　todo:并不知道是否可以去掉主键
        if table_name_list[0] == "visit.inpat_record":
            with open("{}/{}".format(dst_path, "visit.inpat_record.csv"), 'r', encoding='utf-8',
                      newline='') as visit_file:
                r = csv.reader(visit_file, delimiter='\t')
                rows = [row for row in r if any(row)]
                del rows[1]
                with open("{}/{}".format(dst_path, "visit.inpat_record.csv"), 'w', encoding='utf-8', newline='') as f:
                    for all_v in rows:
                        if any(all_v):
                            ww = csv.writer(f, delimiter='\t')
                            ww.writerow(all_v)
        if table_name_list[0] != "visit.inpat_record":
            self.modify_table_id(table_name_list[0] + ".csv")
        return all_value_list

    def diff_table_insert(self, all_info_list, patient_id, visit_record_values_list):
        """不同表分别插入数据"""
        # 将相同表的放入一个字典列表，另一个放入一个字典列表
        base_info = all_info_list[0]
        same_t_dict_list = list()
        diff_t_dict_list = list()
        same_v_list = list()
        diff_v_list = list()
        for info_dict in all_info_list:
            if info_dict.get("table") == base_info.get("table"):
                same_t_dict_list.append(info_dict)
            else:
                diff_t_dict_list.append(info_dict)
        same_t_key_list, same_t_value_list = self.read_csv(same_t_dict_list[0].get("table"))
        for same_dict in same_t_dict_list:
            index_cck_v_d = same_t_key_list.index(same_dict.get("field"))
            same_index = same_t_dict_list.index(same_dict)
            visit_id_index = self.manage_patient_id_etc(same_t_key_list, same_t_value_list, patient_id)
            if same_index == 0:
                for same_k, same_v in enumerate(same_dict.get("value")):
                    same_t_value_list[index_cck_v_d] = same_v
                    if isinstance(visit_id_index, int):
                        same_t_value_list[visit_id_index] = visit_record_values_list[same_k]
                    same_t_value_list_d = copy.deepcopy(same_t_value_list)
                    same_v_list.append(same_t_value_list_d)
            else:
                self.manage_csv(all_info_list, same_v_list, same_index, same_t_key_list, same_dict.get("field"),
                                visit_id_index, visit_record_values_list)
        # print("same_t_dict_list", same_t_dict_list)
        with open("./{}/{}.csv".format(dst_path, base_info.get("table")), 'a+', encoding='utf-8',
                  newline='') as csv_file:
            for all_v in same_v_list:
                if any(all_v):
                    w = csv.writer(csv_file, delimiter='\t')
                    w.writerow(all_v)
        if base_info.get("table") != "visit.inpat_record":
            self.modify_table_id(base_info.get("table") + ".csv")
        if base_info.get("table") == "lab.lab_report_result":
            self.modify_report_id_diff(base_info.get("table") + ".csv")
        # diff 另一个表
        # print("diff_t_dict_list", diff_t_dict_list)
        diff_t_key_list, diff_t_value_list = self.read_csv(diff_t_dict_list[0].get("table"))
        for diff_dict in diff_t_dict_list:
            diff_index_cck_v_d = diff_t_key_list.index(diff_dict.get("field"))
            diff_index = diff_t_dict_list.index(diff_dict)
            diff_visit_id_index = self.manage_patient_id_etc(diff_t_key_list, diff_t_value_list, patient_id)
            if diff_index == 0:
                for diff_k, diff_v in enumerate(diff_dict.get("value")):
                    diff_t_value_list[diff_index_cck_v_d] = diff_v
                    if isinstance(diff_visit_id_index, int):
                        diff_t_value_list[diff_visit_id_index] = visit_record_values_list[diff_k]
                    diff_t_value_list_d = copy.deepcopy(diff_t_value_list)
                    diff_v_list.append(diff_t_value_list_d)
            else:
                self.manage_csv(diff_t_dict_list, diff_v_list, diff_index, diff_t_key_list, diff_dict.get("field"),
                           diff_visit_id_index, visit_record_values_list)

        with open("./{}/{}.csv".format(dst_path, diff_t_dict_list[0].get("table")), 'a+', encoding='utf-8',
                  newline='') as csv_file:
            for all_v in diff_v_list:
                if any(all_v):
                    w = csv.writer(csv_file, delimiter='\t')
                    w.writerow(all_v)
        # 修改主键值　todo:并不知道哪个是主键
        if diff_t_dict_list[0].get("table") != "visit.inpat_record":
            self.modify_table_id(diff_t_dict_list[0].get("table") + ".csv")
        if diff_t_dict_list[0].get("table") == "lab.lab_report_result":
            self.modify_report_id_diff(diff_t_dict_list[0].get("table") + ".csv")

    def modify_report_id_diff(self, file):
        """包含report_result, report表"""
        path = "{}/{}".format(dst_path, file)
        with open(path, 'r', encoding='utf-8', newline='') as f:
            r = csv.reader(f, delimiter='\t')
            rows = [row for row in r if any(row)]
            if patient_id == 1000000:
                i = 10
            if patient_id == 2000000:
                i = 110
            if patient_id == 3000000:
                i = 220
            if patient_id == 4000000:
                i = 330
            if patient_id == 5000000:
                i = 440
            for index, data_li in enumerate(rows):
                if index == 0:
                    continue
                rows[index][1] = i
                i += 1
            with open(path, 'w', encoding='utf-8', newline='') as w:
                for all_v in rows:
                    if any(all_v):
                        ww = csv.writer(w, delimiter='\t')
                        ww.writerow(all_v)

    def modify_visit_record_id(self, table_name):
        """修改visit_reocrd 中id 为visit_id"""
        visit_all_value_list = list()
        with open("{}/{}".format(dst_path, "visit.visit_record.csv"), 'r', encoding='utf-8',
                  newline='') as visit_csv_file:
            r = csv.reader(visit_csv_file, delimiter='\t')
            rows = [row for row in r if any(row)]
            for row in rows:
                if any(row):
                    if rows.index(row) == 0:
                        visit_all_value_list.append(row)
                    else:
                        row[0] = row[5]
                        visit_all_value_list.append(row)
            with open("{}/{}".format(dst_path, "visit.visit_record.csv"), 'w', encoding='utf-8', newline='') as f:
                for all_v in visit_all_value_list:
                    if any(all_v):
                        ww = csv.writer(f, delimiter='\t')
                        ww.writerow(all_v)
        # print(visit_all_value_list)

    def manage_patient_id_etc(self, key_list, base_value_list, patient_id):
        """处理patient_id,visit_id 和inpat_id"""
        visit_id_index = ""
        if "patient_id" in key_list:
            patient_id_index = key_list.index("patient_id")
            base_value_list[patient_id_index] = patient_id
        if "visit_id" in key_list:
            visit_id_index = key_list.index("visit_id")
        elif "inpat_id" in key_list:
            visit_id_index = key_list.index("inpat_id")
            # print("inpat_id_index:", visit_id_index)
        return visit_id_index

    def manage_csv(self, all_info_list, all_value_list, field_name_index, base_key_list, field_name, visit_id_index,
                   visit_record_values_list):
        """处理下标不为0时的所有值的列表"""
        for values in all_value_list:
            key_index = base_key_list.index(field_name)
            v_list = all_info_list[field_name_index].get("value")
            values_index = all_value_list.index(values)
            values[key_index] = v_list[values_index]
            if isinstance(visit_id_index, int):
                values[visit_id_index] = visit_record_values_list[values_index]

    def table_field_value(self, var_index, sheet_name, table_name_list, field_name_list):
        """获取表名,字段名对应的值"""
        all_info_list = list()
        for num in range(0, var_index):
            info_dict = dict()
            info_dict["table"] = table_name_list[num]
            info_dict["field"] = field_name_list[num]
            info_dict["value"] = self.excel_one_line_to_list(sheet_name, num)
            all_info_list.append(info_dict)
        return all_info_list

    def excel_one_line_to_list(self, name, ncolumn):
        # 读取项目名称列,不要列名
        result = []
        df = pd.read_excel("./data.xlsx", sheet_name=name, usecols=[ncolumn + 1], name=None)
        df_li = df.values.tolist()
        for s_li in df_li:
            s_li_data = s_li[0]
            if isinstance(s_li[0], float):
                # 浮点类型转换
                if str(s_li[0]).split('.')[-1] == '0':
                    s_li_data = int(s_li[0])
                else:
                    s_li_data = s_li[0]
            result.append(s_li_data)
        return result

    def read_csv(self, t_name):
        """读取csv获取第一行字段名和第二行base值"""
        with open("{}/{}.csv".format(dst_path, t_name), 'r', encoding='utf-8', newline='') as csvfile:
            r = csv.reader(csvfile, delimiter='\t')
            rows = [row for row in r if any(row)]
            cc_k_d = copy.deepcopy(rows[0])
            cc_v_d = copy.deepcopy(rows[1])
        return cc_k_d, cc_v_d

    def insert_db(self):
        """将csv数据导入到数据库中"""
        file_list = os.listdir(dst_path)
        conn = psycopg2.connect(database="", user="sixixi", password="", host="",
                                port=5432)
        cursor = conn.cursor()
        # print(file_list)
        try:
            for file in file_list:
                file_path = "{}/{}".format(dst_path, file)
                with open(file_path, 'r', encoding='utf-8', newline='') as f:
                    # print(f)
                    table = '.'.join(file.split(".")[:-1])
                    insert_sql = """COPY %s FROM STDIN WITH (FORMAT CSV,HEADER true, NULL 'nan', DELIMITER '\t')""" % table
                    cursor.copy_expert(insert_sql, f)
                    conn.commit()
        except Exception as e:
            print(str(e))
            conn.rollback()
        # 关闭游标
        cursor.close()
        # 关闭数据库连接
        conn.close()

    def delete_table_data(self, set_table_name_list, patient_id):
        """将一个sheet导入的数据删除"""
        # 删除导入数据的表
        log.logger.info("正在删除，请稍后...")
        # 删除visit_record 表
        conn = psycopg2.connect(database="", user="", password="", host="",
                                port=5432)
        cursor = conn.cursor()
        if "lab.lab_report_result" in set_table_name_list:
            report_id_list = list()
            with open("{}/{}.csv".format(dst_path, "lab.lab_report_result"), 'r', encoding='utf-8', newline='') as csvfile:
                r = csv.reader(csvfile, delimiter='\t')
                rows = [row for row in r if any(row)]
                for index, data_li in enumerate(rows):
                    if index == 0:
                        continue
                    report_id_list.append(data_li[1])
            real_report_list = set(report_id_list)
            log.logger.info("要删除的report_id列表为: %s" % report_id_list)
            for report_id in real_report_list:
                try:
                    delete_sql_result = "delete from {} where report_id in ('{}');".format("lab.lab_report_result", report_id)
                    delete_sql_report = "delete from {} where report_id in ('{}');".format("lab.lab_report", report_id)
                    cursor.execute(delete_sql_result)
                    time.sleep(1.5)
                    conn.commit()
                    time.sleep(1)
                    cursor.execute(delete_sql_report)
                    time.sleep(1.5)
                    conn.commit()
                    time.sleep(1)
                except Exception as e:
                    print("删除报错为：", str(e))
                    conn.rollback()
                    time.sleep(1)
            set_table_name_list.remove("lab.lab_report_result")
            if "lab.lab_report" in set_table_name_list:
                set_table_name_list.remove("lab.lab_report")
        if "cases.case_diagnose" in set_table_name_list:
            case_id_list = list()
            with open("{}/{}.csv".format(dst_path, "cases.case_base"), 'r', encoding='utf-8', newline='') as csvfile:
                r = csv.reader(csvfile, delimiter='\t')
                rows = [row for row in r if any(row)]
                for index, data_li in enumerate(rows):
                    if index == 0:
                        continue
                    case_id_list.append(data_li[0])
            log.logger.info("要删除的case_id列表为: %s" % case_id_list)
            for case_id in case_id_list:
                try:
                    delete_sql = "delete from {} where case_id in ('{}');".format("cases.case_diagnose", case_id)
                    cursor.execute(delete_sql)
                    time.sleep(1.5)
                    conn.commit()
                    time.sleep(1)
                    delete_sql_case = "delete from {} where case_id in ('{}');".format("cases.case_base", case_id)
                    cursor.execute(delete_sql_case)
                    time.sleep(1.5)
                    conn.commit()
                    time.sleep(1)
                except Exception as e:
                    print("删除报错为：", str(e))
                    conn.rollback()
                    time.sleep(1)
            set_table_name_list.remove("cases.case_diagnose")

        for table_name in set_table_name_list:
            try:
                delete_sql = "delete from {} where patient_id in ('{}');".format(table_name, patient_id)
                cursor.execute(delete_sql)
                time.sleep(1.5)
                conn.commit()
                time.sleep(1)
            except Exception as e:
                print("删除报错为：", str(e))
                conn.rollback()
                time.sleep(1)
        try:
            delete_sql = "delete from {} where patient_id in ('{}');".format('visit.visit_record', patient_id)
            cursor.execute(delete_sql)
            time.sleep(1.5)
            conn.commit()
            time.sleep(1)
        except Exception as e:
            print("删除报错为：", str(e))
            conn.rollback()
            time.sleep(1)
        cursor.close()
        conn.close()

    def modify_table_id(self, file):
        """根据主键修改成自增"""
        path = "{}/{}".format(dst_path, file)
        with open(path, 'r', encoding='utf-8', newline='') as f:
            r = csv.reader(f, delimiter='\t')
            rows = [row for row in r if any(row)]
            del rows[1]
            if patient_id == 1000000:
                i = 10
            if patient_id == 2000000:
                i = 110
            if patient_id == 3000000:
                i = 220
            if patient_id == 4000000:
                i = 330
            if patient_id == 5000000:
                i = 440
            for index, data_li in enumerate(rows):
                if index == 0:
                    continue
                rows[index][0] = i
                i += 1
            with open(path, 'w', encoding='utf-8', newline='') as w:
                for all_v in rows:
                    if any(all_v):
                        ww = csv.writer(w, delimiter='\t')
                        ww.writerow(all_v)

    def add_visit_record(self, patient_id, visit_record_values_list, var_table_field_value):
        """在增加其他表的时候,visit_record表同步增加"""
        table_name = ".".join(var_table_field_value.split(".")[:-1])
        visit_key_list, visit_value_list = self.read_csv(table_name)
        visit_all_value_list = list()
        visit_record_id_list = list(set(visit_record_values_list))
        for visit_id in visit_record_id_list:
            visit_value_list[0] = visit_id
            visit_value_list[1] = patient_id
            visit_value_list[5] = visit_id
            visit_value_list_d = copy.deepcopy(visit_value_list)
            visit_all_value_list.append(visit_value_list_d)
        with open("{}/{}".format(dst_path, "visit.visit_record.csv"), 'a+', encoding='utf-8',
                  newline='') as visit_csv_file:
            for all_v in visit_all_value_list:
                if any(all_v):
                    w = csv.writer(visit_csv_file, delimiter='\t')
                    w.writerow(all_v)
        with open("{}/{}".format(dst_path, "visit.visit_record.csv"), 'r', encoding='utf-8', newline='') as visit_file:
            r = csv.reader(visit_file, delimiter='\t')
            rows = [row for row in r if any(row)]
            del rows[1]
            with open("{}/{}".format(dst_path, "visit.visit_record.csv"), 'w', encoding='utf-8', newline='') as f:
                for all_v in rows:
                    if any(all_v):
                        ww = csv.writer(f, delimiter='\t')
                        ww.writerow(all_v)


class AutoGenerateBaseCsv(object):
    """自动更新base_insert_data csv"""
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
        conn = psycopg2.connect(database="", user="sixixi", password="", host="",
                                port=5432)
        if 'cases.case_diagnose' in self.excel_table_list:
            if 'cases.case_base' not in self.excel_table_list:
                self.excel_table_list.append('cases.case_base')
        if 'lab.lab_report_result' in self.excel_table_list:
            if 'lab.lab_report' not in self.excel_table_list:
                self.excel_table_list.append('lab.lab_report')
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
                if db_value != '':
                    if not db_value:
                        db_value = 'nan'
                if isinstance(db_value, list):
                    db_value = []
                if isinstance(db_value, dict):
                    db_value = {}
                if isinstance(db_value, str):
                    if db_value == '' or db_value == ' ':
                        db_value = '1'
                    else:
                        db_value = db_value.strip()
                final_db_value_list.append(db_value)
            db_key_value_list.append(final_db_value_list)
            self.db_excel_data_dict[table] = db_key_value_list
        # 关闭游标
        cursor.close()
        # 关闭数据库连接
        conn.close()

    def save_to_csv(self):
        """将查询到的最新base数据写入csv"""
        if not os.path.exists(src_path):
            os.mkdir(src_path)
        print(self.db_excel_data_dict)
        for k, v in self.db_excel_data_dict.items():
            with open(f'{src_path}/{k}.csv', 'w', encoding='utf-8', newline='') as excel_db_file:
                for all_v in v:
                    if any(all_v):
                        ww = csv.writer(excel_db_file, delimiter='\t')
                        ww.writerow(all_v)
            if k == "visit.visit_record":
                with open(f'./{k}.csv', 'w', encoding='utf-8', newline='') as visit_record_csv:
                    for all_v in v:
                        if any(all_v):
                            ww = csv.writer(visit_record_csv, delimiter='\t')
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


def clear_log():
    """每次测试前清空上一次的log内容"""
    with open('./summary.log', 'r+', encoding='utf-8', newline='') as f:
        f.seek(0)
        f.truncate()


if __name__ == '__main__':
    clear_log()
    log = Logger('summary.log', level='info')
    src_path = './base_insert_data'
    dst_path = './new_insert_data'
    visit_record_path = 'visit.visit_record.csv'
    new_visit_path = './new_visit_record'
    command_csv = input("请输入是否要执行更新表的base_csv文件：(y/n)\n")
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
    continue_test = input("请输入是否继续dsl测试:(y/n)\n")
    continue_test = continue_test.lower()
    if continue_test == 'y' or continue_test == 'yes':
        log.logger.info("*******************测试开始*******************")
        patient_id = int(input("请输入你被分配到的patient_id：\n"))
        tag = input("请输入此次的task的tag,如果不输入，默认全部：")
        auto_dsl = AutoDSL()
        auto_dsl.get_csv_content(patient_id)
    else:
        log.logger.info("*******************测试未执行 结束*******************")
