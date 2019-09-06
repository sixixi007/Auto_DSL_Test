# DSL_TEST_auto

## 脚本功能说明

### auto test for DSL

insert test case to postgresql

use test case's patient_id and visit_id for trigger request

compare the patient_var_record_落库变量 's value with excel's value

summary the error rate and error row and column

### auto update base_insert_data folder 

不再需要手动从数据库复制数据，实现自动更新base 数据

注意：需要在执行测试的时候，检查每个csv是否将光标移动到下一行，有时候是没有移动到下一行的
然后再按照脚本指示进行下一步操作

### log 说明

log 会将info级别的信息都存到summary.log 文件中，每次在进行下一次测试前，会将上一次的测试log 清空,
避免误导


## Test Case 格式说明

1. 如果是测试空值情况, 请直接将excel 置空,不要写null

2. 第一行的第一列是visit_visie_record_visit_id , 建议写:1000001, 1000002 依次递加

3. 落库变量 和 期望值 见 模板excel

4. 相同的visit_id 也就是相同的test case

5. 附上模板: ./template_excel/DSL_Template.xlsx


## Base Test Case 说明

### 自动更新base_insert_csv 

1. 以制表符\t 作为分隔符,避免逗号引起的冲突

2. 期望值的true, false 和NULL 就按照true, false, NULL 的方式(true, false 小写)

### excel 中的注意事项

3. excel 整体格式不用可以设置, 常规最好, 时间以文本形式显示, 期望值以文本形式显示，其他不做要求，已在代码中做转换

4. **需要注意空白行的问题，有些原来有数据，被删除后的空白行，会被解读为空字符串，尽量使用删除行，而不只是删除数据**

5. 空格键代表的是空白值, 不是空值, 可以在test case 中使用

6. 时间格式转换成文本，=TEXT(value, format)

7. 时间格式也可以通过```'2018-09-02 09:30``来实现





