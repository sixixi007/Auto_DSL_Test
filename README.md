# DSL_Test_Software_Install

## 说明

为了考虑大多数测试人员的操作系统为windows 环境，此软件包是基于windosw环境的

此压缩文件是作为不依赖安装环境，进行DSL 测试用例，数据库hdr_medtest 和cdss_medtest ，以及 http://172.16.127.101:37125/swagger/index.html 之间的交互

## 使用指南

1. 替换你想运行的excel test case ， 文件名：```data.xlsx``` ，  路径： ```DSL_Test_Tools\dist\data.xlsx ```（其他文件都是依赖文件，请不要随意更改或者删除）
2. 在cmd 窗口中运行  ```DSL_Test_Tools\dist\auto_new_windows.exe```
3. 文件夹 base_insert_data 中的csv文件是即将导入数据库的表以及字段，以\t 进行分隔 
4. 生成的最终结果保存在summary.log 文件中，txt格式
5. 关于文件的详细说明可以参考 ```DSL_Test_Tools\dist\README.md``` 
7. ```data.xlsx```中的第一个sheet 的sheet名请命名为bugs ，这个不会进入测试环节。



