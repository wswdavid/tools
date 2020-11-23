# COS 工具脚本

## 安装

使用pip安装： `pip install -r requirements.txt`

## 配置

配置在本地的 `config.ini` 文件中，需要`secret_id` 、`secret_key`、`bucket`、`region`等信息用于把日志下载到本地。

需要提前确认使用的`secret id`的账号有存储桶的`GET Bucket`和`GET Object`的访问权限。

## cos_log_analyse.py 检索存储桶的logging日志

参数配置说明如下：

| **选项**         | **说明**                                                                              |
| ---------------- | ------------------------------------------------------------------------------------- |
|-prefix|存储桶日志所在的日志前缀 -> cos-access-log/2020/11/12|
|-op| 模糊匹配分析单条 URL 的操作记录，检索结果生成一个csv文件: `MMdd-HHmmss.csv`|
|-e|当输入参数为op时，模糊匹配COS中的事件类型|
|-u|精准匹配分析单条 URL 的出入流量、请求次数|
|-csv|将当前读取到的log日志保存为一个csv格式的文件|
|-f|从本地读取已保存的csv文件，使用该参数时不需要输入密钥信息|

配置好密钥等信息后，在终端下执行:

```
python3 cos_log_analyse.py -prefix cos-access-log/2020/11/11 -op access

python3 cos_log_analyse.py -prefix cos-access-log/2020/11/11 -u access
```
