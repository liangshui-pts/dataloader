# MySQLLoader Usage

```bash
# 查看帮助
python3 loader.py --help

# 查看参数信息
python3 loader.py load --help

#上传数据
python3 loader.py load --source [sourceType] --filepath [data file] --uri [database connect uri in python style]  --database [database name]

## 例如：
python3 loader.py load --source mysql --filepath creditcard.csv --database_uri mysql+pymysql://root:123456@localhost:3306 --database test

```
