# 本地运行指南（不需要 AWS / Snowflake 账号）

原版项目要求开通 Amazon S3 和 Snowflake 两个云账号，并且只能用 Docker 起 Airflow。
这份指南记录的是**在本机把它真跑起来**的版本：数据湖用本地目录、数仓用 DuckDB，
PySpark 仍然是真实的 Spark，代码结构、星型模型、ETL 分层都保持原样。

---

## 1. 一条命令跑完全流程

```bat
run.bat
```

等价于（在项目根目录执行）：

```bat
set PYTHONPATH=%CD%
.venv\Scripts\python.exe main.py
```

只跑某一段：

```bat
.venv\Scripts\python.exe main.py etl        :: 抽取 + 转换 + 入湖 + 入仓
.venv\Scripts\python.exe main.py analysis   :: 只重跑分析和出图
```

一次完整运行约 15 秒（含 Spark 启停）。

---

## 2. 已经装好的环境

| 组件 | 位置 / 版本 | 说明 |
|---|---|---|
| Python 虚拟环境 | `.venv`（Python 3.13.14） | 所有依赖装在这里，不污染系统 |
| JDK | `.jdk/jdk-17.0.20.1+1`（Temurin 17） | 便携版，Spark 专用；系统里那个 JRE 1.8 是坏的，没动它 |
| pyspark | 4.2.0 | 本机 Python 3.13 需要 Spark 4.x（Python 3.11/3.12 可用 3.5.4） |
| pandas / duckdb / matplotlib | 2.3.3 / 1.5.6 / 内置 | |
| 依赖清单 | `requirements-local.txt` | 重新装环境用这个 |

重装环境：

```bat
python -m venv .venv
.venv\Scripts\python -m pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements-local.txt
```

> JDK 不在 PATH 里也没关系：`scripts/spark_utils.py` 会自动到 `.jdk/` 下找工作目录并注入
> `JAVA_HOME`，所以换机器只要把 `.jdk` 一起带上。

---

## 3. 产物在哪

```
lake/                                   ← 数据湖（替代 S3 桶）
├── data/statsfinal.csv                 原始区
├── processed_data/output_<时间戳>.csv  加工区（去重靠内容 MD5）
└── .metadata/*.json                    模拟 S3 对象元数据（hash、行数、版本）

warehouse/retail_sales.duckdb           ← 数据仓库（替代 Snowflake）
   ├─ date_dim        (date_key, year, month, day)
   ├─ product_dim     (product_key, product_name)
   ├─ sales_fact      (date_key, product_key, total_units_sold, total_revenue)
   └─ processed_files (filename, processed_at)   ← 幂等控制

visualizations/total_sales_by_product.png
visualizations/monthly_sales_trend.png
logs/*.log                              每个环节一个日志文件
data/raw, data/processed                下载/落地的中间副本
```

直接查数仓：

```bat
.venv\Scripts\python -c "import duckdb;c=duckdb.connect('warehouse/retail_sales.duckdb',read_only=True);print(c.execute('select * from sales_fact limit 5').fetchall())"
```

---

## 4. 这次实际跑出来的结果

- 原始 CSV：**4,600** 行（2010-06-13 → 2023-02-03，日粒度）
- 清洗后：**4,574** 天 × 4 个产品 = **18,296** 条事实行
- 全期营收 **¥236,131,892**，销量 **48,121,022** 件，日均营收约 **¥51,625**

| 产品 | 销量（件） | 营收（元） | 件均价 |
|---|---:|---:|---:|
| P3 | 14,379,603 | 77,937,448 | 5.42 |
| P2 | 9,741,271 | 61,759,658 | 6.34 |
| P1 | 18,860,169 | 59,786,736 | 3.17 |
| P4 | 5,139,979 | 36,648,050 | 7.13 |

结论：**P1 卖得最多但单价最低**，营收只排第三；**P3 靠 5.42 的均价拿下营收第一**；
P4 单价最高（7.13）但走量最少。

---

## 5. 修掉的原始 Bug

顺手修了 7 个会让流程跑不起来的问题（都是原仓库真实存在的）：

1. **列名不匹配（致命）**：`transform_data` 输出小写 `date/product_id/total_units_sold`，
   而 `load_to_s3.clean_and_validate_data` 却在找 `Date/Total_Units_Sold` → 必崩。
   现在两边统一成 `date, year, month, day, product_key, total_units_sold, total_revenue`。
2. **去重命中后崩溃**：`load_to_s3` 内容重复时返回 `None`，`etl_task` 把 `None` 传给
   Snowflake 装载函数 → 崩。现在回退到"最近一个加工文件"，保持幂等。
3. **日志目录不存在**：`log_config` 用 `RotatingFileHandler('logs/xxx.log')`，首次运行
   `logs/` 不存在直接抛错。现在自动 `makedirs`，并避免重复挂 handler。
4. **Dockerfile 架构写死**：下载的是 `openjdk-22_linux-aarch64`，在 x86_64 / Apple 以外的
   机器上 `java -version` 直接失败。改成 apt 装 `openjdk-17-jre-headless`。
5. **Airflow 2.9 弃用 API**：`DummyOperator`、`dag=DAG(...)` 传参、`schedule_interval`
   都改成 `EmptyOperator` + `with DAG(...)` + `schedule`。
6. **源数据脏**：26 行日期是 `31-9-2010`、`31-11-2010` 这类不存在的日期（每年各一条，
   2010–2022 共 26 行）；另外闰年 2 月的 28/29 日整段缺失。前者用 `try_to_date` 解析后
   记警告丢弃（占 0.57%），后者保留但趋势图里会剔除首尾不完整月份，避免图末尾假暴跌。
7. **占位符硬编码**：`config.py` 里直接写 `your-aws-access-key`。现在全部走 `.env`
   （`.env.example` 模板），密钥不进版本库。

## 6. 架构上的改动

为了让同一套代码能跑在"云上"和"本地"，把两个外部依赖抽成了适配层：

| 新增文件 | 作用 |
|---|---|
| `scripts/storage.py` | 数据湖抽象。`LocalLake`（目录 + side-car 元数据）/ `S3Lake`（boto3），接口一致：download / put_bytes / list_objects / latest |
| `scripts/warehouse.py` | 数仓抽象。`LocalWarehouse`（DuckDB）/ `SnowflakeWarehouse`（connector），统一 execute / query / register_frame |
| `scripts/spark_utils.py` | Spark 会话工厂 + JDK 自动定位 |

`scripts/config.py` 里的 `USE_LOCAL`（默认 `true`）是唯一开关，其余业务代码不关心底层是云还是本地。

---

## 7. 想切回真实的 AWS S3 + Snowflake

```bat
copy .env.example .env
```

编辑 `.env`：

```ini
USE_LOCAL=false
AWS_S3_BUCKET_NAME=你的桶名
AWS_ACCESS_KEY=...
AWS_SECRET_KEY=...
AWS_REGION=us-east-1
SNOWFLAKE_ACCOUNT=xxxxxx.region
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
```

然后 `run.bat`。注意两点：

- S3 桶需要预先存在（`data/statsfinal.csv` 会自动补传，`processed_data/` 会自动建）；
- Snowflake 的 `COPY INTO` 走的是外部 S3 路径，桶和账号需要在同一区域可访问。

### Airflow 调度（可选）

`dags/retail_data_pipeline_dag.py` 已经修好，但 Airflow 在 Windows 上必须跑在容器里，
本机目前**没有装 Docker Desktop**（只有 docker CLI，daemon 没起）。要用的话：

1. 装 Docker Desktop 并启动；
2. `docker compose build` → `docker compose up -d`；
3. 打开 http://localhost:8080（airflow / airflow），启用 `retail_data_pipeline`。

注意容器内的路径映射和本机 `.venv`、`.jdk` 无关：镜像里是独立 Python 环境 + apt 装的 JDK 17。
