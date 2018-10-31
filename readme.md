# etlconfig

*Configured ETL with Spark.*

Define your simple Data loads with JSON. Spark process the data based on configurations:

- input files
- a SQL command
- a destination command


## Demo

### Copy files to your tmp directory 

- these files are to support the demo

```bash
cp ./tests/resources/*.csv ./tests/resources/config_example.json /tmp/
```

### Run the ./demo.py python file

`python demo.py`

Doing this will load the following configuration file from disk:

```json
{
    "py/object": "etlconfig.etl_config.EtlConfig",
    "_destination": "/tmp/output.csv",
    "_source_tables": [
        {
            "py/object": "etlconfig.etl_config.TableObject",
            "_alias_name": "census",
            "_path_to_file": "/tmp/2010_Census_Populations_by_Zip_Code.csv"
        },
        {
            "py/object": "etlconfig.etl_config.TableObject",
            "_alias_name": "more_data",
            "_path_to_file": "/tmp/more_data.csv"
        },
        {
            "py/object": "etlconfig.etl_config.TableObject",
            "_alias_name": "special_zip",
            "_path_to_file": "/tmp/selected_zip_code.csv"
        }
    ],
    "_sql": "select m.more_data,c.`Total Households` from census c inner join more_data m on m.zip = c.zip inner join special_zip s on s.zip = c.zip"
}
```

### terminal results

- The terminal will then show the results of loading 3 files and running the SQL

```bash
$ python demo.py

2018-10-30 21:20:26 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
+---------+----------------+
|more_data|Total Households|
+---------+----------------+
|        a|           10727|
|        b|           18646|
|        c|           19892|
+---------+----------------+
```

## set up

- etlconfig must be added to PYTHONPATH
- load python package dependencies in requirements.txt

## Data Sources

- https://catalog.data.gov/dataset?res_format=CSV

## To Do 

- better example CSV files
- saving the result back to disk
- sourcing file systems from cloud data stores
- support reading file formats besides csv