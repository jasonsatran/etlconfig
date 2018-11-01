# etlconfig

*Configured ETL with Spark.*

Define your simple Data loads with JSON. Spark process the data based on configurations:

- input files
- a SQL command
- a destination command

## set up

- etlconfig must be added to PYTHONPATH
- load python package dependencies in requirements.txt

## Demo

### Copy files to your tmp directory 

- these files are to support the demo

```bash
cp -r ./tests/resources/ /tmp/etlconfig/
```

### Run the ./demo.py python file

`python demo.py`

Doing this will load the following configuration file from disk:

```json
{
    "py/object": "etlconfig.etl_config.EtlConfig",
    "_destination": "/tmp/etlconfig/output/",
    "_source_tables": [
        {
            "py/object": "etlconfig.etl_config.TableObject",
            "_alias_name": "city",
            "_path_to_file": "/tmp/etlconfig/city_table.csv"
        },
        {
            "py/object": "etlconfig.etl_config.TableObject",
            "_alias_name": "state",
            "_path_to_file": "/tmp/etlconfig/state_table.csv"
        }
    ],
    "_sql": "select city.city,city.population,state.census_division from state inner join city on state.abbreviation = city.state order by state.name asc, city.city asc"
}%
```

### terminal results

- The terminal will then show the results of loading 3 files and running the SQL

```bash
2018-11-01 08:44:49 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
+----------+----------+---------------+
|      city|population|census_division|
+----------+----------+---------------+
|   Chicago|   2716000|              3|
|  Fort Lee|     37907|              2|
|    Newark|    285154|              2|
|Huntington|    203276|              2|
| Manhattan|   1665000|              2|
+----------+----------+---------------+
```

### output written to disk

Additionally, the output is saved to disk at the configured location, which  is /tmp/etlconfig/output/

````
$ cat /tmp/etlconfig/output/*.csv
Chicago,2716000,3
Fort Lee,37907,2
Newark,285154,2
Huntington,203276,2
Manhattan,1665000,2
````


## Data Sources

- https://catalog.data.gov/dataset?res_format=CSV
- https://statetable.com/
- google for city populations

## To Do 

- sourcing file systems from cloud data stores
- support reading file formats besides csv
