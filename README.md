# PySpark_cheat_sheet

This PySpark cheat sheet will give overview of Pyspark functions & code samples covers the basics like initializing Spark in Python, loading data, queriing data, filtering data and repartitioning.


```python
from pyspark.sql import SparkSession
```


```python
spark = SparkSession.builder.master("local[0]").appName("SparkExamples.com").getOrCreate()
```


```python
data = [(1,'James',None,'Smith',51,10,'.Net','Newyork', '1991-04-01','M',3000),
  (2,'Michael','Rose',None,45,11,'Ruby','Newyork', '2000-05-19','M',4000),
  (3,'Robert',None,'Williams',32,10,'Scala','California','1978-09-05','M',4000),
  (4,'Maria','Anne','Jones',36,9,'Java','Hyderabad','1967-12-01','F',5000),
  (5,'Jen','Mary','Brown',39,5,'Scala','Nagpur','1980-02-17','F',0),
  (6,'Prabhakar','B','G',33,11,'Python','Pune','1967-12-01','M',5000),
  (7,'Praveen','B','G',21,13,'Java','Hyderabad','1967-12-01','M',6500),
  (8,'Rajesh','B','G',25,2,'Scala','Nellore','1967-12-01','M',5100),
  (9,'Pramodh','B','G',49,9,'Ruby','Pune','1967-12-01','M',5000),
  (10,'Ajay',None,None,50,10,None,'Pune','1967-12-01','M',2500),
  (11,'Bob','D',None,43,14,'R','Hyderabad','1967-12-01','M',2500),
  (12,'Chris','B','Smith',47,12,'JavaScript',None,'1967-12-01','M',2500),
  (13,None,None,None,55,None,None,None,None,None,None),
]

columns = ["SrNo", "firstname","middlename","lastname","age","experiance","skill","city", "dob","gender","salary"]
```


```python
df = spark.createDataFrame(data=data, schema = columns)
```


```python
df.show()
```

    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance|     skill|      city|       dob|gender|salary|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    |   1|    James|      null|   Smith| 51|        10|      .Net|   Newyork|1991-04-01|     M|  3000|
    |   2|  Michael|      Rose|    null| 45|        11|      Ruby|   Newyork|2000-05-19|     M|  4000|
    |   3|   Robert|      null|Williams| 32|        10|     Scala|California|1978-09-05|     M|  4000|
    |   4|    Maria|      Anne|   Jones| 36|         9|      Java| Hyderabad|1967-12-01|     F|  5000|
    |   5|      Jen|      Mary|   Brown| 39|         5|     Scala|    Nagpur|1980-02-17|     F|     0|
    |   6|Prabhakar|         B|       G| 33|        11|    Python|      Pune|1967-12-01|     M|  5000|
    |   7|  Praveen|         B|       G| 21|        13|      Java| Hyderabad|1967-12-01|     M|  6500|
    |   8|   Rajesh|         B|       G| 25|         2|     Scala|   Nellore|1967-12-01|     M|  5100|
    |   9|  Pramodh|         B|       G| 49|         9|      Ruby|      Pune|1967-12-01|     M|  5000|
    |  10|     Ajay|      null|    null| 50|        10|      null|      Pune|1967-12-01|     M|  2500|
    |  11|      Bob|         D|    null| 43|        14|         R| Hyderabad|1967-12-01|     M|  2500|
    |  12|    Chris|         B|   Smith| 47|        12|JavaScript|      null|1967-12-01|     M|  2500|
    |  13|     null|      null|    null| 55|      null|      null|      null|      null|  null|  null|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    


# Convert dataframe to SQL object


```python
df.createOrReplaceTempView('PERSON_DATA')
```

# SQL Query operations on Dataframe


```python
df_sql = spark.sql("select * from PERSON_DATA")
```


```python
df_sql.show()
```

    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance|     skill|      city|       dob|gender|salary|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    |   1|    James|      null|   Smith| 51|        10|      .Net|   Newyork|1991-04-01|     M|  3000|
    |   2|  Michael|      Rose|    null| 45|        11|      Ruby|   Newyork|2000-05-19|     M|  4000|
    |   3|   Robert|      null|Williams| 32|        10|     Scala|California|1978-09-05|     M|  4000|
    |   4|    Maria|      Anne|   Jones| 36|         9|      Java| Hyderabad|1967-12-01|     F|  5000|
    |   5|      Jen|      Mary|   Brown| 39|         5|     Scala|    Nagpur|1980-02-17|     F|     0|
    |   6|Prabhakar|         B|       G| 33|        11|    Python|      Pune|1967-12-01|     M|  5000|
    |   7|  Praveen|         B|       G| 21|        13|      Java| Hyderabad|1967-12-01|     M|  6500|
    |   8|   Rajesh|         B|       G| 25|         2|     Scala|   Nellore|1967-12-01|     M|  5100|
    |   9|  Pramodh|         B|       G| 49|         9|      Ruby|      Pune|1967-12-01|     M|  5000|
    |  10|     Ajay|      null|    null| 50|        10|      null|      Pune|1967-12-01|     M|  2500|
    |  11|      Bob|         D|    null| 43|        14|         R| Hyderabad|1967-12-01|     M|  2500|
    |  12|    Chris|         B|   Smith| 47|        12|JavaScript|      null|1967-12-01|     M|  2500|
    |  13|     null|      null|    null| 55|      null|      null|      null|      null|  null|  null|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    



```python
spark.sql("select gender,count(*) from PERSON_DATA group by gender").show()
```

    +------+--------+
    |gender|count(1)|
    +------+--------+
    |     F|       2|
    |  null|       1|
    |     M|      10|
    +------+--------+
    



```python
spark.sql("select city,count(*) from PERSON_DATA group by city").show()
```

    +----------+--------+
    |      city|count(1)|
    +----------+--------+
    |      null|       2|
    |   Newyork|       2|
    |   Nellore|       1|
    |      Pune|       3|
    |    Nagpur|       1|
    | Hyderabad|       3|
    |California|       1|
    +----------+--------+
    



```python
spark.sql("select skill as Skill,count(*) as Total_Employees from PERSON_DATA group by skill").show()
```

    +----------+---------------+
    |     Skill|Total_Employees|
    +----------+---------------+
    |      .Net|              1|
    |JavaScript|              1|
    |      null|              2|
    |         R|              1|
    |     Scala|              3|
    |      Ruby|              2|
    |    Python|              1|
    |      Java|              2|
    +----------+---------------+
    



```python
spark.sql("select * from PERSON_DATA where firstname like '%P%'").show()
```

    +----+---------+----------+--------+---+----------+------+---------+----------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance| skill|     city|       dob|gender|salary|
    +----+---------+----------+--------+---+----------+------+---------+----------+------+------+
    |   6|Prabhakar|         B|       G| 33|        11|Python|     Pune|1967-12-01|     M|  5000|
    |   7|  Praveen|         B|       G| 21|        13|  Java|Hyderabad|1967-12-01|     M|  6500|
    |   9|  Pramodh|         B|       G| 49|         9|  Ruby|     Pune|1967-12-01|     M|  5000|
    +----+---------+----------+--------+---+----------+------+---------+----------+------+------+
    



```python
spark.sql("select * from PERSON_DATA where salary > 3000").show()
```

    +----+---------+----------+--------+---+----------+------+----------+----------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance| skill|      city|       dob|gender|salary|
    +----+---------+----------+--------+---+----------+------+----------+----------+------+------+
    |   2|  Michael|      Rose|    null| 45|        11|  Ruby|   Newyork|2000-05-19|     M|  4000|
    |   3|   Robert|      null|Williams| 32|        10| Scala|California|1978-09-05|     M|  4000|
    |   4|    Maria|      Anne|   Jones| 36|         9|  Java| Hyderabad|1967-12-01|     F|  5000|
    |   6|Prabhakar|         B|       G| 33|        11|Python|      Pune|1967-12-01|     M|  5000|
    |   7|  Praveen|         B|       G| 21|        13|  Java| Hyderabad|1967-12-01|     M|  6500|
    |   8|   Rajesh|         B|       G| 25|         2| Scala|   Nellore|1967-12-01|     M|  5100|
    |   9|  Pramodh|         B|       G| 49|         9|  Ruby|      Pune|1967-12-01|     M|  5000|
    +----+---------+----------+--------+---+----------+------+----------+----------+------+------+
    


# Read CSV file content


```python
df_csv = spark.read.csv('/home/pgadupudi/Documents/employees.csv',header=True, inferSchema=True)
df_csv.show()
```

    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance|     skill|      city|       dob|gender|salary|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    |   1|    James|      None|   Smith| 51|        10|      .Net|   Newyork|1991-04-01|     M|  3000|
    |   2|  Michael|      Rose|    None| 45|        11|      Ruby|   Newyork|2000-05-19|     M|  4000|
    |   3|   Robert|      None|Williams| 32|        10|     Scala|California|1978-09-05|     M|  4000|
    |   4|    Maria|      Anne|   Jones| 36|         9|      Java| Hyderabad|1967-12-01|     F|  5000|
    |   5|      Jen|      Mary|   Brown| 39|         5|     Scala|    Nagpur|1980-02-17|     F|     0|
    |   6|Prabhakar|         B|       G| 33|        11|    Python|      Pune|1967-12-01|     M|  5000|
    |   7|  Praveen|         B|       G| 21|        13|      Java| Hyderabad|1967-12-01|     M|  6500|
    |   8|   Rajesh|         B|       G| 25|         2|     Scala|   Nellore|1967-12-01|     M|  5100|
    |   9|  Pramodh|         B|       G| 49|         9|      Ruby|      Pune|1967-12-01|     M|  5000|
    |  10|     Ajay|      None|    None| 50|        10|      None|      Pune|1967-12-01|     M|  2500|
    |  11|      Bob|         D|    None| 43|        14|         R| Hyderabad|1967-12-01|     M|  2500|
    |  12|    Chris|         B|   Smith| 47|        12|JavaScript|      None|1967-12-01|     M|  2500|
    |  13|     None|      None|    None| 55|      None|      None|      None|      None|  None|  None|
    |  14|      Bob|         D|    None| 43|        14|         R| Hyderabad|1967-12-01|     M|  2500|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    


# Convert CSV file dataframe to SQL table



```python
df_csv.createOrReplaceTempView('EMPLOYEES')
spark.sql('select * from EMPLOYEES').show()
```

    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance|     skill|      city|       dob|gender|salary|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    |   1|    James|      None|   Smith| 51|        10|      .Net|   Newyork|1991-04-01|     M|  3000|
    |   2|  Michael|      Rose|    None| 45|        11|      Ruby|   Newyork|2000-05-19|     M|  4000|
    |   3|   Robert|      None|Williams| 32|        10|     Scala|California|1978-09-05|     M|  4000|
    |   4|    Maria|      Anne|   Jones| 36|         9|      Java| Hyderabad|1967-12-01|     F|  5000|
    |   5|      Jen|      Mary|   Brown| 39|         5|     Scala|    Nagpur|1980-02-17|     F|     0|
    |   6|Prabhakar|         B|       G| 33|        11|    Python|      Pune|1967-12-01|     M|  5000|
    |   7|  Praveen|         B|       G| 21|        13|      Java| Hyderabad|1967-12-01|     M|  6500|
    |   8|   Rajesh|         B|       G| 25|         2|     Scala|   Nellore|1967-12-01|     M|  5100|
    |   9|  Pramodh|         B|       G| 49|         9|      Ruby|      Pune|1967-12-01|     M|  5000|
    |  10|     Ajay|      None|    None| 50|        10|      None|      Pune|1967-12-01|     M|  2500|
    |  11|      Bob|         D|    None| 43|        14|         R| Hyderabad|1967-12-01|     M|  2500|
    |  12|    Chris|         B|   Smith| 47|        12|JavaScript|      None|1967-12-01|     M|  2500|
    |  13|     None|      None|    None| 55|      None|      None|      None|      None|  None|  None|
    |  14|      Bob|         D|    None| 43|        14|         R| Hyderabad|1967-12-01|     M|  2500|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    



```python
spark.sql("select * from EMPLOYEES where skill='Python'").show()
```

    +----+---------+----------+--------+---+----------+------+----+----------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance| skill|city|       dob|gender|salary|
    +----+---------+----------+--------+---+----------+------+----+----------+------+------+
    |   6|Prabhakar|         B|       G| 33|        11|Python|Pune|1967-12-01|     M|  5000|
    +----+---------+----------+--------+---+----------+------+----+----------+------+------+
    


# PySpark cheat sheet


```python
## ACtual Dataframe output
```


```python
df.show()
```

    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance|     skill|      city|       dob|gender|salary|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    |   1|    James|      null|   Smith| 51|        10|      .Net|   Newyork|1991-04-01|     M|  3000|
    |   2|  Michael|      Rose|    null| 45|        11|      Ruby|   Newyork|2000-05-19|     M|  4000|
    |   3|   Robert|      null|Williams| 32|        10|     Scala|California|1978-09-05|     M|  4000|
    |   4|    Maria|      Anne|   Jones| 36|         9|      Java| Hyderabad|1967-12-01|     F|  5000|
    |   5|      Jen|      Mary|   Brown| 39|         5|     Scala|    Nagpur|1980-02-17|     F|     0|
    |   6|Prabhakar|         B|       G| 33|        11|    Python|      Pune|1967-12-01|     M|  5000|
    |   7|  Praveen|         B|       G| 21|        13|      Java| Hyderabad|1967-12-01|     M|  6500|
    |   8|   Rajesh|         B|       G| 25|         2|     Scala|   Nellore|1967-12-01|     M|  5100|
    |   9|  Pramodh|         B|       G| 49|         9|      Ruby|      Pune|1967-12-01|     M|  5000|
    |  10|     Ajay|      null|    null| 50|        10|      null|      Pune|1967-12-01|     M|  2500|
    |  11|      Bob|         D|    null| 43|        14|         R| Hyderabad|1967-12-01|     M|  2500|
    |  12|    Chris|         B|   Smith| 47|        12|JavaScript|      null|1967-12-01|     M|  2500|
    |  13|     null|      null|    null| 55|      null|      null|      null|      null|  null|  null|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    


## Drop duplicates


```python
df.dropDuplicates().show()

# Here 'Prabhakar' user name entry was apperead two types. dropDuplicates function will remove duplicate entries
```

    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance|     skill|      city|       dob|gender|salary|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    |   8|   Rajesh|         B|       G| 25|         2|     Scala|   Nellore|1967-12-01|     M|  5100|
    |   6|Prabhakar|         B|       G| 33|        11|    Python|      Pune|1967-12-01|     M|  5000|
    |  10|     Ajay|      null|    null| 50|        10|      null|      Pune|1967-12-01|     M|  2500|
    |  11|      Bob|         D|    null| 43|        14|         R| Hyderabad|1967-12-01|     M|  2500|
    |   2|  Michael|      Rose|    null| 45|        11|      Ruby|   Newyork|2000-05-19|     M|  4000|
    |  12|    Chris|         B|   Smith| 47|        12|JavaScript|      null|1967-12-01|     M|  2500|
    |   7|  Praveen|         B|       G| 21|        13|      Java| Hyderabad|1967-12-01|     M|  6500|
    |   1|    James|      null|   Smith| 51|        10|      .Net|   Newyork|1991-04-01|     M|  3000|
    |   3|   Robert|      null|Williams| 32|        10|     Scala|California|1978-09-05|     M|  4000|
    |  13|     null|      null|    null| 55|      null|      null|      null|      null|  null|  null|
    |   4|    Maria|      Anne|   Jones| 36|         9|      Java| Hyderabad|1967-12-01|     F|  5000|
    |   5|      Jen|      Mary|   Brown| 39|         5|     Scala|    Nagpur|1980-02-17|     F|     0|
    |   9|  Pramodh|         B|       G| 49|         9|      Ruby|      Pune|1967-12-01|     M|  5000|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    


## Select


```python
df.select('firstname').show()
```

    +---------+
    |firstname|
    +---------+
    |    James|
    |  Michael|
    |   Robert|
    |    Maria|
    |      Jen|
    |Prabhakar|
    |  Praveen|
    |   Rajesh|
    |  Pramodh|
    |     Ajay|
    |      Bob|
    |    Chris|
    |     null|
    +---------+
    



```python
df.select('firstname', 'lastname', 'city').show()
```

    +---------+--------+----------+
    |firstname|lastname|      city|
    +---------+--------+----------+
    |    James|   Smith|   Newyork|
    |  Michael|    null|   Newyork|
    |   Robert|Williams|California|
    |    Maria|   Jones| Hyderabad|
    |      Jen|   Brown|    Nagpur|
    |Prabhakar|       G|      Pune|
    |  Praveen|       G| Hyderabad|
    |   Rajesh|       G|   Nellore|
    |  Pramodh|       G|      Pune|
    |     Ajay|    null|      Pune|
    |      Bob|    null| Hyderabad|
    |    Chris|   Smith|      null|
    |     null|    null|      null|
    +---------+--------+----------+
    



```python
df.select(df['firstname'], df['salary'] + 100).show()
# Adding +100 value on each user salary
```

    +---------+--------------+
    |firstname|(salary + 100)|
    +---------+--------------+
    |    James|          3100|
    |  Michael|          4100|
    |   Robert|          4100|
    |    Maria|          5100|
    |      Jen|           100|
    |Prabhakar|          5100|
    |  Praveen|          6600|
    |   Rajesh|          5200|
    |  Pramodh|          5100|
    |     Ajay|          2600|
    |      Bob|          2600|
    |    Chris|          2600|
    |     null|          null|
    +---------+--------------+
    



```python
df.select('firstname', df['salary'] > 4000).show()
```

    +---------+---------------+
    |firstname|(salary > 4000)|
    +---------+---------------+
    |    James|          false|
    |  Michael|          false|
    |   Robert|          false|
    |    Maria|           true|
    |      Jen|          false|
    |Prabhakar|           true|
    |  Praveen|           true|
    |   Rajesh|           true|
    |  Pramodh|           true|
    |     Ajay|          false|
    |      Bob|          false|
    |    Chris|          false|
    |     null|           null|
    +---------+---------------+
    



```python
df.select(df['salary']/df.count()).show()
```

    +------------------+
    |     (salary / 13)|
    +------------------+
    |230.76923076923077|
    | 307.6923076923077|
    | 307.6923076923077|
    |384.61538461538464|
    |               0.0|
    |384.61538461538464|
    |             500.0|
    | 392.3076923076923|
    |384.61538461538464|
    |192.30769230769232|
    |192.30769230769232|
    |192.30769230769232|
    |              null|
    +------------------+
    


## When 


```python
# df.select('firstname', df.when(df.city['Pune'], 1).otherwise(0))
```


```python
df[df.firstname.isin("Prabhakar", "Praveen")].show()
```

    +----+---------+----------+--------+---+----------+------+---------+----------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance| skill|     city|       dob|gender|salary|
    +----+---------+----------+--------+---+----------+------+---------+----------+------+------+
    |   6|Prabhakar|         B|       G| 33|        11|Python|     Pune|1967-12-01|     M|  5000|
    |   7|  Praveen|         B|       G| 21|        13|  Java|Hyderabad|1967-12-01|     M|  6500|
    +----+---------+----------+--------+---+----------+------+---------+----------+------+------+
    


## Like


```python
df.select('firstname', df.firstname.like('Prabhakar')).show()
```

    +---------+------------------------+
    |firstname|firstname LIKE Prabhakar|
    +---------+------------------------+
    |    James|                   false|
    |  Michael|                   false|
    |   Robert|                   false|
    |    Maria|                   false|
    |      Jen|                   false|
    |Prabhakar|                    true|
    |  Praveen|                   false|
    |   Rajesh|                   false|
    |  Pramodh|                   false|
    |     Ajay|                   false|
    |      Bob|                   false|
    |    Chris|                   false|
    |     null|                    null|
    +---------+------------------------+
    


## Startswith - Endswith


```python
df.select('firstname', df.firstname.startswith('Pr')).show()
```

    +---------+-------------------------+
    |firstname|startswith(firstname, Pr)|
    +---------+-------------------------+
    |    James|                    false|
    |  Michael|                    false|
    |   Robert|                    false|
    |    Maria|                    false|
    |      Jen|                    false|
    |Prabhakar|                     true|
    |  Praveen|                     true|
    |   Rajesh|                    false|
    |  Pramodh|                     true|
    |     Ajay|                    false|
    |      Bob|                    false|
    |    Chris|                    false|
    |     null|                     null|
    +---------+-------------------------+
    



```python
df.select('firstname', df.firstname.endswith('een')).show()
```

    +---------+------------------------+
    |firstname|endswith(firstname, een)|
    +---------+------------------------+
    |    James|                   false|
    |  Michael|                   false|
    |   Robert|                   false|
    |    Maria|                   false|
    |      Jen|                   false|
    |Prabhakar|                   false|
    |  Praveen|                    true|
    |   Rajesh|                   false|
    |  Pramodh|                   false|
    |     Ajay|                   false|
    |      Bob|                   false|
    |    Chris|                   false|
    |     null|                    null|
    +---------+------------------------+
    


## Substring


```python
df.select(df.firstname.substr(1,3).alias('name')).show()
## Displays first 3 characters from firstname column values
```

    +----+
    |name|
    +----+
    | Jam|
    | Mic|
    | Rob|
    | Mar|
    | Jen|
    | Pra|
    | Pra|
    | Raj|
    | Pra|
    | Aja|
    | Bob|
    | Chr|
    |null|
    +----+
    


## Between


```python
df.select('firstname', df.salary.between(4000, 5000)).show()
```

    +---------+---------------------------------------+
    |firstname|((salary >= 4000) AND (salary <= 5000))|
    +---------+---------------------------------------+
    |    James|                                  false|
    |  Michael|                                   true|
    |   Robert|                                   true|
    |    Maria|                                   true|
    |      Jen|                                  false|
    |Prabhakar|                                   true|
    |  Praveen|                                  false|
    |   Rajesh|                                  false|
    |  Pramodh|                                   true|
    |     Ajay|                                  false|
    |      Bob|                                  false|
    |    Chris|                                  false|
    |     null|                                   null|
    +---------+---------------------------------------+
    


# Add, Update & Remove columns from Dataframe

## Adding column


```python
df.withColumn('Place', df.city).show()
```

    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+----------+
    |SrNo|firstname|middlename|lastname|age|experiance|     skill|      city|       dob|gender|salary|     Place|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+----------+
    |   1|    James|      null|   Smith| 51|        10|      .Net|   Newyork|1991-04-01|     M|  3000|   Newyork|
    |   2|  Michael|      Rose|    null| 45|        11|      Ruby|   Newyork|2000-05-19|     M|  4000|   Newyork|
    |   3|   Robert|      null|Williams| 32|        10|     Scala|California|1978-09-05|     M|  4000|California|
    |   4|    Maria|      Anne|   Jones| 36|         9|      Java| Hyderabad|1967-12-01|     F|  5000| Hyderabad|
    |   5|      Jen|      Mary|   Brown| 39|         5|     Scala|    Nagpur|1980-02-17|     F|     0|    Nagpur|
    |   6|Prabhakar|         B|       G| 33|        11|    Python|      Pune|1967-12-01|     M|  5000|      Pune|
    |   7|  Praveen|         B|       G| 21|        13|      Java| Hyderabad|1967-12-01|     M|  6500| Hyderabad|
    |   8|   Rajesh|         B|       G| 25|         2|     Scala|   Nellore|1967-12-01|     M|  5100|   Nellore|
    |   9|  Pramodh|         B|       G| 49|         9|      Ruby|      Pune|1967-12-01|     M|  5000|      Pune|
    |  10|     Ajay|      null|    null| 50|        10|      null|      Pune|1967-12-01|     M|  2500|      Pune|
    |  11|      Bob|         D|    null| 43|        14|         R| Hyderabad|1967-12-01|     M|  2500| Hyderabad|
    |  12|    Chris|         B|   Smith| 47|        12|JavaScript|      null|1967-12-01|     M|  2500|      null|
    |  13|     null|      null|    null| 55|      null|      null|      null|      null|  null|  null|      null|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+----------+
    


## Updating column


```python
df.withColumnRenamed('dob', 'Date Of Birth').show()
```

    +----+---------+----------+--------+---+----------+----------+----------+-------------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance|     skill|      city|Date Of Birth|gender|salary|
    +----+---------+----------+--------+---+----------+----------+----------+-------------+------+------+
    |   1|    James|      null|   Smith| 51|        10|      .Net|   Newyork|   1991-04-01|     M|  3000|
    |   2|  Michael|      Rose|    null| 45|        11|      Ruby|   Newyork|   2000-05-19|     M|  4000|
    |   3|   Robert|      null|Williams| 32|        10|     Scala|California|   1978-09-05|     M|  4000|
    |   4|    Maria|      Anne|   Jones| 36|         9|      Java| Hyderabad|   1967-12-01|     F|  5000|
    |   5|      Jen|      Mary|   Brown| 39|         5|     Scala|    Nagpur|   1980-02-17|     F|     0|
    |   6|Prabhakar|         B|       G| 33|        11|    Python|      Pune|   1967-12-01|     M|  5000|
    |   7|  Praveen|         B|       G| 21|        13|      Java| Hyderabad|   1967-12-01|     M|  6500|
    |   8|   Rajesh|         B|       G| 25|         2|     Scala|   Nellore|   1967-12-01|     M|  5100|
    |   9|  Pramodh|         B|       G| 49|         9|      Ruby|      Pune|   1967-12-01|     M|  5000|
    |  10|     Ajay|      null|    null| 50|        10|      null|      Pune|   1967-12-01|     M|  2500|
    |  11|      Bob|         D|    null| 43|        14|         R| Hyderabad|   1967-12-01|     M|  2500|
    |  12|    Chris|         B|   Smith| 47|        12|JavaScript|      null|   1967-12-01|     M|  2500|
    |  13|     null|      null|    null| 55|      null|      null|      null|         null|  null|  null|
    +----+---------+----------+--------+---+----------+----------+----------+-------------+------+------+
    


## Remove column


```python
df.drop('middlename').show()
```

    +----+---------+--------+---+----------+----------+----------+----------+------+------+
    |SrNo|firstname|lastname|age|experiance|     skill|      city|       dob|gender|salary|
    +----+---------+--------+---+----------+----------+----------+----------+------+------+
    |   1|    James|   Smith| 51|        10|      .Net|   Newyork|1991-04-01|     M|  3000|
    |   2|  Michael|    null| 45|        11|      Ruby|   Newyork|2000-05-19|     M|  4000|
    |   3|   Robert|Williams| 32|        10|     Scala|California|1978-09-05|     M|  4000|
    |   4|    Maria|   Jones| 36|         9|      Java| Hyderabad|1967-12-01|     F|  5000|
    |   5|      Jen|   Brown| 39|         5|     Scala|    Nagpur|1980-02-17|     F|     0|
    |   6|Prabhakar|       G| 33|        11|    Python|      Pune|1967-12-01|     M|  5000|
    |   7|  Praveen|       G| 21|        13|      Java| Hyderabad|1967-12-01|     M|  6500|
    |   8|   Rajesh|       G| 25|         2|     Scala|   Nellore|1967-12-01|     M|  5100|
    |   9|  Pramodh|       G| 49|         9|      Ruby|      Pune|1967-12-01|     M|  5000|
    |  10|     Ajay|    null| 50|        10|      null|      Pune|1967-12-01|     M|  2500|
    |  11|      Bob|    null| 43|        14|         R| Hyderabad|1967-12-01|     M|  2500|
    |  12|    Chris|   Smith| 47|        12|JavaScript|      null|1967-12-01|     M|  2500|
    |  13|     null|    null| 55|      null|      null|      null|      null|  null|  null|
    +----+---------+--------+---+----------+----------+----------+----------+------+------+
    



```python
df.na.drop().show()
```

    +----+---------+----------+--------+---+----------+------+---------+----------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance| skill|     city|       dob|gender|salary|
    +----+---------+----------+--------+---+----------+------+---------+----------+------+------+
    |   4|    Maria|      Anne|   Jones| 36|         9|  Java|Hyderabad|1967-12-01|     F|  5000|
    |   5|      Jen|      Mary|   Brown| 39|         5| Scala|   Nagpur|1980-02-17|     F|     0|
    |   6|Prabhakar|         B|       G| 33|        11|Python|     Pune|1967-12-01|     M|  5000|
    |   7|  Praveen|         B|       G| 21|        13|  Java|Hyderabad|1967-12-01|     M|  6500|
    |   8|   Rajesh|         B|       G| 25|         2| Scala|  Nellore|1967-12-01|     M|  5100|
    |   9|  Pramodh|         B|       G| 49|         9|  Ruby|     Pune|1967-12-01|     M|  5000|
    +----+---------+----------+--------+---+----------+------+---------+----------+------+------+
    


# Drop if all columns contains NULL/ NONE 


```python
df.na.drop(how='all').show()
```

    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance|     skill|      city|       dob|gender|salary|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    |   1|    James|      null|   Smith| 51|        10|      .Net|   Newyork|1991-04-01|     M|  3000|
    |   2|  Michael|      Rose|    null| 45|        11|      Ruby|   Newyork|2000-05-19|     M|  4000|
    |   3|   Robert|      null|Williams| 32|        10|     Scala|California|1978-09-05|     M|  4000|
    |   4|    Maria|      Anne|   Jones| 36|         9|      Java| Hyderabad|1967-12-01|     F|  5000|
    |   5|      Jen|      Mary|   Brown| 39|         5|     Scala|    Nagpur|1980-02-17|     F|     0|
    |   6|Prabhakar|         B|       G| 33|        11|    Python|      Pune|1967-12-01|     M|  5000|
    |   7|  Praveen|         B|       G| 21|        13|      Java| Hyderabad|1967-12-01|     M|  6500|
    |   8|   Rajesh|         B|       G| 25|         2|     Scala|   Nellore|1967-12-01|     M|  5100|
    |   9|  Pramodh|         B|       G| 49|         9|      Ruby|      Pune|1967-12-01|     M|  5000|
    |  10|     Ajay|      null|    null| 50|        10|      null|      Pune|1967-12-01|     M|  2500|
    |  11|      Bob|         D|    null| 43|        14|         R| Hyderabad|1967-12-01|     M|  2500|
    |  12|    Chris|         B|   Smith| 47|        12|JavaScript|      null|1967-12-01|     M|  2500|
    |  13|     null|      null|    null| 55|      null|      null|      null|      null|  null|  null|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    


# Drop rows if 'any' column contains NULL/ NONE 


```python
df.na.drop(how='any').show()
```

    +----+---------+----------+--------+---+----------+------+---------+----------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance| skill|     city|       dob|gender|salary|
    +----+---------+----------+--------+---+----------+------+---------+----------+------+------+
    |   4|    Maria|      Anne|   Jones| 36|         9|  Java|Hyderabad|1967-12-01|     F|  5000|
    |   5|      Jen|      Mary|   Brown| 39|         5| Scala|   Nagpur|1980-02-17|     F|     0|
    |   6|Prabhakar|         B|       G| 33|        11|Python|     Pune|1967-12-01|     M|  5000|
    |   7|  Praveen|         B|       G| 21|        13|  Java|Hyderabad|1967-12-01|     M|  6500|
    |   8|   Rajesh|         B|       G| 25|         2| Scala|  Nellore|1967-12-01|     M|  5100|
    |   9|  Pramodh|         B|       G| 49|         9|  Ruby|     Pune|1967-12-01|     M|  5000|
    +----+---------+----------+--------+---+----------+------+---------+----------+------+------+
    


# Filling the missing/ blank values any column with 'Missing Value'


```python
df.na.fill(0,['salary', 'experiance']).na.fill('Missing Value').show()
```

    +----+-------------+-------------+-------------+---+----------+-------------+-------------+-------------+-------------+------+
    |SrNo|    firstname|   middlename|     lastname|age|experiance|        skill|         city|          dob|       gender|salary|
    +----+-------------+-------------+-------------+---+----------+-------------+-------------+-------------+-------------+------+
    |   1|        James|Missing Value|        Smith| 51|        10|         .Net|      Newyork|   1991-04-01|            M|  3000|
    |   2|      Michael|         Rose|Missing Value| 45|        11|         Ruby|      Newyork|   2000-05-19|            M|  4000|
    |   3|       Robert|Missing Value|     Williams| 32|        10|        Scala|   California|   1978-09-05|            M|  4000|
    |   4|        Maria|         Anne|        Jones| 36|         9|         Java|    Hyderabad|   1967-12-01|            F|  5000|
    |   5|          Jen|         Mary|        Brown| 39|         5|        Scala|       Nagpur|   1980-02-17|            F|     0|
    |   6|    Prabhakar|            B|            G| 33|        11|       Python|         Pune|   1967-12-01|            M|  5000|
    |   7|      Praveen|            B|            G| 21|        13|         Java|    Hyderabad|   1967-12-01|            M|  6500|
    |   8|       Rajesh|            B|            G| 25|         2|        Scala|      Nellore|   1967-12-01|            M|  5100|
    |   9|      Pramodh|            B|            G| 49|         9|         Ruby|         Pune|   1967-12-01|            M|  5000|
    |  10|         Ajay|Missing Value|Missing Value| 50|        10|Missing Value|         Pune|   1967-12-01|            M|  2500|
    |  11|          Bob|            D|Missing Value| 43|        14|            R|    Hyderabad|   1967-12-01|            M|  2500|
    |  12|        Chris|            B|        Smith| 47|        12|   JavaScript|Missing Value|   1967-12-01|            M|  2500|
    |  13|Missing Value|Missing Value|Missing Value| 55|         0|Missing Value|Missing Value|Missing Value|Missing Value|     0|
    +----+-------------+-------------+-------------+---+----------+-------------+-------------+-------------+-------------+------+
    


# Dataframes - Filter operations
Filter Operation<br/>
&,|,==<br/>
~<br/>



```python
df.filter("experiance > 10").show()
```

    +----+---------+----------+--------+---+----------+----------+---------+----------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance|     skill|     city|       dob|gender|salary|
    +----+---------+----------+--------+---+----------+----------+---------+----------+------+------+
    |   2|  Michael|      Rose|    null| 45|        11|      Ruby|  Newyork|2000-05-19|     M|  4000|
    |   6|Prabhakar|         B|       G| 33|        11|    Python|     Pune|1967-12-01|     M|  5000|
    |   7|  Praveen|         B|       G| 21|        13|      Java|Hyderabad|1967-12-01|     M|  6500|
    |  11|      Bob|         D|    null| 43|        14|         R|Hyderabad|1967-12-01|     M|  2500|
    |  12|    Chris|         B|   Smith| 47|        12|JavaScript|     null|1967-12-01|     M|  2500|
    +----+---------+----------+--------+---+----------+----------+---------+----------+------+------+
    



```python
df.filter('experiance > 10').select(['SrNo', 'firstname','lastname']).show()
```

    +----+---------+--------+
    |SrNo|firstname|lastname|
    +----+---------+--------+
    |   2|  Michael|    null|
    |   6|Prabhakar|       G|
    |   7|  Praveen|       G|
    |  11|      Bob|    null|
    |  12|    Chris|   Smith|
    +----+---------+--------+
    



```python
df_temp = df.na.fill(0,['experiance','salary'])
# df_temp.filter(df['experiance'] >= 10 | df['experiance'] <= 15).show()
df_temp.filter(df_temp['experiance'] > 10).show()
```

    +----+---------+----------+--------+---+----------+----------+---------+----------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance|     skill|     city|       dob|gender|salary|
    +----+---------+----------+--------+---+----------+----------+---------+----------+------+------+
    |   2|  Michael|      Rose|    null| 45|        11|      Ruby|  Newyork|2000-05-19|     M|  4000|
    |   6|Prabhakar|         B|       G| 33|        11|    Python|     Pune|1967-12-01|     M|  5000|
    |   7|  Praveen|         B|       G| 21|        13|      Java|Hyderabad|1967-12-01|     M|  6500|
    |  11|      Bob|         D|    null| 43|        14|         R|Hyderabad|1967-12-01|     M|  2500|
    |  12|    Chris|         B|   Smith| 47|        12|JavaScript|     null|1967-12-01|     M|  2500|
    +----+---------+----------+--------+---+----------+----------+---------+----------+------+------+
    


# Agregated And GroupBy functions


```python
df_temp.filter(~(df_temp['experiance'] > 10)).show()
```

    +----+---------+----------+--------+---+----------+-----+----------+----------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance|skill|      city|       dob|gender|salary|
    +----+---------+----------+--------+---+----------+-----+----------+----------+------+------+
    |   1|    James|      null|   Smith| 51|        10| .Net|   Newyork|1991-04-01|     M|  3000|
    |   3|   Robert|      null|Williams| 32|        10|Scala|California|1978-09-05|     M|  4000|
    |   4|    Maria|      Anne|   Jones| 36|         9| Java| Hyderabad|1967-12-01|     F|  5000|
    |   5|      Jen|      Mary|   Brown| 39|         5|Scala|    Nagpur|1980-02-17|     F|     0|
    |   8|   Rajesh|         B|       G| 25|         2|Scala|   Nellore|1967-12-01|     M|  5100|
    |   9|  Pramodh|         B|       G| 49|         9| Ruby|      Pune|1967-12-01|     M|  5000|
    |  10|     Ajay|      null|    null| 50|        10| null|      Pune|1967-12-01|     M|  2500|
    |  13|     null|      null|    null| 55|         0| null|      null|      null|  null|     0|
    +----+---------+----------+--------+---+----------+-----+----------+----------+------+------+
    



```python
df_temp.printSchema()
```

    root
     |-- SrNo: long (nullable = true)
     |-- firstname: string (nullable = true)
     |-- middlename: string (nullable = true)
     |-- lastname: string (nullable = true)
     |-- age: long (nullable = true)
     |-- experiance: long (nullable = true)
     |-- skill: string (nullable = true)
     |-- city: string (nullable = true)
     |-- dob: string (nullable = true)
     |-- gender: string (nullable = true)
     |-- salary: long (nullable = true)
    


# GroupBy


```python
df_temp.groupBy('city').sum().show()
```

    +----------+---------+--------+---------------+-----------+
    |      city|sum(SrNo)|sum(age)|sum(experiance)|sum(salary)|
    +----------+---------+--------+---------------+-----------+
    |      null|       25|     102|             12|       2500|
    |   Newyork|        3|      96|             21|       7000|
    |   Nellore|        8|      25|              2|       5100|
    |      Pune|       25|     132|             30|      12500|
    |    Nagpur|        5|      39|              5|          0|
    | Hyderabad|       22|     100|             36|      14000|
    |California|        3|      32|             10|       4000|
    +----------+---------+--------+---------------+-----------+
    



```python
df_temp.groupBy('skill').sum().show()
```

    +----------+---------+--------+---------------+-----------+
    |     skill|sum(SrNo)|sum(age)|sum(experiance)|sum(salary)|
    +----------+---------+--------+---------------+-----------+
    |      .Net|        1|      51|             10|       3000|
    |JavaScript|       12|      47|             12|       2500|
    |      null|       23|     105|             10|       2500|
    |         R|       11|      43|             14|       2500|
    |     Scala|       16|      96|             17|       9100|
    |      Ruby|       11|      94|             20|       9000|
    |    Python|        6|      33|             11|       5000|
    |      Java|       11|      57|             22|      11500|
    +----------+---------+--------+---------------+-----------+
    



```python
df_temp.groupBy('skill').count().show()
```

    +----------+-----+
    |     skill|count|
    +----------+-----+
    |      .Net|    1|
    |JavaScript|    1|
    |      null|    2|
    |         R|    1|
    |     Scala|    3|
    |      Ruby|    2|
    |    Python|    1|
    |      Java|    2|
    +----------+-----+
    



```python
df_temp.groupBy('salary').min().show()
```

    +------+---------+--------+---------------+-----------+
    |salary|min(SrNo)|min(age)|min(experiance)|min(salary)|
    +------+---------+--------+---------------+-----------+
    |     0|        5|      39|              0|          0|
    |  5100|        8|      25|              2|       5100|
    |  4000|        2|      32|             10|       4000|
    |  6500|        7|      21|             13|       6500|
    |  2500|       10|      43|             10|       2500|
    |  3000|        1|      51|             10|       3000|
    |  5000|        4|      33|              9|       5000|
    +------+---------+--------+---------------+-----------+
    



```python
df_temp.groupBy('salary').max().show()
```

    +------+---------+--------+---------------+-----------+
    |salary|max(SrNo)|max(age)|max(experiance)|max(salary)|
    +------+---------+--------+---------------+-----------+
    |     0|       13|      55|              5|          0|
    |  5100|        8|      25|              2|       5100|
    |  4000|        3|      45|             11|       4000|
    |  6500|        7|      21|             13|       6500|
    |  2500|       12|      50|             14|       2500|
    |  3000|        1|      51|             10|       3000|
    |  5000|        9|      49|             11|       5000|
    +------+---------+--------+---------------+-----------+
    



```python
df_temp.groupBy('salary').avg().show()
```

    +------+-----------------+------------------+-----------------+-----------+
    |salary|        avg(SrNo)|          avg(age)|  avg(experiance)|avg(salary)|
    +------+-----------------+------------------+-----------------+-----------+
    |     0|              9.0|              47.0|              2.5|        0.0|
    |  5100|              8.0|              25.0|              2.0|     5100.0|
    |  4000|              2.5|              38.5|             10.5|     4000.0|
    |  6500|              7.0|              21.0|             13.0|     6500.0|
    |  2500|             11.0|46.666666666666664|             12.0|     2500.0|
    |  3000|              1.0|              51.0|             10.0|     3000.0|
    |  5000|6.333333333333333|39.333333333333336|9.666666666666666|     5000.0|
    +------+-----------------+------------------+-----------------+-----------+
    



```python
df_temp.show()
```

    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    |SrNo|firstname|middlename|lastname|age|experiance|     skill|      city|       dob|gender|salary|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    |   1|    James|      null|   Smith| 51|        10|      .Net|   Newyork|1991-04-01|     M|  3000|
    |   2|  Michael|      Rose|    null| 45|        11|      Ruby|   Newyork|2000-05-19|     M|  4000|
    |   3|   Robert|      null|Williams| 32|        10|     Scala|California|1978-09-05|     M|  4000|
    |   4|    Maria|      Anne|   Jones| 36|         9|      Java| Hyderabad|1967-12-01|     F|  5000|
    |   5|      Jen|      Mary|   Brown| 39|         5|     Scala|    Nagpur|1980-02-17|     F|     0|
    |   6|Prabhakar|         B|       G| 33|        11|    Python|      Pune|1967-12-01|     M|  5000|
    |   7|  Praveen|         B|       G| 21|        13|      Java| Hyderabad|1967-12-01|     M|  6500|
    |   8|   Rajesh|         B|       G| 25|         2|     Scala|   Nellore|1967-12-01|     M|  5100|
    |   9|  Pramodh|         B|       G| 49|         9|      Ruby|      Pune|1967-12-01|     M|  5000|
    |  10|     Ajay|      null|    null| 50|        10|      null|      Pune|1967-12-01|     M|  2500|
    |  11|      Bob|         D|    null| 43|        14|         R| Hyderabad|1967-12-01|     M|  2500|
    |  12|    Chris|         B|   Smith| 47|        12|JavaScript|      null|1967-12-01|     M|  2500|
    |  13|     null|      null|    null| 55|         0|      null|      null|      null|  null|     0|
    +----+---------+----------+--------+---+----------+----------+----------+----------+------+------+
    



```python

```
