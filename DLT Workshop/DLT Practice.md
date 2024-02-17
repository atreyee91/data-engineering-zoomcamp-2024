# Install DLT

```python
!pip install dlt[duckdb]
```


```python
import dlt
```

# Use a generator


```python
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1
```


```python
# Example usage:
limit = 5
generator = square_root_generator(limit)
r=0

for sqrt_value in generator:
    r=r+sqrt_value

print(r)
```

    8.382332347441762
    


```python
# Example usage:
limit = 13
generator = square_root_generator(limit)
r=0

for sqrt_value in generator:
    print(sqrt_value)
```

    1.0
    1.4142135623730951
    1.7320508075688772
    2.0
    2.23606797749979
    2.449489742783178
    2.6457513110645907
    2.8284271247461903
    3.0
    3.1622776601683795
    3.3166247903554
    3.4641016151377544
    3.605551275463989
    

# Append a generator to a table with existing data


```python
def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

for person in people_1():
    print(person)
```

    {'ID': 1, 'Name': 'Person_1', 'Age': 26, 'City': 'City_A'}
    {'ID': 2, 'Name': 'Person_2', 'Age': 27, 'City': 'City_A'}
    {'ID': 3, 'Name': 'Person_3', 'Age': 28, 'City': 'City_A'}
    {'ID': 4, 'Name': 'Person_4', 'Age': 29, 'City': 'City_A'}
    {'ID': 5, 'Name': 'Person_5', 'Age': 30, 'City': 'City_A'}
    


```python
r=0
for person in people_1():
    r=r+person['Age']
print(r)
```

    140
    


```python
def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


for person in people_2():
    print(person)
```

    {'ID': 3, 'Name': 'Person_3', 'Age': 33, 'City': 'City_B', 'Occupation': 'Job_3'}
    {'ID': 4, 'Name': 'Person_4', 'Age': 34, 'City': 'City_B', 'Occupation': 'Job_4'}
    {'ID': 5, 'Name': 'Person_5', 'Age': 35, 'City': 'City_B', 'Occupation': 'Job_5'}
    {'ID': 6, 'Name': 'Person_6', 'Age': 36, 'City': 'City_B', 'Occupation': 'Job_6'}
    {'ID': 7, 'Name': 'Person_7', 'Age': 37, 'City': 'City_B', 'Occupation': 'Job_7'}
    {'ID': 8, 'Name': 'Person_8', 'Age': 38, 'City': 'City_B', 'Occupation': 'Job_8'}
    


```python
# define the connection to load to.
pipeline = dlt.pipeline(destination='duckdb', dataset_name='peopleset')

# run the pipeline with default settings, and capture the outcome
info = pipeline.run(people_1,
                    table_name="peopleset",
                    write_disposition="replace",
                    primary_key='ID')

print(info)
```

    Pipeline dlt_ipykernel_launcher load step completed in 0.27 seconds
    1 load package(s) were loaded to destination duckdb and into dataset peopleset
    The duckdb destination used duckdb:///C:\Users\scl\dlt_ipykernel_launcher.duckdb location to store data
    Load package 1708155976.3690548 is LOADED and contains no failed jobs
    


```python
# define the connection to load to.
pipeline = dlt.pipeline(destination='duckdb', dataset_name='peopleset')

# run the pipeline with default settings, and capture the outcome
info = pipeline.run(people_2,
                    table_name="peopleset",
                    write_disposition="append",
                    primary_key='ID')

print(info)
```

    Pipeline dlt_ipykernel_launcher load step completed in 0.23 seconds
    1 load package(s) were loaded to destination duckdb and into dataset peopleset
    The duckdb destination used duckdb:///C:\Users\scl\dlt_ipykernel_launcher.duckdb location to store data
    Load package 1708155985.7650282 is LOADED and contains no failed jobs
    


```python
# show outcome

import duckdb

conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# let's see the tables
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))

print("\n\n\n People table below: Note the times are properly typed")
people = conn.sql("SELECT * FROM peopleset").df()
display(people)

```

    Loaded tables: 
    


    ┌─────────────────────┐
    │        name         │
    │       varchar       │
    ├─────────────────────┤
    │ _dlt_loads          │
    │ _dlt_pipeline_state │
    │ _dlt_version        │
    │ peopleset           │
    └─────────────────────┘


    
    
    
     People table below: Note the times are properly typed
    


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>name</th>
      <th>age</th>
      <th>city</th>
      <th>_dlt_load_id</th>
      <th>_dlt_id</th>
      <th>occupation</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Person_1</td>
      <td>26</td>
      <td>City_A</td>
      <td>1708155976.3690548</td>
      <td>8dfZK6jBldHQcw</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Person_2</td>
      <td>27</td>
      <td>City_A</td>
      <td>1708155976.3690548</td>
      <td>NMzvDDkKbS8ttg</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Person_3</td>
      <td>28</td>
      <td>City_A</td>
      <td>1708155976.3690548</td>
      <td>72QTgAqWh6l0oA</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Person_4</td>
      <td>29</td>
      <td>City_A</td>
      <td>1708155976.3690548</td>
      <td>OMzrqC/T5FpFqA</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Person_5</td>
      <td>30</td>
      <td>City_A</td>
      <td>1708155976.3690548</td>
      <td>dNnUqt6Y/0Jw/w</td>
      <td>None</td>
    </tr>
    <tr>
      <th>5</th>
      <td>3</td>
      <td>Person_3</td>
      <td>33</td>
      <td>City_B</td>
      <td>1708155985.7650282</td>
      <td>WuRGyfN4wA3rJA</td>
      <td>Job_3</td>
    </tr>
    <tr>
      <th>6</th>
      <td>4</td>
      <td>Person_4</td>
      <td>34</td>
      <td>City_B</td>
      <td>1708155985.7650282</td>
      <td>Klh8P1S5palEVw</td>
      <td>Job_4</td>
    </tr>
    <tr>
      <th>7</th>
      <td>5</td>
      <td>Person_5</td>
      <td>35</td>
      <td>City_B</td>
      <td>1708155985.7650282</td>
      <td>XuVUukMV3s79Tg</td>
      <td>Job_5</td>
    </tr>
    <tr>
      <th>8</th>
      <td>6</td>
      <td>Person_6</td>
      <td>36</td>
      <td>City_B</td>
      <td>1708155985.7650282</td>
      <td>cSgrX9/wYsIcAA</td>
      <td>Job_6</td>
    </tr>
    <tr>
      <th>9</th>
      <td>7</td>
      <td>Person_7</td>
      <td>37</td>
      <td>City_B</td>
      <td>1708155985.7650282</td>
      <td>7Bv2dOhM/evTcg</td>
      <td>Job_7</td>
    </tr>
    <tr>
      <th>10</th>
      <td>8</td>
      <td>Person_8</td>
      <td>38</td>
      <td>City_B</td>
      <td>1708155985.7650282</td>
      <td>dbrCVbSCw14n0w</td>
      <td>Job_8</td>
    </tr>
  </tbody>
</table>
</div>



```python
age = conn.sql("select sum(age) from peopleset").df()
display(age)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sum(age)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>353.0</td>
    </tr>
  </tbody>
</table>
</div>



```python
# define the connection to load to.
pipeline = dlt.pipeline(destination='duckdb', dataset_name='merge_people')

# run the pipeline with default settings, and capture the outcome
info = pipeline.run(people_1,
                    table_name="merge_people",
                    write_disposition="replace",
                    primary_key='ID')

print(info)
```

    Pipeline dlt_ipykernel_launcher load step completed in 0.26 seconds
    1 load package(s) were loaded to destination duckdb and into dataset merge_people
    The duckdb destination used duckdb:///C:\Users\scl\dlt_ipykernel_launcher.duckdb location to store data
    Load package 1708155801.526567 is LOADED and contains no failed jobs
    


```python
# define the connection to load to.
pipeline = dlt.pipeline(destination='duckdb', dataset_name='merge_people')

# run the pipeline with default settings, and capture the outcome
info = pipeline.run(people_2,
                    table_name="merge_people",
                    write_disposition="merge",
                    primary_key='ID')

print(info)
```

    Pipeline dlt_ipykernel_launcher load step completed in 0.41 seconds
    1 load package(s) were loaded to destination duckdb and into dataset merge_people
    The duckdb destination used duckdb:///C:\Users\scl\dlt_ipykernel_launcher.duckdb location to store data
    Load package 1708155875.2275379 is LOADED and contains no failed jobs
    


```python

import duckdb

conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# let's see the tables
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))

print("\n\n\n People table below: Note the times are properly typed")
people = conn.sql("SELECT * FROM merge_people").df()
display(people)

```

    Loaded tables: 
    


    ┌─────────────────────┐
    │        name         │
    │       varchar       │
    ├─────────────────────┤
    │ _dlt_loads          │
    │ _dlt_pipeline_state │
    │ _dlt_version        │
    │ merge_people        │
    └─────────────────────┘


    
    
    
     People table below: Note the times are properly typed
    


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>name</th>
      <th>age</th>
      <th>city</th>
      <th>_dlt_load_id</th>
      <th>_dlt_id</th>
      <th>occupation</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Person_1</td>
      <td>26</td>
      <td>City_A</td>
      <td>1708155801.526567</td>
      <td>DfzfSekPk1OqCw</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Person_2</td>
      <td>27</td>
      <td>City_A</td>
      <td>1708155801.526567</td>
      <td>zTF8eMIZeB632g</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>8</td>
      <td>Person_8</td>
      <td>38</td>
      <td>City_B</td>
      <td>1708155875.2275379</td>
      <td>pddXHugbidBPYg</td>
      <td>Job_8</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Person_4</td>
      <td>34</td>
      <td>City_B</td>
      <td>1708155875.2275379</td>
      <td>4oA6ME7zsdftLw</td>
      <td>Job_4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Person_5</td>
      <td>35</td>
      <td>City_B</td>
      <td>1708155875.2275379</td>
      <td>uWTFKnoX8s/Gfw</td>
      <td>Job_5</td>
    </tr>
    <tr>
      <th>5</th>
      <td>3</td>
      <td>Person_3</td>
      <td>33</td>
      <td>City_B</td>
      <td>1708155875.2275379</td>
      <td>H7oSf34l0PG+Bg</td>
      <td>Job_3</td>
    </tr>
    <tr>
      <th>6</th>
      <td>6</td>
      <td>Person_6</td>
      <td>36</td>
      <td>City_B</td>
      <td>1708155875.2275379</td>
      <td>XGn5dbT+08YD+Q</td>
      <td>Job_6</td>
    </tr>
    <tr>
      <th>7</th>
      <td>7</td>
      <td>Person_7</td>
      <td>37</td>
      <td>City_B</td>
      <td>1708155875.2275379</td>
      <td>h/32mjx5OfGK2A</td>
      <td>Job_7</td>
    </tr>
  </tbody>
</table>
</div>



```python
merge_age = conn.sql("select sum(age) from merge_people").df()
display(merge_age)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sum(age)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>266.0</td>
    </tr>
  </tbody>
</table>
</div>



```python

```
