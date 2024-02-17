```python
pip install dlt[duckdb]
```

    Collecting dlt[duckdb]
      Obtaining dependency information for dlt[duckdb] from https://files.pythonhosted.org/packages/5b/0a/3bcfec9a96d8f77b0cd8e3ce34ab452a92bcddfb5398b008e2ea2ae2a5a0/dlt-0.4.4-py3-none-any.whl.metadata
      Downloading dlt-0.4.4-py3-none-any.whl.metadata (9.7 kB)
    Requirement already satisfied: PyYAML>=5.4.1 in c:\programdata\anaconda3\lib\site-packages (from dlt[duckdb]) (6.0)
    Requirement already satisfied: SQLAlchemy>=1.4.0 in c:\programdata\anaconda3\lib\site-packages (from dlt[duckdb]) (1.4.39)
    Collecting astunparse>=1.6.3 (from dlt[duckdb])
      Downloading astunparse-1.6.3-py2.py3-none-any.whl (12 kB)
    Requirement already satisfied: click>=7.1 in c:\programdata\anaconda3\lib\site-packages (from dlt[duckdb]) (8.0.4)
    Collecting duckdb<0.10.0,>=0.6.1 (from dlt[duckdb])
      Obtaining dependency information for duckdb<0.10.0,>=0.6.1 from https://files.pythonhosted.org/packages/a6/7a/e15904563fa63d7d3cc542a697377486ddea8ba4914810391812ffefc9b1/duckdb-0.9.2-cp311-cp311-win_amd64.whl.metadata
      Downloading duckdb-0.9.2-cp311-cp311-win_amd64.whl.metadata (798 bytes)
    Requirement already satisfied: fsspec>=2022.4.0 in c:\programdata\anaconda3\lib\site-packages (from dlt[duckdb]) (2023.4.0)
    Collecting gitpython>=3.1.29 (from dlt[duckdb])
      Obtaining dependency information for gitpython>=3.1.29 from https://files.pythonhosted.org/packages/67/c7/995360c87dd74e27539ccbfecddfb58e08f140d849fcd7f35d2ed1a5f80f/GitPython-3.1.42-py3-none-any.whl.metadata
      Downloading GitPython-3.1.42-py3-none-any.whl.metadata (12 kB)
    Collecting giturlparse>=0.10.0 (from dlt[duckdb])
      Obtaining dependency information for giturlparse>=0.10.0 from https://files.pythonhosted.org/packages/dd/94/c6ff3388b8e3225a014e55aed957188639aa0966443e0408d38f0c9614a7/giturlparse-0.12.0-py2.py3-none-any.whl.metadata
      Downloading giturlparse-0.12.0-py2.py3-none-any.whl.metadata (4.5 kB)
    Collecting hexbytes>=0.2.2 (from dlt[duckdb])
      Obtaining dependency information for hexbytes>=0.2.2 from https://files.pythonhosted.org/packages/73/99/f6beab7c3fe2ad62f6763a5e92af476225d74408b2588fe5614a67c63ee4/hexbytes-1.0.0-py3-none-any.whl.metadata
      Downloading hexbytes-1.0.0-py3-none-any.whl.metadata (5.3 kB)
    Collecting humanize>=4.4.0 (from dlt[duckdb])
      Obtaining dependency information for humanize>=4.4.0 from https://files.pythonhosted.org/packages/aa/2b/2ae0c789fd08d5b44e745726d08a17e6d3d7d09071d05473105edc7615f2/humanize-4.9.0-py3-none-any.whl.metadata
      Downloading humanize-4.9.0-py3-none-any.whl.metadata (7.9 kB)
    Collecting jsonpath-ng>=1.5.3 (from dlt[duckdb])
      Obtaining dependency information for jsonpath-ng>=1.5.3 from https://files.pythonhosted.org/packages/16/0a/3b1ee3721b4bd684b0e29c724ab82ed3b2c0e42285beb8bf9e18de8c903f/jsonpath_ng-1.6.1-py3-none-any.whl.metadata
      Downloading jsonpath_ng-1.6.1-py3-none-any.whl.metadata (18 kB)
    Collecting makefun>=1.15.0 (from dlt[duckdb])
      Obtaining dependency information for makefun>=1.15.0 from https://files.pythonhosted.org/packages/47/45/51d50062d95a0c2fd8f5f1cc8849878ea5c76d2f6a049a0b9d449272e97f/makefun-1.15.2-py2.py3-none-any.whl.metadata
      Downloading makefun-1.15.2-py2.py3-none-any.whl.metadata (3.0 kB)
    Collecting orjson<=3.9.10,>=3.6.7 (from dlt[duckdb])
      Obtaining dependency information for orjson<=3.9.10,>=3.6.7 from https://files.pythonhosted.org/packages/5d/67/d7837cf0ac956e3c81c67dda3e8f2ffc60dd50ffc480ec7c17f2e22a36ae/orjson-3.9.10-cp311-none-win_amd64.whl.metadata
      Downloading orjson-3.9.10-cp311-none-win_amd64.whl.metadata (50 kB)
         ---------------------------------------- 0.0/50.5 kB ? eta -:--:--
         ---------------------------------------- 50.5/50.5 kB 2.5 MB/s eta 0:00:00
    Requirement already satisfied: packaging>=21.1 in c:\programdata\anaconda3\lib\site-packages (from dlt[duckdb]) (23.1)
    Collecting pathvalidate>=2.5.2 (from dlt[duckdb])
      Obtaining dependency information for pathvalidate>=2.5.2 from https://files.pythonhosted.org/packages/0c/ab/673cce13ab635fd755d206b18c0a371ef6e28ddbe25fadba9ae6c59f22a5/pathvalidate-3.2.0-py3-none-any.whl.metadata
      Downloading pathvalidate-3.2.0-py3-none-any.whl.metadata (11 kB)
    Requirement already satisfied: pendulum>=2.1.2 in c:\programdata\anaconda3\lib\site-packages (from dlt[duckdb]) (2.1.2)
    Requirement already satisfied: pytz>=2022.6 in c:\programdata\anaconda3\lib\site-packages (from dlt[duckdb]) (2023.3.post1)
    Requirement already satisfied: requests>=2.26.0 in c:\programdata\anaconda3\lib\site-packages (from dlt[duckdb]) (2.31.0)
    Collecting requirements-parser>=0.5.0 (from dlt[duckdb])
      Downloading requirements_parser-0.5.0-py3-none-any.whl (18 kB)
    Collecting semver>=2.13.0 (from dlt[duckdb])
      Obtaining dependency information for semver>=2.13.0 from https://files.pythonhosted.org/packages/9a/77/0cc7a8a3bc7e53d07e8f47f147b92b0960e902b8254859f4aee5c4d7866b/semver-3.0.2-py3-none-any.whl.metadata
      Downloading semver-3.0.2-py3-none-any.whl.metadata (5.0 kB)
    Requirement already satisfied: setuptools>=65.6.0 in c:\programdata\anaconda3\lib\site-packages (from dlt[duckdb]) (68.0.0)
    Collecting simplejson>=3.17.5 (from dlt[duckdb])
      Obtaining dependency information for simplejson>=3.17.5 from https://files.pythonhosted.org/packages/b6/8e/3e12d122dfdf549a8d12eaf39954ee39f2027060aa38b63430f8ab3244e7/simplejson-3.19.2-cp311-cp311-win_amd64.whl.metadata
      Downloading simplejson-3.19.2-cp311-cp311-win_amd64.whl.metadata (3.2 kB)
    Requirement already satisfied: tenacity>=8.0.2 in c:\programdata\anaconda3\lib\site-packages (from dlt[duckdb]) (8.2.2)
    Collecting tomlkit>=0.11.3 (from dlt[duckdb])
      Obtaining dependency information for tomlkit>=0.11.3 from https://files.pythonhosted.org/packages/6e/43/159750d32481f16e34cc60090b53bc0a14314ad0c1f67a9bb64f3f3a0551/tomlkit-0.12.3-py3-none-any.whl.metadata
      Downloading tomlkit-0.12.3-py3-none-any.whl.metadata (2.7 kB)
    Requirement already satisfied: typing-extensions>=4.0.0 in c:\programdata\anaconda3\lib\site-packages (from dlt[duckdb]) (4.7.1)
    Requirement already satisfied: tzdata>=2022.1 in c:\programdata\anaconda3\lib\site-packages (from dlt[duckdb]) (2023.3)
    Collecting win-precise-time>=1.4.2 (from dlt[duckdb])
      Obtaining dependency information for win-precise-time>=1.4.2 from https://files.pythonhosted.org/packages/f9/9c/46d69220d468c82ca2044284c5a8089705c5eb66be416abcbba156365a14/win_precise_time-1.4.2-cp311-cp311-win_amd64.whl.metadata
      Downloading win_precise_time-1.4.2-cp311-cp311-win_amd64.whl.metadata (3.0 kB)
    Requirement already satisfied: wheel<1.0,>=0.23.0 in c:\programdata\anaconda3\lib\site-packages (from astunparse>=1.6.3->dlt[duckdb]) (0.38.4)
    Requirement already satisfied: six<2.0,>=1.6.1 in c:\programdata\anaconda3\lib\site-packages (from astunparse>=1.6.3->dlt[duckdb]) (1.16.0)
    Requirement already satisfied: colorama in c:\programdata\anaconda3\lib\site-packages (from click>=7.1->dlt[duckdb]) (0.4.6)
    Collecting gitdb<5,>=4.0.1 (from gitpython>=3.1.29->dlt[duckdb])
      Obtaining dependency information for gitdb<5,>=4.0.1 from https://files.pythonhosted.org/packages/fd/5b/8f0c4a5bb9fd491c277c21eff7ccae71b47d43c4446c9d0c6cff2fe8c2c4/gitdb-4.0.11-py3-none-any.whl.metadata
      Downloading gitdb-4.0.11-py3-none-any.whl.metadata (1.2 kB)
    Requirement already satisfied: ply in c:\programdata\anaconda3\lib\site-packages (from jsonpath-ng>=1.5.3->dlt[duckdb]) (3.11)
    Requirement already satisfied: python-dateutil<3.0,>=2.6 in c:\programdata\anaconda3\lib\site-packages (from pendulum>=2.1.2->dlt[duckdb]) (2.8.2)
    Requirement already satisfied: pytzdata>=2020.1 in c:\programdata\anaconda3\lib\site-packages (from pendulum>=2.1.2->dlt[duckdb]) (2020.1)
    Requirement already satisfied: charset-normalizer<4,>=2 in c:\programdata\anaconda3\lib\site-packages (from requests>=2.26.0->dlt[duckdb]) (2.0.4)
    Requirement already satisfied: idna<4,>=2.5 in c:\programdata\anaconda3\lib\site-packages (from requests>=2.26.0->dlt[duckdb]) (3.4)
    Requirement already satisfied: urllib3<3,>=1.21.1 in c:\programdata\anaconda3\lib\site-packages (from requests>=2.26.0->dlt[duckdb]) (1.26.16)
    Requirement already satisfied: certifi>=2017.4.17 in c:\programdata\anaconda3\lib\site-packages (from requests>=2.26.0->dlt[duckdb]) (2024.2.2)
    Collecting types-setuptools>=57.0.0 (from requirements-parser>=0.5.0->dlt[duckdb])
      Obtaining dependency information for types-setuptools>=57.0.0 from https://files.pythonhosted.org/packages/b0/9f/ea652206f791959f7f743a06ce491b10675c5f542d4bde7c415e8644b83a/types_setuptools-69.1.0.20240217-py3-none-any.whl.metadata
      Downloading types_setuptools-69.1.0.20240217-py3-none-any.whl.metadata (1.6 kB)
    Requirement already satisfied: greenlet!=0.4.17 in c:\programdata\anaconda3\lib\site-packages (from SQLAlchemy>=1.4.0->dlt[duckdb]) (2.0.1)
    Collecting smmap<6,>=3.0.1 (from gitdb<5,>=4.0.1->gitpython>=3.1.29->dlt[duckdb])
      Obtaining dependency information for smmap<6,>=3.0.1 from https://files.pythonhosted.org/packages/a7/a5/10f97f73544edcdef54409f1d839f6049a0d79df68adbc1ceb24d1aaca42/smmap-5.0.1-py3-none-any.whl.metadata
      Downloading smmap-5.0.1-py3-none-any.whl.metadata (4.3 kB)
    Downloading duckdb-0.9.2-cp311-cp311-win_amd64.whl (10.3 MB)
       ---------------------------------------- 0.0/10.3 MB ? eta -:--:--
       - -------------------------------------- 0.4/10.3 MB 12.9 MB/s eta 0:00:01
       --- ------------------------------------ 0.9/10.3 MB 11.4 MB/s eta 0:00:01
       ----- ---------------------------------- 1.5/10.3 MB 11.6 MB/s eta 0:00:01
       ------- -------------------------------- 2.0/10.3 MB 11.6 MB/s eta 0:00:01
       --------- ------------------------------ 2.5/10.3 MB 11.5 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------ --------------------------- 3.1/10.3 MB 11.8 MB/s eta 0:00:01
       ------------- -------------------------- 3.4/10.3 MB 3.1 MB/s eta 0:00:03
       --------------- ------------------------ 3.9/10.3 MB 3.4 MB/s eta 0:00:02
       ---------------- ----------------------- 4.3/10.3 MB 3.6 MB/s eta 0:00:02
       ------------------ --------------------- 4.8/10.3 MB 3.8 MB/s eta 0:00:02
       -------------------- ------------------- 5.3/10.3 MB 4.1 MB/s eta 0:00:02
       ---------------------- ----------------- 5.8/10.3 MB 4.3 MB/s eta 0:00:02
       ------------------------ --------------- 6.3/10.3 MB 4.5 MB/s eta 0:00:01
       --------------------------- ------------ 7.1/10.3 MB 4.9 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ---------------------------- ----------- 7.3/10.3 MB 5.0 MB/s eta 0:00:01
       ------------------------------ --------- 7.8/10.3 MB 2.7 MB/s eta 0:00:01
       --------------------------------- ------ 8.6/10.3 MB 2.9 MB/s eta 0:00:01
       ------------------------------------ --- 9.3/10.3 MB 3.1 MB/s eta 0:00:01
       -------------------------------------- - 10.0/10.3 MB 3.3 MB/s eta 0:00:01
       ---------------------------------------  10.3/10.3 MB 3.3 MB/s eta 0:00:01
       ---------------------------------------- 10.3/10.3 MB 3.3 MB/s eta 0:00:00
    Downloading GitPython-3.1.42-py3-none-any.whl (195 kB)
       ---------------------------------------- 0.0/195.4 kB ? eta -:--:--
       --------------------------------------- 195.4/195.4 kB 11.6 MB/s eta 0:00:00
    Downloading giturlparse-0.12.0-py2.py3-none-any.whl (15 kB)
    Downloading hexbytes-1.0.0-py3-none-any.whl (5.9 kB)
    Downloading humanize-4.9.0-py3-none-any.whl (126 kB)
       ---------------------------------------- 0.0/126.8 kB ? eta -:--:--
       ---------------------------------------- 126.8/126.8 kB 7.3 MB/s eta 0:00:00
    Downloading jsonpath_ng-1.6.1-py3-none-any.whl (29 kB)
    Downloading makefun-1.15.2-py2.py3-none-any.whl (22 kB)
    Downloading orjson-3.9.10-cp311-none-win_amd64.whl (135 kB)
       ---------------------------------------- 0.0/135.0 kB ? eta -:--:--
       ---------------------------------------- 135.0/135.0 kB ? eta 0:00:00
    Downloading pathvalidate-3.2.0-py3-none-any.whl (23 kB)
    Downloading semver-3.0.2-py3-none-any.whl (17 kB)
    Downloading simplejson-3.19.2-cp311-cp311-win_amd64.whl (75 kB)
       ---------------------------------------- 0.0/75.3 kB ? eta -:--:--
       ---------------------------------------- 75.3/75.3 kB 4.3 MB/s eta 0:00:00
    Downloading tomlkit-0.12.3-py3-none-any.whl (37 kB)
    Downloading win_precise_time-1.4.2-cp311-cp311-win_amd64.whl (14 kB)
    Downloading dlt-0.4.4-py3-none-any.whl (540 kB)
       ---------------------------------------- 0.0/540.7 kB ? eta -:--:--
       --------------------------------------- 540.7/540.7 kB 17.1 MB/s eta 0:00:00
    Downloading gitdb-4.0.11-py3-none-any.whl (62 kB)
       ---------------------------------------- 0.0/62.7 kB ? eta -:--:--
       ---------------------------------------- 62.7/62.7 kB 3.3 MB/s eta 0:00:00
    Downloading types_setuptools-69.1.0.20240217-py3-none-any.whl (51 kB)
       ---------------------------------------- 0.0/51.8 kB ? eta -:--:--
       ---------------------------------------- 51.8/51.8 kB ? eta 0:00:00
    Downloading smmap-5.0.1-py3-none-any.whl (24 kB)
    Installing collected packages: makefun, win-precise-time, types-setuptools, tomlkit, smmap, simplejson, semver, pathvalidate, orjson, jsonpath-ng, humanize, hexbytes, giturlparse, duckdb, astunparse, requirements-parser, gitdb, gitpython, dlt
      Attempting uninstall: tomlkit
        Found existing installation: tomlkit 0.11.1
        Uninstalling tomlkit-0.11.1:
          Successfully uninstalled tomlkit-0.11.1
    Successfully installed astunparse-1.6.3 dlt-0.4.4 duckdb-0.9.2 gitdb-4.0.11 gitpython-3.1.42 giturlparse-0.12.0 hexbytes-1.0.0 humanize-4.9.0 jsonpath-ng-1.6.1 makefun-1.15.2 orjson-3.9.10 pathvalidate-3.2.0 requirements-parser-0.5.0 semver-3.0.2 simplejson-3.19.2 smmap-5.0.1 tomlkit-0.12.3 types-setuptools-69.1.0.20240217 win-precise-time-1.4.2
    Note: you may need to restart the kernel to use updated packages.
    


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
