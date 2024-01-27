#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


pd.__version__


# In[6]:


df=pd.read_csv('yellow_tripdata_2021-01.csv',nrows=100)


# In[4]:


pwd


# In[7]:


df


# In[11]:


df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)


# In[20]:


from sqlalchemy import create_engine


# In[41]:


engine=create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[42]:


engine.connect()


# In[24]:


print(pd.io.sql.get_schema(df,name='yello_taxi_data',con=engine))


# In[46]:


df_iter=pd.read_csv('yellow_tripdata_2021-01.csv',iterator=True, chunksize=100000)


# In[47]:


df=next(df_iter)


# In[48]:


len(df)


# In[49]:


df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)


# In[51]:


df.head(n=1)


# In[52]:


df.head(n=0).to_sql(name='yello_taxi_data',con=engine,if_exists='replace')


# In[53]:


get_ipython().run_line_magic('time', "df.to_sql(name='yello_taxi_data',con=engine,if_exists='append')")


# In[33]:


from time import time


# In[54]:


while True:
    t_start=time()
    df=next(df_iter)
    
    df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)
    
    df.to_sql(name='yello_taxi_data',con=engine,if_exists='append')
    
    t_end=time()
    
    print('inserted another chunk......, took %.3f second' % (t_end-t_start))


# In[55]:


query = """
select * from pg_catalog.pg_tables
where schemaname != 'pg_catalog' and
schemaname != 'information_schema';
"""

pd.read_sql(query,con=engine)


# In[56]:


query = """
select max(tpep_pickup_datetime), min(tpep_pickup_datetime), max(total_amount) from public.yello_taxi_data limit 100;
"""

pd.read_sql(query,con=engine)


# In[ ]:




