#!/usr/bin/env python
# coding: utf-8

# In[1]:


# import libraries
import pandas as pd
import numpy as np
from pymysql import connect
from faker import Faker
from datetime import date
from sqlalchemy import create_engine
import warnings


# In[2]:


# initializing the faker library
fake=Faker()


# In[3]:


# defining number of rows
users=500
transactions=10000


# In[4]:


# creating the user table
np.random.seed(42)
fake.seed_instance(42)
user=pd.DataFrame({"user_id":np.arange(1,users+1),
                  "first_name":[fake.first_name() for _ in range(users)],
                  "last_name":[fake.last_name() for _ in range(users)],
                  "email":[fake.email() for _ in range(users)],
                  "phone_number":[fake.numerify("+44 #### ### ###") for _ in range(users)],
                  "age":np.random.randint(18,65, users)})
user


# In[5]:


warnings.filterwarnings('ignore')
user['email'][0]='_pollard_michael_example.net'
user['email'][24]='stephanie71example.org'
user['email'][276]='jfaulkner-@example.net'
user['email'][29]='caroline51#@example.net'
user['email'][55]='.christopher13@example.com'
user['email'][98]='mariah.davis@example.org.'
user['email'][113]='myerstheodore@example_net'
user['email'][167]='5barkereric@example.com'


# In[6]:


# checking for null values in user table
user.isnull().sum()


# In[7]:


# checking for datatypes
user.dtypes


# In[8]:


# checking for duplicates
user[user.duplicated]


# In[9]:


# creating the account table
account_num=800
np.random.seed(22)
fake.seed_instance(22)
account=pd.DataFrame({"account_id":np.arange(1,account_num+1),
                     "user_id":np.random.choice(user['user_id'],account_num),
                     "account_balance":np.round(np.random.uniform(500,1000, account_num),2),
                     "account_type":np.random.choice(['checking','savings','loan'], account_num),
                     "signup_date":[fake.date_between(start_date='-2y', end_date='-15d') for _ in range(account_num)],
                     "account_status":np.random.choice(['Active','Inactive','Suspended'], account_num)})


# In[10]:


account['activity_status']=np.where(account['account_status']=='Active', date.today(), 
                                   [fake.date_between(start_date=signup, end_date='-1m') for signup in account['signup_date']])
account=account.drop_duplicates(subset=['user_id','account_type']).reset_index(drop=True)


# In[11]:


account


# In[12]:


# checking for duplicates
account[account.duplicated()]


# In[13]:


# checking datatypes
account.dtypes


# In[14]:


# converting datatypes
account[['signup_date', 'activity_status']]=account[['signup_date', 'activity_status']].astype('datetime64[ns]')


# In[15]:


# confirming datatypes
account.dtypes


# In[16]:


# checking for null values
account.isnull().sum()


# In[17]:


# creating the transaction table
warnings.filterwarnings('ignore')
np.random.seed(42)
fake.seed_instance(42)
sign_date = account.set_index('user_id')['signup_date'].to_dict()
account['signup_date'] = pd.to_datetime(account['signup_date'])
account['activity_status'] = pd.to_datetime(account['activity_status'])
transaction=pd.DataFrame({"transaction_id":np.arange(1,transactions+1),
                          "account_id": np.random.choice(account["account_id"], size=transactions, replace=True),
                         "amount":np.round(np.random.uniform(10,1000, size=transactions), 2),
                         "transaction_type":np.random.choice(["Deposit","Withdrawal","Transfer","Online payment"], transactions),
                         "transaction_status":np.random.choice(["Successful","Failed"], transactions)})
transaction = transaction.merge(account[['account_id', 'user_id', 'account_type', 'signup_date', 'activity_status']], 
                                on='account_id', how='left')

transaction['transaction_date'] = transaction.apply(lambda row: fake.date_time_between(
    start_date=row['signup_date'], end_date=row['activity_status']
), axis=1)


# In[18]:


transaction


# In[19]:


transaction=transaction[['transaction_id','user_id','account_id','amount','transaction_type','transaction_status','account_type','transaction_date']]


# In[20]:


transaction


# In[21]:


# checking for null values
transaction.isnull().sum()


# In[22]:


# checking datatypes
transaction.dtypes


# In[23]:


# checking duplicates
transaction[transaction.duplicated]


# In[24]:


# creating fraud table
np.random.seed(42)
Faker.seed(22)
samples= transaction.sample(n=1000)
fraud=pd.DataFrame({"fraud_id": np.arange(1,1000+1),
                   "user_id":samples['user_id'].values,
                   "transaction_id":samples['transaction_id'].values,
                   "alert_date":samples['transaction_date'].values,
                   "alert_reason":np.where(samples['transaction_status']=='Successful', 'Suspicious activity', np.random.choice(['Insufficient balance', 'Exceeded daily limits'], size=1000))})
fraud


# In[25]:


fraud['alert_reason'].value_counts()


# In[26]:


# checking for null values
fraud.isnull().sum()


# In[27]:


# checking for duplicates
fraud[fraud.duplicated()]


# In[28]:


# checking for datatypes
fraud.dtypes


# In[29]:


# connecting to sql server
project=connect(user='root',host='localhost',password='Adedolapo11@')
cursor=project.cursor()


# In[30]:


# creating a database
cursor.execute('DROP DATABASE IF EXISTS portfolio')
cursor.execute('CREATE DATABASE portfolio')
project.commit()


# In[31]:


connection=connect(user='root', host='localhost', password='Adedolapo11@', database='portfolio')
engine=create_engine('mysql+pymysql://root:Adedolapo11%40@localhost/portfolio')


# In[32]:


# exporting data to database
account.to_sql(name='account', con=engine, if_exists='replace', index=False)


# In[33]:


# exporting data to database
user.to_sql(name='user', con=engine, if_exists='replace', index=False)
transaction.to_sql(name='transaction', con=engine, if_exists='replace', index=False)
fraud.to_sql(name='fraud', con=engine, if_exists='replace', index=False)


# In[ ]:




