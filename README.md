**Analytical Exploration of Fintech Transactions: Automation, Advanced Analytics and Fraud Detection.**
---
**CASE STUDY** 

A financial technology company aims to continually monitor user retention, engagements and activities to gain deeper insights into user behavior, enabling better strategic decisions across departments. For instance, 
this analysis allows the marketing team to identify users who may need re-engagement and target them effectively. By analyzing user behavior, such as transaction patterns and security-related events, the company can also 
uncover potential red flags or fraud like transactions. Achieving these insights relies heavily on data-driven analysis.

For this project, I created synthetic datasets using Python's NumPy and Faker libraries, generating realistic data to address key business questions i developed. Through this approach, 
I aim to demonstrate how data can guide effective decision-making and support customer-centric strategies.

**TOOLS USED**
---
- Jupyter notebook
- MySQL

**DATA STRUCTURE**
---
There are 4 tables, each containing a 2years transaction records. These datasets collectively provide comprehensive information on various aspects of user data, such as:

- User table: This table contains 6 columns and 500 rows. It tells personal information about the user.
- Account table: The table contains 7 columns and 613 rows. Each user in the User table has at least one account type (savings, loan, checking) and at most three account types. It provides essential information about each account.
- Transaction table: It contains 8 columns and 10,000 rows. The table gives a detailed information on each transaction that has taken place throughout the company's existence.
- Fraud table: Includes 5 columns and 1000 rows. This table logs transaction records flagged as having potential indicators of fraud.

  After creating the datasets, I ensured there were no duplicates, verified that columns had appropriate data types, and confirmed there were no null values, preparing the tables for querying in MySQL.
  I then connected the MySQL server to Jupyter Notebook, created a database, and imported the tables into the database.
  
**ANSWERING BUSINESS QUESTIONS**
---
  I curated a list of advanced business questions, along with the corresponding queries, including the use of triggers and stored procedures, aimed at supporting the company's goal of effectively monitoring user retention,
  engagement, and activity levels. These analytical questions provide actionable insights and use of complex SQL techniques to help the company make data-driven decisions, ultimately enhancing their ability to understand and meet
  customer needs.

I provided a detailed explanation of the business impact of these analyses on both the company and the users. Read here: https://medium.com/@abowabat/analytical-exploration-of-fintech-transactions-automation-advanced-analytics-and-fraud-detection-c0bcccec885f


Connect with me on LinkedIn - www.linkedin.com/in/taibat-abowaba


