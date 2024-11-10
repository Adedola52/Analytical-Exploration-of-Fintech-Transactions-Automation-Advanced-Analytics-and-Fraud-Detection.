USE portfolio;
SELECT * FROM account;
SELECT * FROM fraud;
SELECT * FROM transaction;
SELECT * FROM user;
DESCRIBE account;
DESCRIBE fraud;
DESCRIBE transaction;
DESCRIBE user;

-- 1. Write a query to identify users who have invalid emails (emails not following the proper format).
 SELECT * FROM user WHERE email NOT IN
 (SELECT email FROM user 
  WHERE email REGEXP '^[A-Za-z][A-Za-z0-9.]*@example\\.com$' OR
  email REGEXP '^[A-Za-z][A-Za-z0-9.]*@example\\.net$' 
  OR email REGEXP '[A-Za-z][A-Za-z0-9.]*@example\\.org$');
  
  -- 2. Write a query to display all users who have made more than 5 transactions 
-- within the last 6 months. For each user, also display the total transaction amount within that period
WITH cte as (SELECT u.user_id, a.account_id, u.first_name, u.last_name, 
	 round(sum(t.amount), 2) total_transaction,
     count(t.account_id) users FROM user u 
     JOIN transaction t ON u.user_id=t.user_id
     JOIN account a on a.account_id=t.account_id
     WHERE transaction_date>= date_sub((SELECT max(transaction_date) from transaction), interval 6 month)
     GROUP BY 1,2,3,4
     HAVING users>5)
SELECT cte.account_id, cte.user_id, cte.first_name, cte.last_name, total_transaction FROM cte;

-- 3.  Analyze Transaction Trends Over Time. For each user, calculate the month-over-month growth in 
-- transaction value for the past 6 months. Return the user IDs along with their average % growth rate.
WITH cte AS (SELECT a.account_id, month(t.transaction_date) months, sum(amount) amounts
		 FROM account a JOIN transaction t ON 
		 a.account_id=t.user_id 
         WHERE date(transaction_date) >= date_sub((SELECT max(transaction_date) from transaction),
		 interval 6 month)
         GROUP BY 1,2
         ORDER BY 1, 2),
cte2 AS (SELECT cte.account_id, cte.months, cte.amounts, CASE WHEN LAG(cte.amounts) 
		 OVER(PARTITION BY cte.account_id
         ORDER BY cte.months) = 0
         OR LAG(cte.amounts) OVER(PARTITION BY cte.account_id ORDER BY cte.months) IS NULL
         THEN 100 ELSE ((cte.amounts - LAG(cte.amounts) 
         OVER(PARTITION BY cte.account_id ORDER BY  cte.months)) / LAG(cte.amounts) 
         OVER(PARTITION BY cte.account_id ORDER BY cte.months) * 100) END AS months_over FROM cte)
SELECT cte2.account_id, concat(round(AVG(months_over),2), '%') AS average_growth FROM cte2 
JOIN cte on cte.account_id=cte2.account_id
GROUP BY 1;

-- 4. Write a query to identify users whose number of transactions dropped by at least 50% 
-- between any two consecutive months.
WITH cte AS(SELECT user_id, month(transaction_date) AS cur_month, count(user_id) AS cur_tran FROM transaction
     WHERE month(transaction_date) = (SELECT max(month(transaction_date)) 
     FROM transaction WHERE year(transaction_date)=2024)
     GROUP BY 1,2),
cte2 AS (SELECT user_id, month(transaction_date) AS pre_month, count(user_id) AS pre_tran
     FROM transaction WHERE month(transaction_date) =  
     (SELECT max(month(transaction_date)) FROM transaction 
     WHERE month(transaction_date) < (SELECT max(month(transaction_date)) FROM transaction
     WHERE year(transaction_date)=2024))
     GROUP BY 1,2)
SELECT cte.user_id, cte.cur_month, cte.cur_tran, cte2.pre_month, cte2.pre_tran 
FROM cte JOIN cte2 ON cte.user_id=cte2.user_id
WHERE pre_tran>=cur_tran * 1.5;
  
-- 5. Retrieve all users who signed up in the last 6 months
SELECT u.user_id, u.first_name, u.last_name, u.age, a.account_type, a.signup_date 
FROM user u JOIN account a ON 
u.user_id=a.user_id 
WHERE a.signup_date >= date_sub((SELECT max(transaction_date) from transaction), interval 6 month);

-- 6. Find all users who have been inactive for more than a year
SELECT u.user_id, a.account_id, u.first_name, u.last_name, a.account_status, a.activity_status 
FROM user u JOIN account a ON u.user_id=a.user_id 
WHERE a.account_status='Inactive' AND 
a.activity_status<= date_add((SELECT max(transaction_date) from transaction), interval -1 year);

-- 7. Write a query to identify users who made more than multiple transactions within a 10-minute window.
WITH cte AS (SELECT u.user_id, u.first_name, u.last_name, t.transaction_date,
			 t.transaction_status, t.account_type FROM user u
	         JOIN transaction t ON u.user_id=t.user_id
			 ORDER BY 1)
SELECT cte.user_id, cte.first_name, cte.transaction_date, cte.transaction_status, 
cte.account_type FROM cte JOIN cte cte2 ON 
cte.user_id=cte2.user_id
WHERE cte.account_type =cte2.account_type
AND date(cte.transaction_date) = date(cte2.transaction_date)
AND hour(cte.transaction_date) = hour(cte2.transaction_date)
AND ABS(TIMESTAMPDIFF(MINUTE, cte.transaction_date, cte2.transaction_date)) <= 10
AND cte.transaction_date != cte2.transaction_date
ORDER BY 1;

-- 8. Calculate the correlation between the number of transactions and the number of 
-- fraud alerts.
WITH corr AS (WITH cte AS (SELECT t.user_id, count(t.user_id) AS count_t, avg(count(t.user_id)) 
              OVER() AS avg_t FROM transaction t
              GROUP BY 1
			  ORDER BY 1),
cte2 AS (SELECT f.user_id, count(f.fraud_id) AS count_f, avg(count(f.user_id)) 
              OVER() AS avg_f FROM fraud f 
              GROUP BY 1
              ORDER BY 1)
SELECT cte.user_id, cte.count_t, cte.avg_t, cte2.count_f, 
cte2.avg_f FROM cte JOIN cte2 ON cte.user_id=cte2.user_id)
SELECT round(sum((corr.count_t-corr.avg_t) * 
(corr.count_f-corr.avg_f))/sqrt(sum(power(corr.count_t-corr.avg_t,2)) * 
sum(power(corr.count_f-corr.avg_f,2))),3) AS correlation_coefficient FROM corr;

-- 9. Write a query to rank users based on their total transaction value in the past 3 months. 
SELECT u.user_id, a.account_id, u.first_name, u.last_name, t.account_type, round(sum(t.amount),2) sums,
dense_rank() OVER(ORDER BY sum(t.amount)) ranks FROM user u JOIN transaction t ON
u.user_id = t.user_id
JOIN account a ON a.account_id = t.account_id
WHERE date(t.transaction_date)>= date_sub((SELECT max(date(transaction_date)) 
FROM transaction), interval 3 month)
GROUP BY 1,2,3,4,5
ORDER BY 7 DESC;

-- 10. Write a trigger that checks for inactive users with a balance greater than $500 
-- and flags them for reactivation. Create a table and log users.
CREATE TABLE reactivation (
user_id int,
first_name varchar (100),
last_name varchar(100),
account_balance decimal(10,2),
email varchar(100),
phone_number varchar(100));
DROP TRIGGER IF EXISTS after_update_activity;
DELIMITER $$
CREATE TRIGGER after_update_activity
AFTER UPDATE ON account
FOR EACH ROW
BEGIN
DECLARE first_name varchar (100);
DECLARE last_name varchar (100);
DECLARE email varchar (100);
DECLARE phone_number varchar (100);
IF NEW.account_status ="Inactive" AND OLD.account_balance>= 500 THEN 
SELECT u.first_name, u.last_name, u.email, u.phone_number 
INTO first_name, last_name, email, phone_number FROM user u 
WHERE u.user_id = OLD.user_id;
INSERT INTO reactivation (user_id, first_name, last_name, account_balance, email, phone_number) 
VALUES (OLD.user_id, first_name, last_name, OLD.account_balance, email, phone_number);
END IF;
END $$
DELIMITER ;
SHOW triggers;
UPDATE account
SET account_status = "Inactive"
WHERE account_id=152;
SELECT * FROM reactivation;

-- 11. Create a stored procedure that generates a transaction report for a given user ID within a specific date range, 
-- Procedure should also generate the transaction count, total transaction amount, 
-- and breakdown by transaction type (Deposit, Withdrawal, etc.).
DROP PROCEDURE IF EXISTS monthly_report;
DELIMITER $$
CREATE PROCEDURE monthly_report(IN user_ids int, IN tran_start date,IN tran_end date, IN acc_id int)
BEGIN 
    SELECT t.amount, t.transaction_type, t.account_type, t.transaction_date FROM transaction t
    WHERE date(t.transaction_date) BETWEEN tran_start AND tran_end
    AND t.user_id = user_ids
    AND t.transaction_status = 'Successful'
    AND t.account_id = acc_id;
    SELECT t.transaction_type, count(t.account_id) AS transaction_count, 
    sum(t.amount) AS transaction_amount FROM 
    TRANSACTION t 
    WHERE date(t.transaction_date) BETWEEN tran_start AND tran_end
    AND t.user_id = user_ids
    AND t.account_id =  acc_id 
    AND t.transaction_status ='Successful'
    GROUP BY t.transaction_type;
END $$
DELIMITER ;
CALL monthly_report( 180, '2024-09-01', '2024-09-30', 128);

-- 12. Create a trigger that automatically flags a transaction as 'Suspicious activity' if the transaction amount 
-- exceeds the user's average transaction in the last 30 days. Trigger fails a transaction if the
-- transaction amount is above the user's balance, else if the amount is below or equals the balance,
-- transaction should be successful.
DROP TRIGGER IF EXISTS before_insert_transaction;
DELIMITER $$
CREATE TRIGGER before_insert_transaction
BEFORE INSERT ON transaction
FOR EACH ROW 
BEGIN
DECLARE account_balance double;
DECLARE fraud_id int;
SELECT a.account_balance
INTO account_balance
FROM account a 
WHERE a.account_id= NEW.account_id;
SELECT coalesce(max(f.fraud_id), 0) + 1
INTO fraud_id from fraud f;
IF NEW.amount > account_balance 
THEN SET NEW.transaction_status = "Failed";
ELSE SET NEW.transaction_status = "Successful";
END IF; 
IF NEW.amount > (select avg(amount) from transaction where user_id=NEW.user_id 
and date(transaction_date) >= date_sub(NEW.transaction_date, interval 30 day))THEN 
INSERT INTO fraud (fraud_id, user_id, transaction_id, alert_date, alert_reason)
VALUES (fraud_id, NEW.user_id, NEW.transaction_id, NEW.transaction_date, 'Suspicious activity');
END IF; 
END $$
DELIMITER ; 

INSERT INTO transaction 
VALUES (10001, 180, 128, 950.50, 'Transfer', NULL, 'loan', '2024-9-15');
SELECT * FROM transaction
WHERE transaction_id = 10001;
SELECT * FROM fraud;