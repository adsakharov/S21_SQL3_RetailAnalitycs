CREATE MATERIALIZED VIEW Customers AS
    WITH Customer_Segments AS (
        WITH Customer_Features AS (
            SELECT
                Customer_ID,
                AVG(Transaction_Summ) AS Customer_Average_Check,
                EXTRACT(EPOCH FROM (MAX(Transactions.Transaction_DateTime) - MIN(Transactions.Transaction_DateTime))) / 86400 / COUNT(DISTINCT Transactions.Transaction_DateTime) AS Customer_Frequency,
                EXTRACT(EPOCH FROM analysis_formation - MAX(Transactions.Transaction_DateTime)) / 86400 AS Customer_Inactive_Period,
                (EXTRACT(EPOCH FROM analysis_formation - MAX(Transactions.Transaction_DateTime)) / 86400) / (EXTRACT(EPOCH FROM (MAX(Transactions.Transaction_DateTime) - MIN(Transactions.Transaction_DateTime))) / 86400 / COUNT(DISTINCT Transactions.Transaction_DateTime))  AS Customer_Churn_Rate
            FROM
                Cards
                JOIN Transactions
                    ON Transactions.Customer_Card_ID = Cards.Customer_Card_ID
                CROSS JOIN date_formation_analysis
            GROUP BY
                Customer_ID,
                analysis_formation
        ), Customer_Favourite_Store AS (
            WITH Transactions_Ordered AS (
                SELECT
                    ROW_NUMBER() OVER(PARTITION BY Customer_ID ORDER BY Transaction_DateTime DESC) as r,
                    Customer_ID,
                    Transaction_Store_ID
                FROM
                    Cards
                    JOIN Transactions
                        ON Transactions.Customer_Card_ID = Cards.Customer_Card_ID
            )
            SELECT
                Customer_ID, 
                Transaction_Store_ID
            FROM
                Transactions_Ordered
            WHERE r <=3
        ), Store_Share AS (
            WITH Total_Transactions AS (
                SELECT
                Customer_ID,
                COUNT(DISTINCT Transaction_ID) AS Total_Transactions
                FROM
                    Cards
                    JOIN Transactions
                        ON Cards.Customer_Card_ID = Transactions.Customer_Card_ID
                GROUP BY
                    Customer_ID
            )
            SELECT
                ROW_NUMBER() OVER(PARTITION BY Cards.Customer_ID ORDER BY CAST(COUNT(DISTINCT Transactions.Transaction_ID) AS FLOAT) / Total_Transactions DESC, MAX(Transaction_DateTime) DESC) as r,
                Cards.Customer_ID,
                Transactions.Transaction_Store_ID
            FROM
                Cards
                JOIN Transactions
                    ON Cards.Customer_Card_ID = Transactions.Customer_Card_ID
                JOIN Total_Transactions
                    ON Total_Transactions.Customer_ID = Cards.Customer_ID
            GROUP BY
                Cards.Customer_ID,
                Transactions.Transaction_Store_ID,
                Total_Transactions.Total_Transactions
        )
        SELECT
            Customer_Features.Customer_ID,
            Customer_Average_Check,
            CASE
                WHEN AVG(Customer_Favourite_Store.Transaction_Store_ID) = MAX(Customer_Favourite_Store.Transaction_Store_ID) THEN MAX(Customer_Favourite_Store.Transaction_Store_ID)
                ELSE Store_Share.Transaction_Store_ID
            END AS A,
            CASE
                WHEN PERCENT_RANK() OVER(ORDER BY Customer_Average_Check) >= 0.9 THEN 'High'
                WHEN PERCENT_RANK() OVER(ORDER BY Customer_Average_Check) >= 0.65 THEN 'Medium'
                ELSE 'Low'
            END AS Customer_Average_Check_Segment,
            Customer_Frequency,
            CASE
                WHEN PERCENT_RANK() OVER(ORDER BY Customer_Frequency DESC) >= 0.9 THEN 'Often'
                WHEN PERCENT_RANK() OVER(ORDER BY Customer_Frequency DESC) >= 0.65 THEN 'Occasionally'
                ELSE 'Rarely'
            END AS Customer_Frequency_Segment,
            Customer_Inactive_Period,
            Customer_Churn_Rate,
            CASE
                WHEN Customer_Churn_Rate >= 0 AND Customer_Churn_Rate < 2 THEN 'Low'
                WHEN Customer_Churn_Rate >= 2 AND Customer_Churn_Rate < 5 THEN 'Medium'
                WHEN Customer_Churn_Rate >= 5 THEN 'High'
            END AS Customer_Churn_Segment
        FROM
            Customer_Features
            JOIN Customer_Favourite_Store
                ON Customer_Features.Customer_ID = Customer_Favourite_Store.Customer_ID
            JOIN Store_Share
                ON Customer_Features.Customer_ID = Store_Share.Customer_ID
                AND Store_Share.r = 1
        GROUP BY
            Customer_Features.Customer_ID,
            customer_features.customer_average_check,
            customer_features.customer_frequency, 
            customer_features.customer_inactive_period,
            customer_features.customer_churn_rate,
            Store_Share.Transaction_Store_ID
    )
    SELECT
        Customer_ID,
        Customer_Average_Check,
        Customer_Average_Check_Segment,
        Customer_Frequency,
        Customer_Frequency_Segment,
        Customer_Inactive_Period,
        Customer_Churn_Rate,
        Customer_Churn_Segment,
        CASE
            WHEN Customer_Average_Check_Segment = 'Low' AND Customer_Frequency_Segment = 'Rarely' AND Customer_Churn_Segment = 'Low' THEN 1
            WHEN Customer_Average_Check_Segment = 'Low' AND Customer_Frequency_Segment = 'Rarely' AND Customer_Churn_Segment = 'Medium' THEN 2
            WHEN Customer_Average_Check_Segment = 'Low' AND Customer_Frequency_Segment = 'Rarely' AND Customer_Churn_Segment = 'High' THEN 3
            WHEN Customer_Average_Check_Segment = 'Low' AND Customer_Frequency_Segment = 'Occasionally' AND Customer_Churn_Segment = 'Low' THEN 4
            WHEN Customer_Average_Check_Segment = 'Low' AND Customer_Frequency_Segment = 'Occasionally' AND Customer_Churn_Segment = 'Medium' THEN 5
            WHEN Customer_Average_Check_Segment = 'Low' AND Customer_Frequency_Segment = 'Occasionally' AND Customer_Churn_Segment = 'High' THEN 6
            WHEN Customer_Average_Check_Segment = 'Low' AND Customer_Frequency_Segment = 'Often' AND Customer_Churn_Segment = 'Low' THEN 7
            WHEN Customer_Average_Check_Segment = 'Low' AND Customer_Frequency_Segment = 'Often' AND Customer_Churn_Segment = 'Medium' THEN 8
            WHEN Customer_Average_Check_Segment = 'Low' AND Customer_Frequency_Segment = 'Often' AND Customer_Churn_Segment = 'High' THEN 9
            WHEN Customer_Average_Check_Segment = 'Medium' AND Customer_Frequency_Segment = 'Rarely' AND Customer_Churn_Segment = 'Low' THEN 10
            WHEN Customer_Average_Check_Segment = 'Medium' AND Customer_Frequency_Segment = 'Rarely' AND Customer_Churn_Segment = 'Medium' THEN 11
            WHEN Customer_Average_Check_Segment = 'Medium' AND Customer_Frequency_Segment = 'Rarely' AND Customer_Churn_Segment = 'High' THEN 12
            WHEN Customer_Average_Check_Segment = 'Medium' AND Customer_Frequency_Segment = 'Occasionally' AND Customer_Churn_Segment = 'Low' THEN 13
            WHEN Customer_Average_Check_Segment = 'Medium' AND Customer_Frequency_Segment = 'Occasionally' AND Customer_Churn_Segment = 'Medium' THEN 14
            WHEN Customer_Average_Check_Segment = 'Medium' AND Customer_Frequency_Segment = 'Occasionally' AND Customer_Churn_Segment = 'High' THEN 15
            WHEN Customer_Average_Check_Segment = 'Medium' AND Customer_Frequency_Segment = 'Often' AND Customer_Churn_Segment = 'Low' THEN 16
            WHEN Customer_Average_Check_Segment = 'Medium' AND Customer_Frequency_Segment = 'Often' AND Customer_Churn_Segment = 'Medium' THEN 17
            WHEN Customer_Average_Check_Segment = 'Medium' AND Customer_Frequency_Segment = 'Often' AND Customer_Churn_Segment = 'High' THEN 18
            WHEN Customer_Average_Check_Segment = 'High' AND Customer_Frequency_Segment = 'Rarely' AND Customer_Churn_Segment = 'Low' THEN 19
            WHEN Customer_Average_Check_Segment = 'High' AND Customer_Frequency_Segment = 'Rarely' AND Customer_Churn_Segment = 'Medium' THEN 20
            WHEN Customer_Average_Check_Segment = 'High' AND Customer_Frequency_Segment = 'Rarely' AND Customer_Churn_Segment = 'High' THEN 21
            WHEN Customer_Average_Check_Segment = 'High' AND Customer_Frequency_Segment = 'Occasionally' AND Customer_Churn_Segment = 'Low' THEN 22
            WHEN Customer_Average_Check_Segment = 'High' AND Customer_Frequency_Segment = 'Occasionally' AND Customer_Churn_Segment = 'Medium' THEN 23
            WHEN Customer_Average_Check_Segment = 'High' AND Customer_Frequency_Segment = 'Occasionally' AND Customer_Churn_Segment = 'High' THEN 24
            WHEN Customer_Average_Check_Segment = 'High' AND Customer_Frequency_Segment = 'Often' AND Customer_Churn_Segment = 'Low' THEN 25
            WHEN Customer_Average_Check_Segment = 'High' AND Customer_Frequency_Segment = 'Often' AND Customer_Churn_Segment = 'Medium' THEN 26
            WHEN Customer_Average_Check_Segment = 'High' AND Customer_Frequency_Segment = 'Often' AND Customer_Churn_Segment = 'High' THEN 27
        END AS Customer_Segment,
        A AS Customer_Primary_Store
    FROM
        Customer_Segments;