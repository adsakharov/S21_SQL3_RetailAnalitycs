CREATE MATERIALIZED VIEW Groups AS
    WITH Transaction_Info AS (
            SELECT
                EXTRACT(EPOCH from Transaction_DateTime) / 86400 AS Transaction_DateTime,
                Group_ID,
                Customer_ID,
                ROW_NUMBER() OVER () as r
            FROM (
                SELECT
                    Transaction_DateTime,
                    Customer_ID,
                    Group_ID
                FROM
                    Purchase_History
                ORDER BY 2, 3, 1
                )
            AS A),
    Discount_Info AS (
    SELECT
        Cards.Customer_ID,
        product_matrix.Group_id,
        COUNT(Transactions.transaction_id) AS Group_Purchase,
        SUM(Group_Summ_Paid) / SUM(Group_Summ) AS Group_Average_Discount
    FROM
        Transactions
        JOIN Cards
            ON Transactions.Customer_Card_ID = Cards.Customer_Card_ID
        JOIN Checks
            ON Transactions.Transaction_ID = Checks.Transaction_ID
        JOIN product_matrix
            ON product_matrix.SKU_ID = Checks.SKU_ID
        JOIN trade_points
            ON trade_points.SKU_ID = product_matrix.SKU_ID
            AND trade_points.Transaction_Store_ID = transactions.Transaction_Store_ID
        JOIN Purchase_History
            ON Transactions.Transaction_ID = Purchase_History.Transaction_ID
            AND Purchase_History.Customer_ID = Cards.Customer_ID
    WHERE Checks.SKU_Discount > 0
    GROUP BY
        Cards.Customer_ID,
        product_matrix.Group_id
    )
    SELECT
        Purchase_History.Customer_ID,
        Purchase_History.Group_ID,
        CAST(Periods.Group_Purchase AS FLOAT) / CAST(COUNT(DISTINCT Transactions.Transaction_ID) AS FLOAT) 
            AS Group_Affinity_Index,
        EXTRACT(EPOCH from (Analysis_Formation - MAX(Purchase_history.Transaction_DateTime))) / 86400 / Group_Frequency
            AS Group_Churn_Rate,
        COALESCE(AVG(Group_Stability_Rate), 1)
            AS Group_Stability_Rate,
        AVG(Group_Summ_Paid - Group_Cost) * COUNT(DISTINCT Group_Summ_Paid)
            AS Group_Margin,
        COALESCE(Discount_Info.Group_Purchase / CAST(Periods.Group_Purchase AS FLOAT), 0)
            AS Group_Discount_Share,
        Group_Min_Discount,
        AVG(Group_Average_Discount)
            AS Group_Average_Discount
    FROM
        Cards
        JOIN Transactions
            ON Transactions.Customer_Card_ID = Cards.Customer_Card_ID
        JOIN Checks
            ON Checks.Transaction_ID = Transactions.Transaction_ID
        JOIN Product_matrix
            ON Product_matrix.SKU_ID = Checks.SKU_ID
        JOIN trade_points
            ON trade_points.Transaction_Store_ID = Transactions.Transaction_Store_ID
            AND trade_points.SKU_ID = Product_matrix.SKU_ID
        JOIN Periods
            ON Cards.Customer_ID = Periods.Customer_ID
        JOIN Purchase_History
            ON Cards.Customer_ID = Purchase_History.Customer_ID
            AND Periods.Group_ID = Purchase_History.Group_ID
        FULL JOIN (
            SELECT ABS(Group_Frequency - (B.Transaction_DateTime - A.Transaction_DateTime)) / Group_Frequency AS Group_Stability_Rate,
                A.Group_ID AS Group_ID,
                A.Customer_ID AS Customer_ID
            FROM Transaction_Info AS A
            JOIN Transaction_Info AS B
            ON A.r + 1 = B.r AND A.Group_ID = B.Group_ID AND A.Customer_ID = B.Customer_ID
            JOIN Periods ON A.Customer_ID = Periods.Customer_ID AND A.Group_ID = Periods.Group_ID
        ) AS A
            ON A.Group_ID = Purchase_History.Group_ID
            AND A.Customer_ID = Purchase_History.Customer_ID
        FULL JOIN Discount_Info
            ON Discount_Info.Customer_ID = Cards.Customer_ID
            AND Discount_Info.Group_ID = Purchase_History.Group_ID
        CROSS JOIN Date_Formation_Analysis
    WHERE
        Transactions.Transaction_DateTime >= Periods.First_Group_Purchase_Date
        AND Transactions.Transaction_DateTime <= Periods.Last_Group_Purchase_Date
    GROUP BY
        Purchase_History.Customer_ID,
        Purchase_History.Group_ID,
        Periods.Group_Purchase,
        Discount_Info.Group_Purchase,
        Group_Frequency,
        Analysis_Formation,
        Group_Min_Discount;


SELECT
    Purchase_History.Customer_ID,
    AVG(Group_Summ_Paid - Group_Cost) * COUNT(DISTINCT Group_Summ_Paid)
        AS Group_Margin
FROM
    Cards
    JOIN Transactions
        ON Transactions.Customer_Card_ID = Cards.Customer_Card_ID
    JOIN Checks
        ON Checks.Transaction_ID = Transactions.Transaction_ID
    JOIN Product_matrix
        ON Product_matrix.SKU_ID = Checks.SKU_ID
    JOIN trade_points
        ON trade_points.Transaction_Store_ID = Transactions.Transaction_Store_ID
        AND trade_points.SKU_ID = Product_matrix.SKU_ID
    JOIN Periods
        ON Cards.Customer_ID = Periods.Customer_ID
    JOIN Purchase_History
        ON Cards.Customer_ID = Purchase_History.Customer_ID
        AND Periods.Group_ID = Purchase_History.Group_ID
    CROSS JOIN Date_Formation_Analysis
WHERE
    Transactions.Transaction_DateTime >= Analysis_Formation - INTERVAL '100 days'
    AND Transactions.Transaction_DateTime <= Periods.Last_Group_Purchase_Date
GROUP BY
    Purchase_History.Customer_ID,
    Purchase_History.Group_ID,
    Periods.Group_Purchase,
    Group_Frequency,
    Analysis_Formation,
    Group_Min_Discount;

SELECT
    Purchase_History.Customer_ID,
    AVG(Group_Summ_Paid - Group_Cost) * COUNT(DISTINCT Group_Summ_Paid)
        AS Group_Margin
FROM
    Cards
    JOIN (
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY Customer_ID ORDER BY Transaction_DateTime DESC) AS Tr_Count,
            Transactions.Customer_Card_ID,
            Transactions.Transaction_ID,
            Transactions.Transaction_Store_ID,
            Transactions.Transaction_DateTime
            FROM Transactions
            JOIN Cards ON Transactions.Customer_Card_ID = Cards.Customer_Card_ID
    ) AS Transactions
        ON Transactions.Customer_Card_ID = Cards.Customer_Card_ID
    JOIN Checks
        ON Checks.Transaction_ID = Transactions.Transaction_ID
    JOIN Product_matrix
        ON Product_matrix.SKU_ID = Checks.SKU_ID
    JOIN trade_points
        ON trade_points.Transaction_Store_ID = Transactions.Transaction_Store_ID
        AND trade_points.SKU_ID = Product_matrix.SKU_ID
    JOIN Periods
        ON Cards.Customer_ID = Periods.Customer_ID
    JOIN Purchase_History
        ON Cards.Customer_ID = Purchase_History.Customer_ID
        AND Periods.Group_ID = Purchase_History.Group_ID
    CROSS JOIN Date_Formation_Analysis
WHERE
    Transactions.Transaction_DateTime >= Analysis_Formation - INTERVAL '100 days'
    AND Transactions.Transaction_DateTime <= Periods.Last_Group_Purchase_Date
    AND Tr_Count < 5
GROUP BY
    Purchase_History.Customer_ID,
    Purchase_History.Group_ID,
    Periods.Group_Purchase,
    Group_Frequency,
    Analysis_Formation,
    Group_Min_Discount;

