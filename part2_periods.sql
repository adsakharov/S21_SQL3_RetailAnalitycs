CREATE MATERIALIZED VIEW Periods AS
    SELECT
        Customer_ID,
        sku_group.group_id,
        MIN(Transaction_DateTime) AS First_Group_Purchase_Date,
        MAX(Transaction_DateTime) AS Last_Group_Purchase_Date,
        COUNT(Transactions.transaction_id) AS Group_Purchase,
        (CAST(EXTRACT('epoch' FROM (MAX(Transaction_DateTime) - MIN(Transaction_DateTime))) AS FLOAT) / 86400.0 + 1.0) / CAST(COUNT(Transactions.transaction_id) AS FLOAT) AS Group_Frequency,
        CASE WHEN MAX(Checks.SKU_Discount) = 0 THEN 0
        ELSE MIN(
            (CASE WHEN Checks.SKU_Discount != 0 THEN Checks.SKU_Discount END)
         / Checks.SKU_Summ)
        END AS Group_Min_Discount
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
        JOIN sku_group
            ON sku_group.group_id = product_matrix.group_id
    GROUP BY
        Cards.Customer_ID,
        sku_group.group_id;