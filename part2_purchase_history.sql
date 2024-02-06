CREATE MATERIALIZED VIEW Purchase_History AS
    SELECT
        Customer_ID,
        transactions.Transaction_ID,
        Transaction_DateTime,
        Group_ID,
        SUM(SKU_Purchase_Price * SKU_Amount) AS Group_Cost,
        SUM(SKU_Summ) AS Group_Summ,
        SUM(SKU_Summ_Paid) AS Group_Summ_Paid
    FROM
        Cards
        JOIN transactions
            ON Cards.customer_card_id = transactions.customer_card_id
        JOIN Checks
            ON Checks.transaction_id = transactions.transaction_id
        JOIN product_matrix
            ON product_matrix.SKU_ID = Checks.SKU_ID
        JOIN trade_points
            ON trade_points.Transaction_Store_ID = Transactions.Transaction_Store_ID
            AND product_matrix.SKU_ID = trade_points.SKU_ID
    GROUP BY        
        Customer_id,
        transactions.transaction_id,
        product_matrix.group_id;
    