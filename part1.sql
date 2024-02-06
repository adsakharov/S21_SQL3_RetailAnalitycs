-- Напишите скрипт part1.sql, создающий базу данных и таблицы, описанные выше в разделе входные данные.
-- Также внесите в скрипт процедуры, позволяющие импортировать и экспортировать данные для каждой таблицы
-- из файлов/в файлы с расширением .csv и .tsv.
-- В качестве параметра каждой процедуры для импорта из csv файла указывается разделитель.

SET DateStyle to 'German';

CREATE TABLE Personal_Data (
    Customer_ID SERIAL PRIMARY KEY,
    Customer_Name VARCHAR CHECK (Customer_Name ~ '^[А-ЯЁA-Z][а-яёa-z\- ]*$'),
    Customer_Surname VARCHAR CHECK (Customer_Surname ~ '^[А-ЯЁA-Z][а-яёa-z\- ]*$'),
    Customer_Primary_Email VARCHAR CHECK ( Customer_Primary_Email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
    Customer_Primary_Phone VARCHAR CHECK ( Customer_Primary_Phone ~ '^\+7\d{10}$')
);

CREATE TABLE cards (
    Customer_Card_ID SERIAL PRIMARY KEY,
    Customer_ID BIGINT NOT NULL REFERENCES Personal_Data(Customer_ID)
);

CREATE TABLE sku_group (
    Group_ID SERIAL PRIMARY KEY,
    Group_Name VARCHAR CHECK (Group_Name ~ '^[А-ЯЁа-яёA-Za-z0-9 \s\!\"\#\$\%\&\(\)\*\+\,\-\.\/\:\;\<\=\>\?\@\[\\\]\^\_\`{|}~]*$')
);

CREATE TABLE product_matrix (
    SKU_ID SERIAL PRIMARY KEY,
    SKU_Name VARCHAR CHECK (SKU_Name ~ '^[А-ЯЁа-яёA-Za-z0-9 \s\!\"\#\$\%\&\(\)\*\+\,\-\.\/\:\;\<\=\>\?\@\[\\\]\^\_\`{|}~]*$'),
    Group_ID BIGINT NOT NULL REFERENCES sku_group(Group_ID)
);

CREATE TABLE trade_points(
    Transaction_Store_ID SERIAL,
    SKU_ID BIGINT NOT NULL REFERENCES product_matrix(SKU_ID),
    SKU_Purchase_Price NUMERIC,
    SKU_Retail_Price NUMERIC
);

CREATE TABLE transactions (
    Transaction_ID SERIAL PRIMARY KEY UNIQUE,
    Customer_Card_ID BIGINT NOT NULL REFERENCES cards(Customer_Card_ID),
    Transaction_Summ NUMERIC,
    Transaction_DateTime TIMESTAMP,
    Transaction_Store_ID BIGINT NOT NULL
);

CREATE TABLE checks(
    Transaction_ID SERIAL,
    SKU_ID BIGINT NOT NULL REFERENCES product_matrix(SKU_ID),
    SKU_Amount NUMERIC,
    SKU_Summ NUMERIC,
    SKU_Summ_Paid NUMERIC,
    SKU_Discount NUMERIC
);

CREATE TABLE date_formation_analysis (
    Analysis_Formation timestamp
);

------------------------- IMPORT PROCEDURES -------------------------

CREATE OR REPLACE PROCEDURE import(
    IN table_name VARCHAR,
    IN import_file VARCHAR,
    IN del char)
LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE FORMAT('
        COPY %s
        FROM ''%s''
        WITH (FORMAT CSV, NULL ''NULL'', DELIMITER ''%s'')',
        table_name,
        import_file,
        del);
END;
$$;

CREATE OR REPLACE PROCEDURE import_Personal_Data_csv(IN del char)
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL import(
        'personal_data',
        'PATH/../datasets/Personal_Data_Mini.csv',
        del);
END;
$$;

CREATE OR REPLACE PROCEDURE import_Personal_Data_tsv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL import(
        'personal_data',
        'PATH/../datasets/Personal_Data_Mini.tsv',
        E'\t');
END;
$$;

CREATE OR REPLACE PROCEDURE import_Cards_csv(IN del char)
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL import(
        'cards',
        'PATH/../datasets/Cards_Mini.csv',
        del);
END;
$$;

CREATE OR REPLACE PROCEDURE import_Cards_tsv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL import(
        'cards',
        'PATH/../datasets/Cards_Mini.tsv',
        E'\t');
END;
$$;

CREATE OR REPLACE PROCEDURE import_Groups_SKU_csv(IN del char)
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL import(
        'sku_group',
        'PATH/../datasets/Groups_SKU_Mini.csv',
        del);
END;
$$;


CREATE OR REPLACE PROCEDURE import_Groups_SKU_tsv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL import(
        'sku_group',
        'PATH/../datasets/Groups_SKU_Mini.tsv',
        E'\t');
END;
$$;

CREATE OR REPLACE PROCEDURE import_SKU_csv(IN del char)
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL import(
        'product_matrix',
        'PATH/../datasets/SKU_Mini.csv',
        del);
END;
$$;

CREATE OR REPLACE PROCEDURE import_SKU_tsv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL import(
        'product_matrix',
        'PATH/../datasets/SKU_Mini.tsv',
        E'\t');
END;
$$;


CREATE OR REPLACE PROCEDURE import_Stores_csv(IN del char)
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL import(
        'trade_points',
        'PATH/../datasets/Stores_Mini.csv',
        del);
END;
$$;

CREATE OR REPLACE PROCEDURE import_Stores_tsv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL import(
        'trade_points',
        'PATH/../datasets/Stores_Mini.tsv',
        E'\t');
END;
$$;

CREATE OR REPLACE PROCEDURE import_Transactions_csv(IN del char)
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL import(
        'transactions',
        'PATH/../datasets/Transactions_Mini.csv',
        del);
END;
$$;

CREATE OR REPLACE PROCEDURE import_Transactions_tsv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL import(
        'transactions',
        'PATH/../datasets/Transactions_Mini.tsv',
        E'\t');
END;
$$;

CREATE OR REPLACE PROCEDURE import_Checks_csv(IN del char)
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL import(
        'checks',
        'PATH/../datasets/Checks_Mini.csv',
        del);
END;
$$;


CREATE OR REPLACE PROCEDURE import_Checks_tsv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL import(
        'checks',
        'PATH/../datasets/Checks_Mini.tsv',
        E'\t');
END;
$$;

CREATE OR REPLACE PROCEDURE import_Date_Of_Analysis_Formation_csv(IN del char)
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL import(
        'date_formation_analysis',
        'PATH/../datasets/Date_Of_Analysis_Formation.csv',
        del);
END;
$$;

CREATE OR REPLACE PROCEDURE import_Date_Of_Analysis_Formation_tsv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL import(
        'date_formation_analysis',
        'PATH/../datasets/Date_Of_Analysis_Formation.tsv',
        E'\t');
END;
$$;

------------------------- EXPORT PROCEDURES -------------------------

CREATE OR REPLACE PROCEDURE export(IN table_name VARCHAR, IN exp_file VARCHAR, IN del char)
LANGUAGE plpgsql
AS
$$
DECLARE
    ext VARCHAR(3) := 'csv';
BEGIN
	EXECUTE FORMAT('
	COPY %s
	TO ''%s''
	WITH (FORMAT CSV, NULL ''NULL'', DELIMITER ''%s'')',
    table_name,
    exp_file,
    del);
END;
$$;

CREATE OR REPLACE PROCEDURE export_Personal_Data(IN del char, IN ext VARCHAR)
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export(
        'personal_data',
        'PATH/../datasets/Personal_Data_Mini_Export.' || ext,
        del);
END;
$$;

CREATE OR REPLACE PROCEDURE export_Personal_Data_csv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export_Personal_Data(',', 'csv');
END;
$$;

CREATE OR REPLACE PROCEDURE export_Personal_Data_tsv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export_Personal_Data(E'\t', 'tsv');
END;
$$;

CREATE OR REPLACE PROCEDURE export_Cards(IN del char, IN ext VARCHAR)
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export(
        'cards',
        'PATH/../datasets/Cards_Mini_Export.' || ext,
        del);
END;
$$;

CREATE OR REPLACE PROCEDURE export_Cards_csv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export_Cards(',', 'csv');
END;
$$;

CREATE OR REPLACE PROCEDURE export_Cards_tsv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export_Cards(E'\t', 'tsv');
END;
$$;

CREATE OR REPLACE PROCEDURE export_Groups_SKU(IN del char, IN ext VARCHAR)
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export(
        'sku_group',
        'PATH/../datasets/Groups_SKU_Mini_Export.' || ext,
        del);
END;
$$;

CREATE OR REPLACE PROCEDURE export_Groups_SKU_csv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export_Groups_SKU(',', 'csv');
END;
$$;

CREATE OR REPLACE PROCEDURE export_Groups_SKU_tsv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export_Groups_SKU(E'\t', 'tsv');
END;
$$;

CREATE OR REPLACE PROCEDURE export_SKU(IN del char, IN ext VARCHAR)
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export(
        'product_matrix',
        'PATH/../datasets/SKU_Mini_Export.' || ext,
        del);
END;
$$;

CREATE OR REPLACE PROCEDURE export_SKU_csv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export_SKU(',', 'csv');
END;
$$;

CREATE OR REPLACE PROCEDURE export_SKU_tsv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export_SKU(E'\t', 'tsv');
END;
$$;

CREATE OR REPLACE PROCEDURE export_Stores(IN del char, IN ext VARCHAR)
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export(
        'trade_points',
        'PATH/../datasets/Stores_Mini_Export.' || ext,
        del);
END;
$$;

CREATE OR REPLACE PROCEDURE export_Stores_csv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export_Stores(',', 'csv');
END;
$$;

CREATE OR REPLACE PROCEDURE export_Stores_tsv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export_Stores(E'\t', 'tsv');
END;
$$;

CREATE OR REPLACE PROCEDURE export_Transactions(IN del char, IN ext VARCHAR)
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export(
        'transactions',
        'PATH/../datasets/Transactions_Mini_Export.' || ext,
        del);
END;
$$;

CREATE OR REPLACE PROCEDURE export_Transactions_csv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export_Transactions(',', 'csv');
END;
$$;

CREATE OR REPLACE PROCEDURE export_Transactions_tsv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export_Transactions(E'\t', 'tsv');
END;
$$;

CREATE OR REPLACE PROCEDURE export_Checks(IN del char, IN ext VARCHAR)
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export(
        'checks',
        'PATH/../datasets/Checks_Mini_Export.' || ext,
        del);
END;
$$;

CREATE OR REPLACE PROCEDURE export_Checks_csv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export_Checks(',', 'csv');
END;
$$;

CREATE OR REPLACE PROCEDURE export_Checks_tsv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export_Checks(E'\t', 'tsv');
END;
$$;

CREATE OR REPLACE PROCEDURE export_Date_Of_Analysis_Formation(IN del char, IN ext VARCHAR)
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export(
        'date_formation_analysis',
        'PATH/../datasets/Date_Of_Analysis_Formation_Export.' || ext,
        del);
END;
$$;

CREATE OR REPLACE PROCEDURE export_Date_Of_Analysis_Formation_csv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export_Date_Of_Analysis_Formation(',', 'csv');
END;
$$;

CREATE OR REPLACE PROCEDURE export_Date_Of_Analysis_Formation_tsv()
LANGUAGE plpgsql
AS
$$
BEGIN
    CALL export_Date_Of_Analysis_Formation(E'\t', 'tsv');
END;
$$;

------------------------- FILL TABLES -------------------------
CALL import_Personal_Data_tsv();
CALL import_Cards_tsv();
CALL import_Groups_SKU_tsv();
CALL import_SKU_tsv();
CALL import_Stores_tsv();
CALL import_Transactions_tsv();
CALL import_Checks_tsv();
CALL import_Date_Of_Analysis_Formation_tsv();

CALL export_Personal_Data_csv();
CALL export_Personal_Data_tsv();
CALL export_Cards_csv();
CALL export_Cards_tsv();
CALL export_Groups_SKU_csv();
CALL export_Groups_SKU_tsv();
CALL export_SKU_csv();
CALL export_SKU_tsv();
CALL export_Stores_csv();
CALL export_Stores_tsv();
CALL export_Transactions_csv();
CALL export_Transactions_tsv();
CALL export_Checks_csv();
CALL export_Checks_tsv();
CALL export_Date_Of_Analysis_Formation_csv();
CALL export_Date_Of_Analysis_Formation_tsv();
