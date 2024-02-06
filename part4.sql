-- main function: your choice
SET DateStyle to 'German';


/**
 * Выбор группы для формирования предложения. Для отдельного клиента.
 * ! Функция проверена, работает правильно
 */ 

CREATE OR REPLACE FUNCTION reward_determination_max_discount (p_customer_id bigint,
															p_group_id bigint,
															p_margin_share real,
															p_num_of_transactions bigint)
RETURNS real
LANGUAGE plpgsql AS
$$
BEGIN
	RETURN (
		SELECT group_margin * p_margin_share / p_num_of_transactions / 100
		FROM "groups"
		WHERE customer_id  = p_customer_id
			AND group_id = p_group_id
	);
END
$$;

/**
 * Определение максимально возможного размера скидки для вознаграждения.
 * Для отедельного клиента по отдельной группе.
 */

CREATE OR REPLACE FUNCTION reward_determination_group (p_customer_id bigint,
														p_max_churn_rate real,
														p_max_discount_share real,
														p_banned_group bigint[])
RETURNS bigint
LANGUAGE plpgsql AS
$$
BEGIN
	IF p_banned_group IS NULL THEN 
		RETURN (
			SELECT group_id
			FROM "groups"
			WHERE	customer_id  = p_customer_id
				AND group_churn_rate <= p_max_churn_rate
				AND group_discount_share < p_max_discount_share
			ORDER BY group_affinity_index DESC LIMIT 1
		);
	ELSE 
		RETURN (
			SELECT group_id
			FROM "groups"
			WHERE	customer_id  = p_customer_id
				AND NOT (group_id = ANY(p_banned_group))
				AND group_churn_rate <= p_max_churn_rate
				AND group_discount_share < p_max_discount_share
			ORDER BY group_affinity_index DESC LIMIT 1
		);
	END IF;
END;
$$;
---

--SELECT * FROM reward_determination_group (1, 1500, 99, NULL);
--SELECT * FROM reward_determination_group (1, 1500, 99, ARRAY[1, 2, 7]);




/**
 * Определение величины скидки для списка клиентов.
 */
CREATE OR REPLACE FUNCTION reward_determination (p_max_churn_rate real,
														p_max_discount_share real,
														p_margin_share real)
RETURNS TABLE (customer_id bigint, group_id bigint, offer_discount_depth real)
LANGUAGE plpgsql AS
$$
DECLARE
	l_banned_group bigint[];
	l_no_more_groups boolean;
	l_group_id bigint;
	l_max_discount real;
	l_min_discount real;
	l_array_of_customers bigint[] := ARRAY(SELECT customers.customer_id FROM customers);
	l_customer_id bigint;
	l_array_of_group_ids bigint[];
	l_array_of_discounts real[];
	l_num_of_transactions bigint;
BEGIN
	FOREACH l_customer_id IN ARRAY l_array_of_customers LOOP
		l_group_id := NULL;
		l_min_discount := NULL;
		l_banned_group := NULL;
		l_no_more_groups := FALSE;
		WHILE l_no_more_groups = FALSE LOOP
			l_group_id := reward_determination_group(l_customer_id,
							p_max_churn_rate, p_max_discount_share, l_banned_group);
			IF l_group_id IS NULL THEN
				l_no_more_groups := TRUE;
				l_array_of_group_ids := array_append(l_array_of_group_ids, NULL);
				l_array_of_discounts := array_append(l_array_of_discounts, NULL);
			ELSE
				l_num_of_transactions := (SELECT count(ph.transaction_id)
											FROM purchase_history ph
											WHERE ph.customer_id = l_customer_id
												AND ph.group_id = l_group_id);
				l_max_discount := reward_determination_max_discount(l_customer_id, 
									l_group_id, p_margin_share, l_num_of_transactions);
				l_min_discount := (SELECT CEILING(group_min_discount * 20) / 20 * 100
									FROM "groups"
									WHERE "groups".customer_id = l_customer_id
										AND "groups".group_id = l_group_id);
				IF l_min_discount < l_max_discount THEN
					l_array_of_group_ids := array_append(l_array_of_group_ids, l_group_id);
					l_array_of_discounts := array_append(l_array_of_discounts, l_min_discount);					
					l_no_more_groups:=TRUE;				
				ELSE
					l_banned_group := array_append(l_banned_group, l_group_id);
				END IF;
			END IF;
		END LOOP;
	END LOOP;
	RETURN QUERY
		SELECT unnest(l_array_of_customers) AS customer_id,
			unnest(l_array_of_group_ids) AS group_id,
			unnest(l_array_of_discounts) AS offer_discount_depth;
END;
$$;
					
--SELECT * FROM reward_determination (1500, 99, 15);
--SELECT * FROM reward_determination (1, 50, 10);
SELECT * FROM reward_determination (3, 70, 30);

-- Параметры функции:
-- метод расчета среднего чека (1 - за период, 2 - за количество)
-- первая и последняя даты периода (для 1 метода)
-- количество транзакций (для 2 метода)
-- коэффициент увеличения среднего чека
-- максимальный индекс оттока
-- максимальная доля транзакций со скидкой (в процентах)
-- допустимая доля маржи (в процентах)

-- -- метод расчета среднего чека (1 - за период)
CREATE OR REPLACE FUNCTION period_method(
first_last_date_1 varchar,
coef_increase_avg_check real
)
RETURNS TABLE (Customer_ID bigint, Required_Check_Measure real)
LANGUAGE plpgsql AS
$$
DECLARE
    first_date date := split_part(first_last_date_1, ' ', 1)::date;
    second_date date := split_part(first_last_date_1, ' ', 2)::date;
BEGIN
    IF (first_date < check_date(1)) THEN -- check dates
        first_date = check_date(1);
    ELSIF (second_date > check_date(2)) THEN
        second_date = check_date(2);
    ELSIF (first_date >= second_date) THEN
        RAISE EXCEPTION 'последняя дата указываемого периода должна быть позже первой';
    END IF;
    RETURN QUERY
        WITH query_avg AS (
            SELECT cards.customer_id AS Customer_ID, (t.transaction_summ) AS trans_summ
            FROM cards
            JOIN transactions t on cards.customer_card_id = t.customer_card_id
            WHERE t.transaction_datetime BETWEEN first_date and second_date)
        SELECT query_avg.Customer_ID, avg(trans_summ)::real * coef_increase_avg_check AS Avg_check
        FROM query_avg
        GROUP BY query_avg.Customer_ID
        ORDER BY Customer_ID;
END;
$$;

-- метод расчета среднего чека (2 - за количество)
CREATE OR REPLACE FUNCTION number_method (transact_num bigint, coef_increase_avg_check real)
RETURNS TABLE (Customer_ID bigint, Required_Check_Measure real)
LANGUAGE plpgsql AS
$$
BEGIN
    RETURN QUERY
    WITH query_avg AS (
        SELECT customer_card_id, transaction_summ
        FROM transactions
        ORDER BY transaction_datetime DESC LIMIT transact_num)
    SELECT c.Customer_ID, avg(transaction_summ)::real * coef_increase_avg_check AS Avg_check
    FROM query_avg
    JOIN cards c ON c.customer_card_id = query_avg.customer_card_id
    GROUP BY c.Customer_ID
    ORDER BY 1;
END;
$$;



-- получаем первую и последнюю дату транзакции для функции period_method
DROP FUNCTION IF EXISTS check_date(integer);
CREATE FUNCTION check_date(number integer)
    RETURNS SETOF date
    LANGUAGE plpgsql AS
    $$
    BEGIN
        IF (number = 1) THEN
            RETURN QUERY
            SELECT transaction_datetime::date
            FROM transactions
            ORDER BY 1 LIMIT 1;
        ELSEIF (number = 2) THEN
            RETURN QUERY
            SELECT transaction_datetime::date
            FROM transactions
            ORDER BY 1 DESC LIMIT 1;
        END IF;
    END;
    $$;

  /**
   * Выбор метода расчета среднего чека
   */
CREATE OR REPLACE FUNCTION choose_avg_check_method(
    choose_method_1_2 integer,
    first_last_date_1 varchar,
    trans_count_2 bigint,
    coef_increase_avg_check real
  )
	RETURNS TABLE (
		customer_id bigint,
		avg_check real)
	LANGUAGE plpgsql AS
	$$
	BEGIN
        IF (choose_method_1_2 = 1) THEN
            RETURN QUERY
            SELECT *
            FROM period_method(first_last_date_1, coef_increase_avg_check);
        ELSEIF  (choose_method_1_2 = 2) THEN 
            RETURN QUERY
            SELECT *
            FROM number_method(trans_count_2, coef_increase_avg_check);
        ELSE
        	RETURN QUERY
        		SELECT NULL AS customer_id, NULL AS avg_check;
        END IF;
    END;
    $$;  	
    
CREATE OR REPLACE FUNCTION determine_offers_greater_agv_check(
    p_choose_method_1_2 integer,
    p_first_last_date_1 varchar,
    p_trans_count_2 bigint,
    p_coef_increase_avg_check real,
    p_max_churn_rate real,
    p_max_discount_share real,
    p_margin_share real
)
    RETURNS TABLE (
        Customer_ID bigint,
        Required_Check_Measure real,
        Group_Name varchar,
        Offer_Discount_Depth real
        )
    LANGUAGE plpgsql AS
    $$
    BEGIN
		RETURN QUERY
			WITH customers_avg_check AS (
				SELECT cacm.customer_id, cacm.avg_check
				FROM choose_avg_check_method(p_choose_method_1_2, p_first_last_date_1,
					p_trans_count_2, p_coef_increase_avg_check) cacm
			),
			reward AS (
				SELECT rewd.customer_id, rewd.group_id, rewd.offer_discount_depth
				FROM reward_determination (p_max_churn_rate, p_max_discount_share, p_margin_share) rewd
			)
			SELECT rd.customer_id AS "Идентификатор клиента",
					cac.avg_check AS "Целевое значение среднего чека",
					sg.group_name AS "Группа предложения",
					rd.offer_discount_depth AS "Максимальная глубина скидки"					
			FROM sku_group sg
			RIGHT JOIN reward rd ON sg.group_id = rd.group_id		
			JOIN customers_avg_check cac ON rd.customer_id = cac.customer_id;
    END;
    $$;

-- Call
SELECT *
FROM determine_offers_greater_agv_check(1, '05.02.2021 21.04.2022', 30,  1.5, 4, 61, 25);
SELECT *
FROM determine_offers_greater_agv_check(2, '05.02.2021 21.04.2022', 100,  1.15, 3, 70, 30);

