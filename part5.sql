SET DateStyle TO 'German';

/**
 * Определение условия предложения, ориентированного на рост частоты визитов.
 * Для всех клиентов.
 */
CREATE OR REPLACE FUNCTION offer_condition_determination (p_start_date date,
												p_end_date date,
												p_added_transactions_number integer)
RETURNS TABLE (customer_id bigint, required_transactions_count integer)
LANGUAGE plpgsql AS
$$
BEGIN
	RETURN QUERY
		SELECT intensity.customer_id, round(base_intensity)::integer + p_added_transactions_number AS required_transactions_count
		FROM
			(SELECT customers.customer_id, (p_end_date - p_start_date)::numeric / customer_frequency AS base_intensity
			FROM customers) intensity;
END
$$;

/**
 * Формирование персональных предложений, ориентированных на рост частоты визитов
 */
CREATE OR REPLACE FUNCTION determine_offers_more_visits (p_start_date date,
												p_end_date date,
												p_added_transactions_number integer,
												p_max_churn_rate real,
												p_max_discount_share real,
												p_margin_share real)
RETURNS TABLE (customer_id bigint,
				start_date date,
				end_date date,
				required_transactions_count integer,
				group_name varchar,
				offer_discount_depth real)
LANGUAGE plpgsql AS
$$
DECLARE
BEGIN
	RETURN QUERY
		WITH offer_condition AS (
			SELECT ocd.customer_id, ocd.required_transactions_count
			FROM offer_condition_determination(p_start_date, p_end_date, p_added_transactions_number) ocd
		),
		reward AS (
			SELECT rewd.customer_id, rewd.group_id, rewd.offer_discount_depth
			FROM reward_determination (p_max_churn_rate, p_max_discount_share, p_margin_share) rewd
		)
		SELECT rd.customer_id AS "Идентификатор клиента",
				p_start_date AS "Дата начала периода",
				p_end_date AS "Дата окончания периода",
				oc.required_transactions_count AS "Целевое количество транзакций",
				sg.group_name AS "Группа предложения",
				rd.offer_discount_depth AS "Максимальная глубина скидки"
		FROM sku_group sg
		RIGHT JOIN reward rd ON sg.group_id = rd.group_id
		JOIN offer_condition oc ON rd.customer_id = oc.customer_id;
END;
$$;

-- SELECT * FROM determine_offers_more_visits ('01.09.2022', '30.09.2022', 3, 1500, 99, 15);
-- SELECT * FROM determine_offers_more_visits ('01.09.2022', '30.09.2022', 3, 1, 50, 10);