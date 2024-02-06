/**
 * Part6.1 Выбор групп для определения предложения
 * Возвращает список групп для каждого клиента
 */
CREATE OR REPLACE FUNCTION choose_group_cross_selling(
	p_num_of_groups integer,
	p_max_churn_rate real,
	p_max_stability_rate real)
RETURNS TABLE (
	customer_id bigint,
	group_id bigint)
LANGUAGE plpgsql AS
$$
DECLARE
	l_array_of_customers_in bigint[] := ARRAY(SELECT customers.customer_id FROM customers);
	l_customer_id bigint;
	l_array_of_groups_temp bigint[];
	l_array_of_customers_out bigint[] = NULL;
	l_array_of_groups_out bigint[] = NULL;
	l_length integer;
	l_counter integer;
BEGIN
	FOREACH l_customer_id IN ARRAY l_array_of_customers_in LOOP
		l_array_of_groups_temp := ARRAY(
			SELECT "groups".group_id
			FROM "groups"
			WHERE group_churn_rate <= p_max_churn_rate
				AND group_stability_rate < p_max_stability_rate
			ORDER BY group_affinity_index DESC LIMIT p_num_of_groups);
		l_array_of_groups_out := l_array_of_groups_out || l_array_of_groups_temp;
		l_length := cardinality(l_array_of_groups_temp);
		l_counter := 0;
		WHILE l_counter < l_length LOOP
			l_array_of_customers_out := array_append(l_array_of_customers_out, l_customer_id);
			l_counter := l_counter + 1;
		END LOOP;
	END LOOP;
	RETURN QUERY
		SELECT unnest(l_array_of_customers_out) AS customer_id,
			unnest(l_array_of_groups_out) AS group_id;
END;
$$;
END;

SELECT * FROM choose_group_cross_selling(3, 1500, 1);


/**
 * 6.2 Функция определяет SKU с максимальной маржой для
 * каждого подобранного сочетания клиент+группа.
 */
CREATE OR REPLACE FUNCTION determine_greatest_sku_margin(
	p_num_of_groups integer,
	p_max_churn_rate real,
	p_max_stability_rate real)
RETURNS TABLE (
	customer_id bigint,
	group_id bigint,
	sku_id bigint)
LANGUAGE plpgsql AS
$$
DECLARE
BEGIN
	RETURN QUERY
		-- Магазин-группа-наименование-маржа по всей базе
		WITH groups_in_shops AS (
			SELECT tp.transaction_store_id,
					sku_group.group_id,
					pm.sku_id,
					tp.sku_retail_price - tp.sku_purchase_price AS margin 
			FROM product_matrix pm
			JOIN sku_group ON pm.group_id = sku_group.group_id
			JOIN trade_points tp ON tp.sku_id  = pm.sku_id),
		-- Клиенты по группам
		groups_cross_selling AS (
			SELECT cgcs.customer_id, cgcs.group_id
			FROM choose_group_cross_selling(p_num_of_groups,
					p_max_churn_rate, p_max_stability_rate) cgcs),
		-- Клиенты по группам с добавлением номера магазина
		shops_cross_selling AS (
			SELECT gcs.customer_id, gcs.group_id, customers.customer_primary_store
			FROM groups_cross_selling gcs
			JOIN customers ON customers.customer_id = gcs.customer_id
		),
		-- Максимальная маржа для заданного сочетания клиент+группа
		max_margin_by_group AS (
			SELECT scs.customer_id, gis.group_id, max(gis.margin) AS margin
			FROM groups_in_shops gis
			JOIN shops_cross_selling scs ON gis.group_id = scs.group_id
				AND scs.customer_primary_store = gis.transaction_store_id
			GROUP BY scs.customer_id, gis.group_id)
		-- Выбирает max(sku_id на случай), если максимальная маржа
		-- одновременно у нескльких товаров
		SELECT mmbg.customer_id::bigint, mmbg.group_id::bigint, max(gis.sku_id)::bigint AS sku_id
		FROM max_margin_by_group mmbg
		JOIN groups_in_shops gis ON mmbg.group_id = gis.group_id
			AND mmbg.margin = gis.margin
		GROUP BY mmbg.customer_id, mmbg.group_id;
END;
$$;
END;

--SELECT * FROM determine_greatest_sku_margin(3, 1500, 1);


/**
 * 6.3 Определение доли SKU в группе
 */
CREATE OR REPLACE FUNCTION determine_sku_share(
	p_num_of_groups integer,
	p_max_churn_rate real,
	p_max_stability_rate real,
	p_max_sku_share real)
RETURNS TABLE (
	customer_id bigint,
	group_id bigint,
	sku_id bigint)
LANGUAGE plpgsql AS
$$
DECLARE
BEGIN
	RETURN QUERY
		-- SKU с максимальной маржой по сочетанию клиент+группа
		WITH greatest_sku_margin AS (
			SELECT dgsm.customer_id, dgsm.group_id,	dgsm.sku_id
			FROM determine_greatest_sku_margin(p_num_of_groups, 
				p_max_churn_rate, p_max_stability_rate) dgsm),
		-- Общее количество транзакций по рассматриваемым сочетаниям клиент+группа
		num_of_all_checks AS (
			SELECT ph.customer_id, ph.group_id, count(checks.sku_id) AS num_of_checks
			FROM checks
			JOIN purchase_history ph ON ph.transaction_id = checks.transaction_id
			JOIN greatest_sku_margin gsm ON gsm.customer_id = ph.customer_id
				AND gsm.group_id = ph.group_id
			GROUP BY ph.customer_id, ph.group_id),
		-- Количество транзакцию по заданному сочетанию клиент+группа+SKU
		num_of_sku_checks AS (
			SELECT ph.customer_id, ph.group_id, count(checks.sku_id) AS num_of_checks
			FROM checks
			JOIN purchase_history ph ON ph.transaction_id = checks.transaction_id
			JOIN greatest_sku_margin gsm ON gsm.customer_id = ph.customer_id
				AND gsm.group_id = ph.group_id
				AND gsm.sku_id = checks.sku_id
			GROUP BY ph.customer_id, ph.group_id)
		SELECT gsm.customer_id, gsm.group_id, gsm.sku_id
		FROM greatest_sku_margin gsm
		JOIN num_of_all_checks noac ON gsm.customer_id = noac.customer_id
			AND gsm.group_id = noac.group_id
		JOIN num_of_sku_checks nosc ON gsm.customer_id = nosc.customer_id
			AND gsm.group_id = nosc.group_id
		WHERE nosc.num_of_checks / noac.num_of_checks * 100 < p_max_sku_share;
END;
$$;
END;

--SELECT * FROM determine_sku_share(3, 1500, 1, 150);

/**
 * 6.5 Расчет скидки
 */
CREATE OR REPLACE FUNCTION determine_discount_cross_selling(
	p_num_of_groups integer,
	p_max_churn_rate real,
	p_max_stability_rate real,
	p_max_sku_share real,
	p_margin_share real)
RETURNS TABLE (
	customer_id bigint,
	sku_name varchar,
	offer_discount_depth real)
LANGUAGE plpgsql AS
$$
DECLARE
BEGIN
	RETURN QUERY
		-- SKU по сочетанию клиент+группа
		WITH sku_for_offer AS (
			SELECT dss.customer_id, dss.group_id, dss.sku_id
			FROM determine_sku_share(p_num_of_groups, p_max_churn_rate,
				p_max_stability_rate, p_max_sku_share) dss),
		-- То же самое + номер магазина клиента + цены закупки и продажи
		sku_for_offer_shop_info AS (
			SELECT sfo.customer_id,
					sfo.group_id,
					sfo.sku_id,
					customers.customer_primary_store,
					tp.sku_purchase_price,
					tp.sku_retail_price
			FROM sku_for_offer sfo
			JOIN customers ON customers.customer_id = sfo.customer_id
			JOIN trade_points tp ON customers.customer_primary_store = tp.transaction_store_id
				AND tp.sku_id = sfo.sku_id),
		sku_for_offer_min_discount AS (
			SELECT sfosi.customer_id,
					sfosi.group_id,
					sfosi.sku_id,
					sfosi.customer_primary_store,
					sfosi.sku_purchase_price,
					sfosi.sku_retail_price,
					(CEILING("groups".group_min_discount * 20) / 20)::real AS offer_discount_depth
			FROM "groups"
			JOIN sku_for_offer_shop_info sfosi ON sfosi.group_id = "groups".group_id
				AND sfosi.customer_id = "groups".customer_id)
		SELECT sfomd.customer_id AS "Customer ID",
				pm.sku_name AS "SKU offers",
				sfomd.offer_discount_depth AS "Maximum discount depth"
		FROM sku_for_offer_min_discount sfomd
		JOIN product_matrix pm ON pm.sku_id = sfomd.sku_id
		WHERE p_margin_share * (sfomd.sku_retail_price - sfomd.sku_purchase_price) / sfomd.sku_retail_price >= sfomd.offer_discount_depth;
END;
$$;
END;

SELECT * FROM determine_discount_cross_selling(3, 1500, 1, 150, 150);