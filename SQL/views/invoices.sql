-- View: bergapp.invoices

DROP MATERIALIZED VIEW IF EXISTS bergapp.invoices;

CREATE MATERIALIZED VIEW IF NOT EXISTS bergapp.invoices
AS
	SELECT inv."ID" AS id_invoice,
		inv."ID_Boss" AS id_seller,
		app."ID_CustomerPay" AS id_payer,
		app."ID_Customer" AS id_consigner,
		app."ID_CustomerOut" AS id_consignee,
		inv."Nomer" AS nomer,
		inv."Date"::date AS inv_date,
		inv."Date" AS inv_date_ts,
		round(inv."Cost"::numeric, 2) AS amount,
		CASE WHEN inv."Date"::date < '2025-12-01'::date THEN 0::numeric ELSE b."NDS"::numeric END AS nds,
		inv."Name" AS content,
		inv."Mem" AS memo,
		jsonb_build_object(
			'id_app', app."ID",
			'nomer', app."Nomer",
			'base_code', CONVERT_FROM(SET_BYTE(E'\\x00'::bytea, 0, app."BaseCode"), 'WIN1251'),
			'date_reg', app."DateReg"::date,
			'cargo', c."Name",
			'weight', ROUND((app."Weight" * 1000)::numeric, 0),
			'volume', ROUND(app."Volume"::numeric, 2),
			'count_pcs', app."CountPcs"::numeric
		) AS app,
		CASE
			WHEN inv."IsFixed" THEN jsonb_build_array(jsonb_build_object('pos', 0, 'name', inv."Name", 'price', round(inv."Cost"::numeric, 2), 'qty', 1, 'mUcode', '796', 'mU', 'шт', 'amount', round(inv."Cost"::numeric, 2)))
			ELSE COALESCE(( SELECT jsonb_agg(jsonb_build_object('pos', invd."Pos", 'name', invd."Name", 'price', round(invd."Price"::numeric, 2), 'qty', invd."Amount", 'mUcode',
					CASE invd."TypeEd"
						WHEN 0 THEN '796'::text
						WHEN 1 THEN '166'::text
						WHEN 2 THEN '113'::text
						WHEN 3 THEN '356'::text
						ELSE '---'::text
					END, 'mU',
					CASE invd."TypeEd"
						WHEN 0 THEN 'шт'::text
						WHEN 1 THEN 'кг'::text
						WHEN 2 THEN 'м³'::text
						WHEN 3 THEN 'час'::text
						ELSE 'рейс'::text
					END, 'amount', round((invd."Price" * invd."Amount")::numeric, 2)) ORDER BY invd."Pos") AS jsonb_agg
			   FROM bergauto."XInvoiceDatas" invd
			  WHERE invd."ID_XInvoice" = inv."ID" AND invd."IsFree" = false), jsonb_build_array(jsonb_build_object('pos', 0, 'name', inv."Name", 'price', round(inv."Cost"::numeric, 2), 'qty', 1, 'mUcode', '796', 'mU', 'шт', 'amount', round(inv."Cost"::numeric, 2))))
		END AS details
	FROM bergauto."XInvoices" inv
	   JOIN bergauto."Applications" app ON app."ID" = inv."ID_Application"
	   JOIN bergauto."Bosses" b ON b."ID" = inv."ID_Boss"
	   JOIN bergauto."Cargos" c ON c."ID" = app."ID_Cargo"
	WHERE inv."Nomer" <> 0
WITH DATA;
