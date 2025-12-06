SELECT
	i."ID" AS id_invoice,
	i."ID_Boss" AS id_boss,
	app."ID_CustomerPay" AS id_payer,
	i."Date"::date AS op_date,
	i."Date" AS sort_ts,
	ROUND(i."Cost"::numeric, 2) AS amount,
	'Счёт №' || COALESCE(i."Nomer"::text, '') || ' от ' || TO_CHAR(i."Date", 'DD.MM.YYYY') AS descr,
	i."Mem" AS memo
FROM berg."XInvoices" i
JOIN berg."Applications" app ON app."ID" = i."ID_Application"
CROSS JOIN params p
WHERE i."Nomer" IS NOT NULL AND i."Nomer" > 0
	AND (p.p_id_payer IS NULL OR app."ID_CustomerPay" = p.p_id_payer)
	AND (p.p_id_boss   IS NULL OR i."ID_Boss" = p.p_id_boss)
