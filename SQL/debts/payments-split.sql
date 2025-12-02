SELECT
	invp."ID" AS id_payment,
	invp."ID_XInvoice" AS id_invoice,
	inv."ID_Boss" AS id_boss,
	app."ID_CustomerPay" AS id_payer,
	invp."Date"::date AS op_date,
	invp."Date" AS sort_ts,
	invp."IsCash",
	inv."Nomer" AS invoice_nomer,
	ROUND(invp."Cost"::numeric, 2) AS amount_paid,
	COALESCE(SUM(invp."Cost") OVER (
		PARTITION BY invp."ID_XInvoice"
		ORDER BY invp."Date"
		ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
	), 0)::numeric AS paid_before
FROM berg."XInvoicePays" invp
JOIN berg."XInvoices" inv ON inv."ID" = invp."ID_XInvoice"
JOIN berg."Applications" app ON app."ID" = inv."ID_Application"
WHERE inv."Nomer" IS NOT NULL AND inv."Nomer" > 0
