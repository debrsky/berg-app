WITH params AS (
	SELECT
        51158::integer AS id_payer,  -- NULL = все плательщики
        3::integer     AS id_seller    -- NULL = все перевозчики
),

invoices AS (
    SELECT
        inv."ID" AS id_invoice,
        inv."ID_Boss" AS id_seller,
        app."ID_CustomerPay" AS id_payer,
        inv."Date"::date AS op_date,
        inv."Date" AS sort_ts,
        ROUND(inv."Cost"::numeric, 2) AS amount,
        'Счёт №' || COALESCE(inv."Nomer"::text, '') || ' от ' || TO_CHAR(inv."Date", 'YYYY-MM-DD') AS descr
    FROM berg."XInvoices" inv
    JOIN berg."Applications" app ON app."ID" = inv."ID_Application"
    CROSS JOIN params p
    WHERE inv."Nomer" IS NOT NULL AND inv."Nomer" > 0
      AND (p.id_payer IS NULL OR app."ID_CustomerPay" = p.id_payer)
      AND (p.id_seller   IS NULL OR inv."ID_Boss" = p.id_seller)
),

payments_split AS (
	SELECT
		invp."ID" AS id_payment,
		invp."ID_XInvoice" AS id_invoice,
		inv."ID_Boss" AS id_seller,
		app."ID_CustomerPay" AS id_payer,
		invp."Date"::date AS op_date,
		invp."Date" AS sort_ts,
		invp."IsCash" AS is_cash,
		inv."Nomer" AS invoice_nomer,
		ROUND(invp."Cost"::numeric, 2) AS amount_paid,
		COALESCE(SUM(ROUND(invp."Cost"::numeric, 2)) OVER (
			PARTITION BY invp."ID_XInvoice"
			ORDER BY invp."Date"
			ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
		), 0)::numeric AS paid_before,
		ROUND(inv."Cost"::numeric, 2) AS invoice_amount,
		GREATEST(
			0::numeric, 
			ROUND(invp."Cost"::numeric, 2) 
			+ COALESCE(SUM(ROUND(invp."Cost"::numeric, 2)) OVER (
				PARTITION BY invp."ID_XInvoice"
				ORDER BY invp."Date"
				ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
			), 0)::numeric 
			- ROUND(inv."Cost"::numeric, 2)
		) AS avans_added
	FROM berg."XInvoicePays" invp
	JOIN berg."XInvoices" inv ON inv."ID" = invp."ID_XInvoice"
	JOIN berg."Applications" app ON app."ID" = inv."ID_Application"
    CROSS JOIN params p
	WHERE inv."Nomer" IS NOT NULL AND inv."Nomer" > 0
      AND (p.id_payer IS NULL OR app."ID_CustomerPay" = p.id_payer)
      AND (p.id_seller   IS NULL OR inv."ID_Boss" = p.id_seller)
),

real_operations AS (
    -- Начисления
    SELECT
        id_seller,
        id_payer,
        id_invoice,
        op_date,
        10                     AS ord,
        sort_ts                AS sort_ts,
		
        'Начисление'::text     AS operation,
        descr                  AS description,
        
		amount,

		amount                 AS debit,
        0::numeric             AS credit,

		0::numeric             AS avans_added,
        0::numeric             AS avans_used
    FROM invoices

    UNION ALL

    SELECT
        p.id_seller,
        p.id_payer,
        p.id_invoice,
        p.op_date,
        20,
        p.sort_ts,
		
        'Оплата счёта'::text,
        'Оплата счёта №' || p.invoice_nomer ||
            CASE WHEN p.is_cash THEN ' (наличные)' ELSE ' (безнал)' END,
		
		p.amount_paid AS amount,
        
		0::numeric AS debit,
        p.amount_paid - p.avans_added AS credit,

		p.avans_added AS avans_added,
        0::numeric AS avans_used
    FROM payments_split p	
), 

balance AS (
	SELECT 
		id_seller,
		id_payer,
		id_invoice,
		op_date,
		ord, 
		sort_ts,
		amount,
		debit,
		credit,
		SUM (debit - credit) OVER (
			PARTITION BY id_seller, id_payer
			ORDER BY op_date, ord, sort_ts
			ROWS UNBOUNDED PRECEDING
		) AS saldo_runnning,
		avans_added,
		avans_used,
		SUM (avans_added - avans_used) OVER (
			PARTITION BY id_seller, id_payer
			ORDER BY op_date, ord, sort_ts
			ROWS UNBOUNDED PRECEDING
		) AS avans_runnning,
		SUM (credit - debit + avans_added) OVER (
			PARTITION BY id_seller, id_payer
			ORDER BY op_date, ord, sort_ts
			ROWS UNBOUNDED PRECEDING
		) AS balance_payer
	FROM real_operations
)

SELECT * FROM balance ORDER BY op_date, ord, sort_ts;
