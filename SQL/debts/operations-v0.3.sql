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
		ROUND(invp."Cost"::numeric, 2) AS amount_paid
	FROM berg."XInvoicePays" invp
	JOIN berg."XInvoices" inv ON inv."ID" = invp."ID_XInvoice"
	JOIN berg."Applications" app ON app."ID" = inv."ID_Application"
    CROSS JOIN params p
	WHERE inv."Nomer" IS NOT NULL AND inv."Nomer" > 0
      AND (p.id_payer IS NULL OR app."ID_CustomerPay" = p.id_payer)
      AND (p.id_seller   IS NULL OR inv."ID_Boss" = p.id_seller)
),

operations AS (
    -- Начисления
    SELECT
        id_seller,
        id_payer,
        id_invoice,
        op_date,
        1                      AS op_type, -- Начисление
        10                     AS ord,
		amount,
        sort_ts
    FROM invoices

    UNION ALL

    SELECT
        p.id_seller,
        p.id_payer,
        p.id_invoice,
        p.op_date,
        2                      AS op_type, -- Оплата
        20                     AS ord,
		p.amount_paid AS amount,
        p.sort_ts
    FROM payments_split p	
)

SELECT * FROM operations ORDER BY id_seller, id_payer, op_date, ord, sort_ts;
