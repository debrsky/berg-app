WITH params AS (
    SELECT
        51158::integer AS p_id_payer,  -- NULL = все плательщики
        3::integer     AS p_id_boss    -- NULL = все перевозчики
),

-- 1. Счета
invoices AS (
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
),

-- 2. Оплаты + расчёт переплаты
payments_split AS (
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
            ORDER BY invp."ID"
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0)::numeric AS paid_before,
        i.amount AS invoice_amount,
        GREATEST(0::numeric,
                 ROUND(invp."Cost"::numeric, 2) - 
                 (i.amount - COALESCE(SUM(invp."Cost") OVER (
                     PARTITION BY invp."ID_XInvoice"
                     ORDER BY invp."ID"
                     ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                 ), 0))
        ) AS avans_from_this_payment
    FROM berg."XInvoicePays" invp
    JOIN berg."XInvoices" inv ON inv."ID" = invp."ID_XInvoice"
    JOIN berg."Applications" app ON app."ID" = inv."ID_Application"
    JOIN invoices i ON i.id_invoice = inv."ID"
    CROSS JOIN params p
    WHERE inv."Nomer" IS NOT NULL AND inv."Nomer" > 0
      AND (p.p_id_payer IS NULL OR app."ID_CustomerPay" = p.p_id_payer)
      AND (p.p_id_boss   IS NULL OR inv."ID_Boss" = p.p_id_boss)
),

-- 3. Реальные операции
real_ops AS (
    -- Начисления
    SELECT
        id_boss,
        id_payer,
        id_invoice,
        op_date,
        sort_ts                AS sort_key,
        10                     AS ord,
        'Начисление'::text     AS operation,
        descr                  AS description,
        memo,
        amount                 AS debit,
        0::numeric             AS credit,
        0::numeric             AS avans_added,
        0::numeric             AS avans_used
    FROM invoices

    UNION ALL

    -- Оплаты — вся сумма в credit
    SELECT
        p.id_boss,
        p.id_payer,
        p.id_invoice,
        p.op_date,
        p.sort_ts,
        20,
        'Оплата счёта'::text,
        'Оплата счёта №' || p.invoice_nomer ||
            CASE WHEN p."IsCash" THEN ' (наличные)' ELSE ' (безнал)' END,
        NULL::text,
        0::numeric,
        p.amount_paid          AS credit,
        p.avans_from_this_payment AS avans_added,
        0::numeric AS avans_used
    FROM payments_split p
),

-- 4. Сальдо
real_ops_with_balance AS (
    SELECT *,
           SUM(debit - credit) OVER (
               PARTITION BY id_payer
               ORDER BY op_date, sort_key, ord
               ROWS UNBOUNDED PRECEDING
           ) AS balance_after
    FROM real_ops
),

-- 5. Баланс до начисления
invoice_prev_balance AS (
    SELECT DISTINCT ON (i.id_invoice)
        i.id_invoice,
        i.id_boss,
        i.id_payer,
        i.op_date,
        i.amount,
        COALESCE((
            SELECT balance_after
            FROM real_ops_with_balance r
            WHERE r.id_payer = i.id_payer
              AND (r.op_date < i.op_date OR (r.op_date = i.op_date AND r.sort_key < i.sort_ts))
            ORDER BY r.op_date DESC, r.sort_key DESC, r.ord DESC
            LIMIT 1
        ), 0)::numeric AS balance_before
    FROM invoices i
)

-- ФИНАЛЬНЫЙ ЧИСТЫЙ ВЫВОД
SELECT
    op_date,
    id_boss,
    id_payer,
    id_invoice,
    operation,
    description,
    memo,
    debit::numeric(12,2)           AS debit,
    credit::numeric(12,2)          AS credit,
    avans_added::numeric(12,2)     AS avans_added,    -- поступило на лицевой счёт (переплата)
    avans_used::numeric(12,2)      AS avans_used,     -- зачтено из аванса в счёт
    balance_after::numeric(12,2)   AS balance_after
FROM (
    -- Основные операции
    SELECT
        op_date,
        id_boss,
        id_payer,
        id_invoice,
        operation,
        description,
        memo,
        debit,
        credit,
        avans_added,
        avans_used,
        sort_key,
        ord,
        balance_after,
        1 AS section
    FROM real_ops_with_balance

    UNION ALL

    -- Зачёт аванса (информационная строка)
    SELECT
        i.op_date,
        ipb.id_boss,
        ipb.id_payer,
        ipb.id_invoice,
        'Зачёт аванса'::text,
        'Зачёт предоплаты в счёт №' || xinv."Nomer" ||
            ' на сумму ' || TO_CHAR(LEAST(ipb.amount, -ipb.balance_before), 'FM999G999G990D00'),
        'Автоматическое погашение долга авансом',
        0::numeric,
        0::numeric,
        0::numeric,
        LEAST(ipb.amount, -ipb.balance_before),
        i.sort_ts + INTERVAL '0.5 microsecond',
        15,
        r.balance_after,
        2
    FROM invoice_prev_balance ipb
    JOIN invoices i ON i.id_invoice = ipb.id_invoice
    JOIN berg."XInvoices" xinv ON xinv."ID" = ipb.id_invoice
    JOIN real_ops_with_balance r 
      ON r.id_payer = ipb.id_payer 
     AND r.op_date = i.op_date 
     AND r.operation = 'Начисление'
    WHERE ipb.balance_before < 0
) t
ORDER BY op_date, sort_key, ord, section;
