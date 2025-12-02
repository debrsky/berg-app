-- Оптимизированная версия get_financial_operations
DROP FUNCTION IF EXISTS public.get_financial_operations(integer, integer);

CREATE OR REPLACE FUNCTION public.get_financial_operations(
    p_id_payer integer DEFAULT NULL::integer,
    p_id_seller integer DEFAULT NULL::integer)
RETURNS TABLE(
    row_num integer,
    id_seller integer,
    id_payer integer,
    id_invoice integer,
    op_date date,
    sort_ts timestamp without time zone,
    op_type integer,
    ord integer,
    inv_amount numeric,
    pre_inv_balance numeric,
    op_amount numeric,
    debit numeric,
    credit numeric,
    balance numeric,
    invoice_balance numeric,
    avans_added numeric,
    avans_used numeric,
    avans_balance numeric,
    invoices_total_debt numeric,
    invoices_state jsonb
)
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;

    -- агрегированные состояния по контрагенту
    current_balance numeric := 0;
    current_avans_balance numeric := 0;
    prev_seller integer := NULL;
    prev_payer integer := NULL;
    row_counter bigint := 0;

    invoice_balances jsonb := '{}'::jsonb;   -- { invoice_id : { bal: ..., date: ... }, ... }
    invoices_total numeric := 0;             -- инкрементальное значение суммы балансов (тот же смысл, что и invoices_total_debt)

    -- временные переменные
    invoice_key text;
    old_inv_bal numeric;
    curr_inv_balance numeric;
    inv_date_ts timestamp;

    avans_added_local numeric;
    avans_used_local numeric;
    debit_local numeric;
    credit_local numeric;

    debt_rec RECORD;
    offset_amount numeric;
    offset_seq integer := 0;
BEGIN
    -- Для продакшна убрать уведомление или оставить опционально:
    -- RAISE NOTICE 'get_financial_operations called with payer=%, seller=%', p_id_payer, p_id_seller;

    FOR rec IN
        WITH invoices AS (
            SELECT
                inv."ID" AS f_id_invoice,
                inv."ID_Boss" AS f_id_seller,
                app."ID_CustomerPay" AS f_id_payer,
                inv."Date"::date AS f_op_date,
                inv."Date" AS f_sort_ts,
                ROUND(inv."Cost"::numeric, 2) AS f_amount,
                ROUND(inv."Cost"::numeric, 2) AS f_inv_amount
            FROM berg."XInvoices" inv
            JOIN berg."Applications" app ON app."ID" = inv."ID_Application"
            WHERE inv."Nomer" IS NOT NULL AND inv."Nomer" > 0
              AND (p_id_payer IS NULL OR app."ID_CustomerPay" = p_id_payer)
              AND (p_id_seller IS NULL OR inv."ID_Boss" = p_id_seller)
        ),
        payments AS (
            SELECT
                invp."ID_XInvoice" AS f_id_invoice,
                inv."ID_Boss" AS f_id_seller,
                app."ID_CustomerPay" AS f_id_payer,
                invp."Date"::date AS f_op_date,
                invp."Date" AS f_sort_ts,
                ROUND(invp."Cost"::numeric, 2) AS f_amount_paid,
                ROUND(inv."Cost"::numeric, 2) AS f_inv_amount
            FROM berg."XInvoicePays" invp
            JOIN berg."XInvoices" inv ON inv."ID" = invp."ID_XInvoice"
            JOIN berg."Applications" app ON app."ID" = inv."ID_Application"
            WHERE inv."Nomer" IS NOT NULL AND inv."Nomer" > 0
              AND (p_id_payer IS NULL OR app."ID_CustomerPay" = p_id_payer)
              AND (p_id_seller IS NULL OR inv."ID_Boss" = p_id_seller)
        ),
        operations AS (
            -- Начисления
            SELECT
                f_id_seller, f_id_payer, f_id_invoice, f_op_date, f_sort_ts,
                1 AS op_type, 10 AS ord,
                f_inv_amount AS inv_amount,
                f_amount AS op_amount,
                0::numeric AS debit,
                f_amount AS credit
            FROM invoices
            UNION ALL
            -- Оплаты
            SELECT
                f_id_seller, f_id_payer, f_id_invoice, f_op_date, f_sort_ts,
                2 AS op_type, 20 AS ord,
                f_inv_amount AS inv_amount,
                f_amount_paid AS op_amount,
                f_amount_paid AS debit,
                0::numeric AS credit
            FROM payments
        )
        SELECT *
        FROM operations
        ORDER BY f_id_seller, f_id_payer, f_sort_ts, ord
    LOOP
        -- reset state when new (seller,payer) encountered
        IF prev_seller IS DISTINCT FROM rec.f_id_seller OR prev_payer IS DISTINCT FROM rec.f_id_payer THEN
            current_balance := 0;
            current_avans_balance := 0;
            invoice_balances := '{}'::jsonb;
            invoices_total := 0;
            offset_seq := 0;
            row_counter := 0;
        END IF;

        invoice_key := rec.f_id_invoice::text;

        -- old balance for invoice (before applying current op)
        old_inv_bal := COALESCE( (invoice_balances -> invoice_key ->> 'bal')::numeric, 0 );
        inv_date_ts := COALESCE( (invoice_balances -> invoice_key ->> 'date')::timestamp, rec.f_sort_ts );

        curr_inv_balance := old_inv_bal;

        avans_added_local := 0;
        avans_used_local := 0;
        debit_local := rec.debit;
        credit_local := rec.credit;

        -- Основная логика: платеж / начисление
        IF rec.op_type = 2 THEN
            -- Платеж
            IF rec.op_amount + old_inv_bal > 0 THEN
                -- платеж превышает долг по счёту -> часть в зачет долга, остальное в аванс
                debit_local := - old_inv_bal;
                avans_added_local := rec.op_amount - debit_local;
                curr_inv_balance := 0;
            ELSE
                -- платеж полностью покрывает часть долга
                debit_local := rec.op_amount;
                curr_inv_balance := old_inv_bal + rec.op_amount;
            END IF;

            current_balance := current_balance + rec.op_amount;

        ELSIF rec.op_type = 1 THEN
            -- Начисление
            inv_date_ts := rec.f_sort_ts;
            IF current_avans_balance > 0 THEN
                avans_used_local := LEAST(current_avans_balance, rec.op_amount);
                credit_local := rec.op_amount;
                curr_inv_balance := old_inv_bal - rec.op_amount + avans_used_local;
            ELSE
                curr_inv_balance := old_inv_bal - rec.op_amount;
            END IF;

            current_balance := current_balance - rec.op_amount;
        END IF;

        -- обновляем аванс
        current_avans_balance := current_avans_balance + avans_added_local - avans_used_local;

        -- обновляем invoice_balances и invoices_total инкрементально (вместо пересчёта jsonb_each)
        IF curr_inv_balance = 0 THEN
            -- если раньше был ненулевой — нужно вычесть старое значение из invoices_total
            IF old_inv_bal <> 0 THEN
                invoices_total := invoices_total - old_inv_bal;
                invoice_balances := invoice_balances - invoice_key;
            END IF;
        ELSE
            -- ставим новое значение (замена или добавление)
            invoice_balances := jsonb_set(
                invoice_balances,
                ARRAY[invoice_key],
                jsonb_build_object('bal', curr_inv_balance, 'date', inv_date_ts),
                true
            );
            -- обновляем invoices_total: вычитаем старое, добавляем новое
            invoices_total := invoices_total - old_inv_bal + curr_inv_balance;
        END IF;

        -- 1. Основная строка
        row_counter := row_counter + 1;

        row_num := row_counter;
        id_seller := rec.f_id_seller;
        id_payer := rec.f_id_payer;
        id_invoice := rec.f_id_invoice;
        op_date := rec.f_op_date;
        sort_ts := rec.f_sort_ts;
        op_type := rec.op_type;
        ord := rec.ord;
        inv_amount := rec.inv_amount;
        pre_inv_balance := old_inv_bal;
        op_amount := rec.op_amount;
        debit := debit_local;
        credit := credit_local;
        balance := current_balance;
        invoice_balance := curr_inv_balance;
        avans_added := avans_added_local;
        avans_used := avans_used_local;
        avans_balance := current_avans_balance;
        invoices_state := invoice_balances;
        invoices_total_debt := invoices_total;

        RETURN NEXT;

        -- контрольный послетовый чек (можно раскомментировать для отладки)
        -- IF current_balance > 0 AND current_avans_balance <> current_balance THEN
        --     RAISE NOTICE 'Баланс аванса расхождение: row %, balance=%, avans=%', row_counter, current_balance, current_avans_balance;
        -- END IF;

        -- 3. Автозачёт аванса после оплаты (проход по существующим долгам)
        IF rec.op_type = 2 AND current_avans_balance > 0 THEN
            offset_seq := offset_seq + 1;

            FOR debt_rec IN
                SELECT
                    (key)::integer AS invoice_id,
                    (value->>'bal')::numeric AS debt_bal,
                    (value->>'date')::timestamp AS inv_date
                FROM jsonb_each(invoice_balances)
                WHERE (value->>'bal')::numeric < 0
                ORDER BY (value->>'date')::timestamp ASC
            LOOP
                EXIT WHEN current_avans_balance <= 0;

                offset_amount := LEAST(current_avans_balance, - debt_rec.debt_bal);

                -- обновим invoice_balances и invoices_total инкрементально
                old_inv_bal := (invoice_balances -> debt_rec.invoice_id::text ->> 'bal')::numeric;
                -- новый баланс для этого счета после зачета
                -- debt_bal уже содержит old_inv_bal, поэтому:
                curr_inv_balance := debt_rec.debt_bal + offset_amount;

                -- записываем в jsonb
                IF curr_inv_balance = 0 THEN
                    invoice_balances := invoice_balances - debt_rec.invoice_id::text;
                ELSE
                    invoice_balances := jsonb_set(
                        invoice_balances,
                        ARRAY[debt_rec.invoice_id::text],
                        jsonb_build_object('bal', curr_inv_balance, 'date', debt_rec.inv_date),
                        true
                    );
                END IF;

                -- invoices_total инкрементально
                invoices_total := invoices_total - old_inv_bal + curr_inv_balance;

                -- уменьшаем аванс
                current_avans_balance := current_avans_balance - offset_amount;

                -- дополнительная строка: автозачет
                row_counter := row_counter + 1;
                row_num := row_counter;
                id_seller := rec.f_id_seller;
                id_payer := rec.f_id_payer;
                id_invoice := debt_rec.invoice_id;
                op_date := rec.f_op_date;
                sort_ts := rec.f_sort_ts + (offset_seq || ' microseconds')::interval;
                op_type := 3;
                ord := 25;
                inv_amount := 0;
                pre_inv_balance := (old_inv_bal); -- баланс ДО зачисления
                op_amount := rec.op_amount;
                debit := offset_amount;
                credit := 0;
                balance := current_balance;
                invoice_balance := curr_inv_balance;
                avans_added := 0;
                avans_used := offset_amount;
                avans_balance := current_avans_balance;
                invoices_state := invoice_balances;
                invoices_total_debt := invoices_total;

                RETURN NEXT;

                offset_seq := offset_seq + 1;

                -- контроль после автозачёта (опционально)
                -- IF current_balance > 0 AND current_avans_balance <> current_balance THEN
                --     RAISE NOTICE 'Нарушение баланса после автозачёта: row %, inv %, balance=%, avans=%',
                --         row_counter, debt_rec.invoice_id, current_balance, current_avans_balance;
                -- END IF;
            END LOOP;
        END IF;

        prev_seller := rec.f_id_seller;
        prev_payer := rec.f_id_payer;
    END LOOP;

    RETURN;
END;
$$;

ALTER FUNCTION public.get_financial_operations(integer, integer) OWNER TO postgres;
