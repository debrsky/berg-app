CREATE OR REPLACE FUNCTION get_financial_operations(
    p_id_payer integer DEFAULT NULL,
    p_id_seller integer DEFAULT NULL
)
RETURNS TABLE(
    id_seller integer,
    id_payer integer,
    id_invoice integer,
    op_date date,
    sort_ts timestamp,
    op_type integer,
    ord integer,
    inv_amount numeric,
    debit numeric,
    credit numeric,
    balance numeric,
    invoice_balance numeric,
    avans_added numeric,
    avans_used numeric,
    avans_balance numeric
)
LANGUAGE plpgsql
AS $$
DECLARE
    cur CURSOR FOR
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
                0::numeric AS debit,
                f_amount AS credit
            FROM invoices
            UNION ALL
            -- Оплаты
            SELECT
                f_id_seller, f_id_payer, f_id_invoice, f_op_date, f_sort_ts,
                2 AS op_type, 20 AS ord,
                f_inv_amount AS inv_amount,
                f_amount_paid AS debit,
                0::numeric AS credit
            FROM payments
        )
        SELECT *
        FROM operations
        ORDER BY f_id_seller, f_id_payer, f_sort_ts, ord;
    
    rec RECORD;
    -- Состояние
    current_balance numeric := 0;
    current_avans_balance numeric := 0;
    prev_seller integer := NULL;
    prev_payer integer := NULL;
    -- Храним для каждого счёта: {остаток, дата_начисления}
    invoice_balances jsonb := '{}'::jsonb;
    invoice_key text;
    avans_added_local numeric;
    avans_used_local numeric;
    debit_local numeric;
    -- Для зачёта аванса
    debt_rec RECORD;
    offset_amount numeric;
    offset_seq integer := 0;
    
    -- Локальные переменные для обработки счета
    curr_inv_balance numeric;
    inv_date_ts timestamp;
BEGIN
    OPEN cur;
    LOOP
        FETCH cur INTO rec;
        EXIT WHEN NOT FOUND;
        
        -- Сброс при смене контрагента
        IF prev_seller IS DISTINCT FROM rec.f_id_seller
           OR prev_payer IS DISTINCT FROM rec.f_id_payer THEN
            current_balance := 0;
            current_avans_balance := 0;
            invoice_balances := '{}'::jsonb;
            offset_seq := 0;
        END IF;
        
        invoice_key := rec.f_id_invoice::text;
        
        -- Текущий остаток по счёту до операции
        curr_inv_balance := COALESCE((invoice_balances -> invoice_key ->> 'bal')::numeric, 0);
        inv_date_ts := COALESCE((invoice_balances -> invoice_key ->> 'date')::timestamp, rec.f_sort_ts);
        
        avans_added_local := 0;
        avans_used_local := 0;
        debit_local := rec.debit;
        
        IF rec.op_type = 2 THEN
            IF curr_inv_balance + rec.debit > 0 THEN
                avans_added_local := curr_inv_balance + rec.debit;
                curr_inv_balance := 0;
                debit_local := rec.debit - avans_added_local;
            ELSE
                curr_inv_balance := curr_inv_balance + rec.debit;
            END IF;
            current_balance := current_balance + rec.debit;
        ELSIF rec.op_type = 1 THEN
            -- Если это начисление — сохраняем дату счёта (один раз)
            inv_date_ts := rec.f_sort_ts;
            current_balance := current_balance - rec.credit;
            curr_inv_balance := curr_inv_balance - rec.credit;
        END IF;

		current_avans_balance = current_avans_balance + avans_added_local - avans_used_local;
        
        -- Основная строка операции
        id_seller := rec.f_id_seller;
        id_payer := rec.f_id_payer;
        id_invoice := rec.f_id_invoice;
        op_date := rec.f_op_date;
        sort_ts := rec.f_sort_ts;
        op_type := rec.op_type;
        ord := rec.ord;
        inv_amount := rec.inv_amount;
        debit := debit_local;
        credit := rec.credit;
        balance := current_balance;
        invoice_balance := curr_inv_balance;
        avans_added := avans_added_local;
        avans_used := avans_used_local;
        avans_balance := current_avans_balance;
        
        RETURN NEXT;
        
        -------------------------------------------------------------------
        -- АВТОМАТИЧЕСКИЙ ЗАЧЁТ АВАНСА ПОСЛЕ ОПЛАТЫ
        -------------------------------------------------------------------
        IF false AND rec.op_type = 2 AND current_avans_balance > 0 THEN
            offset_seq := offset_seq + 1;
            
            -- Перебираем все счета с долгом, отсортированные по дате начисления
            FOR debt_rec IN
                SELECT
                    key::integer AS invoice_id,
                    (value->>'bal')::numeric AS debt_bal,
                    (value->>'date')::timestamp AS inv_date
                FROM jsonb_each(invoice_balances)
                WHERE (value->>'bal')::numeric < 0
                ORDER BY (value->>'date')::timestamp ASC
            LOOP
                IF current_balance <= 0 THEN EXIT; END IF;
                
                offset_amount := LEAST(current_balance, -debt_rec.debt_bal);
                
                -- Строка зачёта аванса
                id_seller := rec.f_id_seller;
                id_payer := rec.f_id_payer;
                id_invoice := debt_rec.invoice_id;
                op_date := rec.f_op_date;
                sort_ts := rec.f_sort_ts + (offset_seq || ' microseconds')::interval;
                op_type := 3;
                ord := 25;
                inv_amount := 0;
                debit := offset_amount;
                credit := 0;
                balance := current_balance;
                invoice_balance := debt_rec.debt_bal + offset_amount;
                avans_added := 0;
                avans_used := offset_amount;
                avans_balance := current_avans_balance - offset_amount;
                
                RETURN NEXT;
                
                -- Обновляем состояние
                current_balance := current_balance - offset_amount;
                current_avans_balance := current_avans_balance - offset_amount;
                
                -- Обновляем баланс счёта в jsonb
                invoice_balances := jsonb_set(
                    invoice_balances,
                    ARRAY[debt_rec.invoice_id::text],
                    jsonb_build_object(
                        'bal', debt_rec.debt_bal + offset_amount,
                        'date', debt_rec.inv_date
                    )
                );
                
                offset_seq := offset_seq + 1;
            END LOOP;
        END IF;
        
        -- Сохраняем/обновляем текущий счёт в invoice_balances
        invoice_balances := jsonb_set(
            invoice_balances,
            ARRAY[invoice_key],
            jsonb_build_object(
                'bal', curr_inv_balance,
                'date', inv_date_ts
            )
        );
        
        prev_seller := rec.f_id_seller;
        prev_payer := rec.f_id_payer;
    END LOOP;
    
    CLOSE cur;
    
    RETURN;
END;
$$;