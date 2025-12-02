/*

Доработать операции по зачету предоплаты, сейчас не все правильно считает


*/

-- FUNCTION: public.get_financial_operations(integer, integer)

DROP FUNCTION IF EXISTS public.get_financial_operations(integer, integer);

CREATE OR REPLACE FUNCTION public.get_financial_operations(
	p_id_payer integer DEFAULT NULL::integer,
	p_id_seller integer DEFAULT NULL::integer)
    RETURNS TABLE(op_seq_num integer, id_seller integer, id_payer integer, id_invoice integer, op_date date, 
	sort_ts timestamp without time zone, op_type integer, ord integer, inv_amount numeric, pre_inv_balance numeric, 
	op_amount numeric, 
	balance_before numeric,
	charge_amount numeric, payment_amount numeric, 
	balance_after numeric,
	prepayment_before numeric,
	prepayment_added numeric,
	prepayment_applied numeric,
	prepayment_after numeric,

	inv_balance_before numeric,
	inv_balance_after numeric,

	debt_total_before numeric,
	debt_total_after numeric,
	debt_invoices_before jsonb,
	debt_invoices jsonb,
	
	balance numeric, invoice_balance numeric, avans_added numeric, avans_used numeric, avans_balance numeric, invoices_total_debt numeric, invoices_state jsonb) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
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
                f_amount AS op_amount,
                f_amount AS charge_amount,
                0::numeric AS payment_amount
            FROM invoices
            UNION ALL
            -- Оплаты
            SELECT
                f_id_seller, f_id_payer, f_id_invoice, f_op_date, f_sort_ts,
                2 AS op_type, 20 AS ord,
                f_inv_amount AS inv_amount,
                f_amount_paid AS op_amount,
                0::numeric AS charge_amount,
                f_amount_paid AS payment_amount
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
    row_counter bigint := 0;
    
    invoice_balances jsonb := '{}'::jsonb;
    invoice_key text;
    avans_added_local numeric;
    avans_used_local numeric;
    charge_amount_local numeric;
    payment_amount_local numeric;
    
    debt_rec RECORD;
    offset_amount numeric;
    offset_seq integer := 0;
   
    curr_inv_balance numeric;
    inv_date_ts timestamp;
BEGIN
    RAISE NOTICE 'Функция вызвана с p_id_payer=%, p_id_seller=%', p_id_payer, p_id_seller;
    
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
			invoices_total_debt := 0;
            offset_seq := 0;
            row_counter := 0;
        END IF;

		debt_invoices_before := invoice_balances;
		debt_total_before := invoices_total_debt;
		
		balance_before := current_balance;
		prepayment_before := current_avans_balance;
			   
        invoice_key := rec.f_id_invoice::text;
       
        curr_inv_balance := COALESCE((invoice_balances -> invoice_key ->> 'bal')::numeric, 0);
        inv_date_ts := COALESCE((invoice_balances -> invoice_key ->> 'date')::timestamp, rec.f_sort_ts);

		inv_balance_before := curr_inv_balance;
		pre_inv_balance := curr_inv_balance;
       
        avans_added_local := 0;
        avans_used_local := 0;
        charge_amount_local := rec.charge_amount;
        payment_amount_local := rec.payment_amount;
       
        -------------------------------------------------------------------
        -- Основная логика
        -------------------------------------------------------------------
		/******************************************************************
			Платеж
			------
			Для оплаты используются:
			pre_inv_balance -- баланс по счету до оплаты (долг отрицательный)
			op_amount -- сумма платежа

			Если сумма платежа больше величины долга по счету,
			т.е. если (op_amount + pre_inv_balance > 0), то
			платеж делим на части, одная часть идет в зачет по долгу по счету
			(payment_amount_local := -pre_inv_balance), а вторая часть идет в аванс 
			(avans_added_local := op_amount - payment_amount_local).

			Иначе, если сумма платежа меньше или равна величине долга по счету, 
			то вся сумма платежа идет в зачет по долгу по счету
			(payment_amount_local := rec.op_amount)
		*********************************************************************/ 
        IF rec.op_type = 2 THEN -- Платеж
            IF rec.op_amount + pre_inv_balance > 0 THEN
				payment_amount_local := -pre_inv_balance;
				avans_added_local := rec.op_amount - payment_amount_local;
			
                curr_inv_balance := 0;
            ELSE
				payment_amount_local := rec.op_amount;
				
                curr_inv_balance := pre_inv_balance + rec.op_amount;
            END IF;
			
            current_balance := current_balance + rec.op_amount; 
			
           
		/******************************************************************
			Начисление
		*********************************************************************/ 
        ELSIF rec.op_type = 1 THEN -- Начисление
            inv_date_ts := rec.f_sort_ts;
            charge_amount_local := rec.op_amount;
           
			IF current_avans_balance > 0 THEN
                avans_used_local := LEAST(current_avans_balance, rec.op_amount);
	            curr_inv_balance := pre_inv_balance - rec.op_amount + avans_used_local;
			ELSE 
				curr_inv_balance := pre_inv_balance - rec.op_amount;
            END IF;
			current_balance := current_balance - rec.op_amount;
        END IF;
       
        current_avans_balance := current_avans_balance + avans_added_local - avans_used_local;
       
		invoice_balances := jsonb_set(
			invoice_balances,
			ARRAY[invoice_key],
			jsonb_build_object('bal', curr_inv_balance, 'date', inv_date_ts)
		);
		
		invoice_balances := (
			SELECT jsonb_object_agg(key, value)
			FROM jsonb_each(invoice_balances) AS t(key, value)
			WHERE (value->>'bal')::numeric <> 0
		);

		invoice_balances := COALESCE(invoice_balances, '{}'::jsonb);

		invoices_total_debt := (
			SELECT COALESCE(SUM((value->>'bal')::numeric), 0)
			FROM jsonb_each(invoice_balances)
		);

		-------------------------------------------------------------------
        -- 1. ОСНОВНАЯ СТРОКА
        -------------------------------------------------------------------
        row_counter := row_counter + 1;

        op_seq_num := row_counter;
        id_seller := rec.f_id_seller;
        id_payer := rec.f_id_payer;
        id_invoice := rec.f_id_invoice;
        op_date := rec.f_op_date;
        sort_ts := rec.f_sort_ts;
        op_type := rec.op_type;
        ord := rec.ord;
        inv_amount := rec.inv_amount;
		pre_inv_balance := pre_inv_balance;
		op_amount := rec.op_amount;
		balance_before := balance_before;
        charge_amount := charge_amount_local;
        payment_amount := payment_amount_local;
		balance_after := current_balance;
		prepayment_before := prepayment_before;
		prepayment_added := avans_added_local;
		prepayment_applied := avans_used_local;
		prepayment_after := current_avans_balance;
		inv_balance_before := inv_balance_before;
		inv_balance_after := curr_inv_balance;
		debt_total_before := debt_total_before;
		debt_total_after := invoices_total_debt;
		debt_invoices := invoice_balances;
		
        balance := current_balance;
        invoice_balance := curr_inv_balance;
        avans_added := avans_added_local;
        avans_used := avans_used_local;
        avans_balance := current_avans_balance;
		invoices_state := invoice_balances;
		invoices_total_debt := invoices_total_debt;
       
        RETURN NEXT;

        -- Проверка целостности после основной строки
        IF current_balance > 0 AND current_avans_balance <> current_balance THEN
            RAISE NOTICE 'Нарушение баланса аванса! Строка %: balance = %, avans_balance = % (должны быть равны при положительном балансе)',
                row_counter, current_balance, current_avans_balance;
        END IF;
       
        -------------------------------------------------------------------
        -- 2. Зачёт аванса при начислении
        -------------------------------------------------------------------
		IF false AND rec.op_type = 1 AND current_avans_balance > 0 THEN
			
			avans_used_local := LEAST(rec.op_amount, current_avans_balance);
			current_avans_balance := current_avans_balance - avans_used_local;
			
			offset_seq := offset_seq + 1;
			row_counter := row_counter + 1;

	        IF curr_inv_balance = 0 THEN
				invoice_balances := invoice_balances - invoice_key;
			ELSE
				invoice_balances := jsonb_set(
		            invoice_balances,
		            ARRAY[invoice_key],
		            jsonb_build_object('bal', curr_inv_balance, 'date', inv_date_ts)
		        );
				
				invoice_balances := (
		            SELECT jsonb_object_agg(key, value)
		            FROM jsonb_each(invoice_balances) AS t(key, value)
		            WHERE (value->>'bal')::numeric <> 0
		        );
		
				invoice_balances := COALESCE(invoice_balances, '{}'::jsonb);
		
				invoices_total_debt := (
				    SELECT COALESCE(SUM((value->>'bal')::numeric), 0)
				    FROM jsonb_each(invoice_balances)
				);
			END IF;
			
			op_seq_num := row_counter;
            id_seller := rec.f_id_seller;
            id_payer := rec.f_id_payer;
            id_invoice := rec.f_id_invoice;
            op_date := rec.f_op_date;
            sort_ts := rec.f_sort_ts + (offset_seq || ' microseconds')::interval;
            op_type := 3;
            ord := 15;
            inv_amount := rec.inv_amount;
			pre_inv_balance := pre_inv_balance;
			op_amount := rec.op_amount;
            charge_amount := rec.op_amount;
            payment_amount := 0;
			balance_after := current_balance;
			prepayment_before := prepayment_before;
			prepayment_added := avans_added_local;
			prepayment_applied := avans_used_local;
			prepayment_after := current_avans_balance;
			inv_balance_before := inv_balance_before;
			inv_balance_after := curr_inv_balance;
			debt_total_before := debt_total_before;
			debt_total_after := invoices_total_debt;
			debt_invoices := invoice_balances;
			
			balance := current_balance;
            invoice_balance := curr_inv_balance;
            avans_added := 0;
            avans_used := avans_used_local;
            avans_balance := current_avans_balance;
			invoices_state := invoice_balances;
			invoices_total_debt := invoices_total_debt;
           
            RETURN NEXT;

            -- Проверка после зачёта
            IF current_balance > 0 AND current_avans_balance <> current_balance THEN
                RAISE NOTICE 'Нарушение баланса аванса после зачёта при начислении! Строка %: balance = %, avans_balance = %',
                    row_counter, current_balance, current_avans_balance;
            END IF;
        END IF;
       
        -------------------------------------------------------------------
        -- 3. Автозачёт аванса после оплаты
        -------------------------------------------------------------------
        IF rec.op_type = 2 AND current_avans_balance > 0 THEN
            offset_seq := offset_seq + 1;
           
            FOR debt_rec IN
                SELECT
                    key::integer AS invoice_id,
                    (value->>'bal')::numeric AS debt_bal,
                    (value->>'date')::timestamp AS inv_date
                FROM jsonb_each(invoice_balances)
                WHERE (value->>'bal')::numeric < 0
                ORDER BY (value->>'date')::timestamp ASC
            LOOP
                EXIT WHEN current_avans_balance <= 0;
               
                offset_amount := LEAST(current_avans_balance, -debt_rec.debt_bal);
                current_avans_balance := current_avans_balance - offset_amount;
               
                row_counter := row_counter + 1;

				invoice_balances := jsonb_set(
					invoice_balances,
					ARRAY[debt_rec.invoice_id::text],
					jsonb_build_object('bal', debt_rec.debt_bal + offset_amount, 'date', debt_rec.inv_date)
				);

				invoice_balances := (
					SELECT jsonb_object_agg(key, value)
					FROM jsonb_each(invoice_balances) AS t(key, value)
					WHERE (value->>'bal')::numeric <> 0
				);
		
				invoice_balances := COALESCE(invoice_balances, '{}'::jsonb);
		
				invoices_total_debt := (
					SELECT COALESCE(SUM((value->>'bal')::numeric), 0)
					FROM jsonb_each(invoice_balances)
				);					
	
				op_seq_num := row_counter;
                id_seller := rec.f_id_seller;
                id_payer := rec.f_id_payer;
                id_invoice := debt_rec.invoice_id;
                op_date := rec.f_op_date;
                sort_ts := rec.f_sort_ts + (offset_seq || ' microseconds')::interval;
                op_type := 3;
                ord := 25;
                inv_amount := 0;
				pre_inv_balance := pre_inv_balance;
	    		op_amount := rec.op_amount;
	            charge_amount := 0;
                payment_amount := offset_amount;
	    		balance_after := current_balance;
				prepayment_before := prepayment_before;
				prepayment_added := 0;
				prepayment_applied := offset_amount;
				prepayment_after := current_avans_balance;
				inv_balance_before := inv_balance_before;
				inv_balance_after := curr_inv_balance;
				debt_total_before := debt_total_before;
				debt_total_after := invoices_total_debt;
				debt_invoices := invoice_balances;
				
	            balance := current_balance;
                invoice_balance := debt_rec.debt_bal + offset_amount;
                avans_added := 0;
                avans_used := offset_amount;
                avans_balance := current_avans_balance;
				invoices_state := invoice_balances;
	    		invoices_total_debt := invoices_total_debt;
           
                RETURN NEXT;
        
               
                offset_seq := offset_seq + 1;

                -- Проверка после каждого автозачёта
                IF current_balance > 0 AND current_avans_balance <> current_balance THEN
                    RAISE NOTICE 'Нарушение баланса аванса после автозачёта! Строка % (счёт %): balance = %, avans_balance = %',
                        row_counter, debt_rec.invoice_id, current_balance, current_avans_balance;
                END IF;
            END LOOP;
        END IF;
       
       
        prev_seller := rec.f_id_seller;
        prev_payer := rec.f_id_payer;
    END LOOP;
   
    CLOSE cur;
    RETURN;
END;
$BODY$;

ALTER FUNCTION public.get_financial_operations(integer, integer)
    OWNER TO postgres;

