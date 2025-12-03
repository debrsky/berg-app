-- FUNCTION: public.get_operations(integer, integer)

DROP FUNCTION IF EXISTS public.get_operations(integer, integer);

CREATE OR REPLACE FUNCTION public.get_operations(
    p_id_payer integer DEFAULT NULL::integer,
    p_id_seller integer DEFAULT NULL::integer)
    RETURNS TABLE(
        op_seq_num integer, -- порядковый номер операции по плательщикам
        id_seller integer, -- продавец
        id_payer integer, -- покупатель
        op_date date, -- дата операции
        op_date_ts timestamp without time zone, -- timestamp операции
        op_type integer, -- тип операции: 1 - начисление, 2 - оплата, 3 - зачет предоплаты

		id_invoice integer, -- счет
        inv_amount numeric, -- сумма по счету, к которому привязана операция
        op_amount numeric, -- сумма операции
        
        balance_before numeric, -- накопленный долг за услуги по плательщику перед операцией
        charge_amount numeric, -- начислено за услуги
        payment_amount numeric, -- оплачено за услуги
        balance_after numeric, -- накопленный долг за услуги по плательщику после операции
        
        prepayment_before numeric, -- накопленная предоплата по плательщику перед операцией
        prepayment_added numeric, -- зачислена предоплата
        prepayment_applied numeric, -- зачтена предоплата
        prepayment_after numeric, -- накопленная предоплата по плательщику после операции
        
        inv_debt_before numeric, -- долг по счету перед операцией
        inv_debt_after numeric, -- долг по счету после операции
        
        debt_invoices_before jsonb, -- список всех счетов плательщика с долгами перед операцией
        debt_invoices_after jsonb -- список всех счетов плательщика с долгами после операции
	)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
    rec RECORD;
	debt_rec RECORD;

    v_op_seq_num integer := 0;

	v_id_seller integer;
	v_id_payer integer;
	v_op_date date;
	v_op_date_ts timestamp without time zone;
	v_op_type integer;
	v_id_invoice integer;
	v_inv_amount numeric;
	v_op_amount numeric;

    v_balance_before numeric := 0;
    v_charge_amount numeric := 0;
    v_payment_amount numeric := 0;
    v_balance_after numeric := 0;
    
    v_prepayment_before numeric := 0;
    v_prepayment_added numeric := 0;
    v_prepayment_applied numeric := 0;
    v_prepayment_after numeric := 0;
    
    v_inv_debt_before numeric := 0;
    v_inv_debt_after numeric := 0;
    
    v_debt_invoices_before jsonb := '{}'::jsonb;
    v_debt_invoices_after jsonb := '{}'::jsonb;


	id_invoice_key text;

	prev_id_seller integer;
	prev_id_payer integer;
BEGIN
    RAISE NOTICE 'Функция вызвана с p_id_payer=%, p_id_seller=%', p_id_payer, p_id_seller;

    FOR rec IN
        WITH invoices AS (
            SELECT
                inv."ID" AS f_id_invoice,
                inv."ID_Boss" AS f_id_seller,
                app."ID_CustomerPay" AS f_id_payer,
                inv."Date"::date AS f_op_date,
                inv."Date" AS f_op_date_ts,
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
                invp."Date" AS f_op_date_ts,
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
                f_id_seller AS id_seller, 
                f_id_payer AS id_payer, 
                f_id_invoice AS id_invoice, 
                f_op_date AS op_date, 
                f_op_date_ts as op_date_ts,
                1 AS op_type, 10 AS ord,
                f_inv_amount AS inv_amount,
                f_amount AS op_amount,
                f_amount AS charge_amount,
                0::numeric AS payment_amount
            FROM invoices
            UNION ALL
            -- Оплаты
            SELECT
                f_id_seller AS id_seller, 
                f_id_payer AS id_payer, 
                f_id_invoice AS id_invoice, 
                f_op_date AS op_date, 
                f_op_date_ts as op_date_ts,
                2 AS op_type, 20 AS ord,
                f_inv_amount AS inv_amount,
                f_amount_paid AS op_amount,
                0::numeric AS charge_amount,
                f_amount_paid AS payment_amount
            FROM payments
        )
        SELECT *
        FROM operations
        ORDER BY id_seller, id_payer, op_date, ord, op_date_ts
		
    LOOP
        -- Сброс при смене контрагента
        IF prev_id_seller IS DISTINCT FROM rec.id_seller
	        OR prev_id_payer IS DISTINCT FROM rec.id_payer THEN

			v_op_seq_num := 0;
			-- Начальные значения, чтобы не было NULL
			v_balance_after := 0;
			v_prepayment_after := 0;
			v_debt_invoices_after := '{}'::jsonb;
        END IF;
		
		v_op_seq_num := v_op_seq_num + 1;
		
		v_balance_before := v_balance_after;
		v_debt_invoices_before := v_debt_invoices_after;
		v_prepayment_before := v_prepayment_after;

		v_id_seller := rec.id_seller;
		v_id_payer := rec.id_payer;
		v_op_date := rec.op_date;
		v_op_date_ts := rec.op_date_ts;
		v_op_type := rec.op_type;
		v_id_invoice := rec.id_invoice;
		v_inv_amount := rec.inv_amount;
		v_op_amount := rec.op_amount;

		id_invoice_key := rec.id_invoice::text;
		v_inv_debt_before := COALESCE((v_debt_invoices_before -> id_invoice_key ->> 'debt')::numeric, 0);

		v_charge_amount := 0;
		v_payment_amount := 0;

		v_prepayment_added := 0;
		v_prepayment_applied := 0;

		IF rec.op_type = 1 THEN
			v_charge_amount := rec.op_amount;


		ELSEIF rec.op_type = 2 THEN
			v_payment_amount := LEAST(rec.op_amount, v_inv_debt_before);
			v_prepayment_added := rec.op_amount - v_payment_amount;

			
		END IF;

		v_balance_after := v_balance_before - v_charge_amount + v_payment_amount;
		v_inv_debt_after := v_inv_debt_before + v_charge_amount - v_payment_amount;
		v_prepayment_after := v_prepayment_before + v_prepayment_added - v_prepayment_applied;


		v_debt_invoices_after := jsonb_set(
			v_debt_invoices_before,
			ARRAY[id_invoice_key],
			jsonb_build_object('debt', v_inv_debt_after, 'date', rec.op_date_ts)
		);
		
		v_debt_invoices_after := (
			SELECT jsonb_object_agg(key, value)
			FROM jsonb_each(v_debt_invoices_after) AS t(key, value)
			WHERE (value->>'debt')::numeric <> 0
		);

		v_debt_invoices_after := COALESCE(v_debt_invoices_after, '{}'::jsonb);

		/***************************************************
			Возврат записи
		***************************************************/
		op_seq_num := v_op_seq_num;
        id_seller := v_id_seller;
        id_payer := v_id_payer;
        op_date := v_op_date;
        op_date_ts := v_op_date_ts;
        op_type := v_op_type;
        id_invoice := v_id_invoice;
        inv_amount := v_inv_amount;
        op_amount := v_op_amount;
        
        balance_before :=  v_balance_before;
        charge_amount := v_charge_amount;
        payment_amount := v_payment_amount;
        balance_after := v_balance_after;
        
        prepayment_before := v_prepayment_before;
        prepayment_added := v_prepayment_added;
        prepayment_applied := v_prepayment_applied;
        prepayment_after := v_prepayment_after;
        
        inv_debt_before := v_inv_debt_before;
        inv_debt_after := v_inv_debt_after;
        
        debt_invoices_before := v_debt_invoices_before;
        debt_invoices_after := v_debt_invoices_after;
		
        RETURN NEXT;

		/*******************************************************
			Если есть накопленная предоплата, то делаем зачет
			по счетам с долгами.
		********************************************************/
		IF (v_prepayment_after > 0) THEN
			/*
			RAISE NOTICE '=============== Зачет предоплаты =====================';
			RAISE NOTICE 'id_seller=%; id_payer=%; v_prepayment_after=%', v_id_seller, v_id_payer, v_prepayment_after;
			RAISE NOTICE '------------------------------------------------------';
			*/
            FOR debt_rec IN
                SELECT
                    key::integer AS id_invoice,
                    (value->>'debt')::numeric AS inv_debt,
                    (value->>'date')::timestamp AS inv_date_ts
                FROM jsonb_each(v_debt_invoices_after)
                WHERE (value->>'debt')::numeric > 0
                ORDER BY (value->>'date')::timestamp ASC
            LOOP
				v_op_seq_num := v_op_seq_num + 1;
				
				v_balance_before := v_balance_after;
				v_debt_invoices_before := v_debt_invoices_after;
				v_prepayment_before := v_prepayment_after;
				
				v_id_seller := rec.id_seller;
				v_id_payer := rec.id_payer;
				v_op_date := rec.op_date;
				
				v_op_date_ts := rec.op_date_ts;
				v_op_type := 0;
				v_id_invoice := 0;
				v_inv_amount := rec.inv_amount;
				v_op_amount := 0;

				v_charge_amount := 0;
				v_payment_amount := 0;
		
				v_prepayment_added := 0;
				v_prepayment_applied := 0;

				v_id_invoice := debt_rec.id_invoice;
				v_inv_amount := NULL;
		        id_invoice_key := v_id_invoice::text;
				v_inv_debt_before := debt_rec.inv_debt;
				-- v_inv_debt_before := COALESCE((v_debt_invoices_before -> id_invoice_key ->> 'debt')::numeric, 0);

				v_op_type := 3;
		
				v_op_amount := LEAST(v_inv_debt_before, v_prepayment_before);
				v_payment_amount := v_op_amount;

				v_prepayment_applied := v_op_amount;


				v_balance_after := v_balance_before - v_charge_amount + v_payment_amount;
				v_inv_debt_after := v_inv_debt_before + v_charge_amount - v_payment_amount;
				v_prepayment_after := v_prepayment_before + v_prepayment_added - v_prepayment_applied;
		
		
				v_debt_invoices_after := jsonb_set(
					v_debt_invoices_before,
					ARRAY[id_invoice_key],
					jsonb_build_object('debt', v_inv_debt_after, 'date', rec.op_date_ts)
				);
				
				v_debt_invoices_after := (
					SELECT jsonb_object_agg(key, value)
					FROM jsonb_each(v_debt_invoices_after) AS t(key, value)
					WHERE (value->>'debt')::numeric <> 0
				);
		
				v_debt_invoices_after := COALESCE(v_debt_invoices_after, '{}'::jsonb);
		
				/***************************************************
					Возврат записи
				***************************************************/
				op_seq_num := v_op_seq_num;
		        id_seller := v_id_seller;
		        id_payer := v_id_payer;
		        op_date := v_op_date;
		        op_date_ts := v_op_date_ts;
		        op_type := v_op_type;
		        id_invoice := v_id_invoice;
		        inv_amount := v_inv_amount;
		        op_amount := v_op_amount;
		        
		        balance_before :=  v_balance_before;
		        charge_amount := v_charge_amount;
		        payment_amount := v_payment_amount;
		        balance_after := v_balance_after;
		        
		        prepayment_before := v_prepayment_before;
		        prepayment_added := v_prepayment_added;
		        prepayment_applied := v_prepayment_applied;
		        prepayment_after := v_prepayment_after;
		        
		        inv_debt_before := v_inv_debt_before;
		        inv_debt_after := v_inv_debt_after;
		        
		        debt_invoices_before := v_debt_invoices_before;
		        debt_invoices_after := v_debt_invoices_after;
				
		        RETURN NEXT;

			    /*
				RAISE NOTICE 'Зачтена предоплата: id_invoice=%, debt=%, date=%, op_amount=%, v_prepayment_after=%', 
					debt_rec.id_invoice, debt_rec.inv_debt, debt_rec.inv_date_ts, op_amount, v_prepayment_after;
				*/		
						
                EXIT WHEN v_prepayment_after <= 0;
			END LOOP;
		END IF;

		prev_id_seller := rec.id_seller;
        prev_id_payer := rec.id_payer;		
    END LOOP;
    RETURN;
END;
$BODY$;

ALTER FUNCTION public.get_operations(integer, integer)
    OWNER TO postgres;

