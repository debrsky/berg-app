SELECT 
        op_seq_num, -- порядковый номер операции по плательщикам
        -- id_seller, -- продавец
        -- id_payer, -- покупатель
        op_date, -- дата операции
        -- op_date_ts, -- timestamp операции
        -- op_type, -- тип операции: 1 - начисление, 2 - оплата, 3 - зачет предоплаты

		-- id_invoice, -- счет
		CASE op_type
			WHEN 1 THEN inv."Nomer"
			ELSE NULL
		END AS nomer,
        -- inv_amount, -- сумма по счету, к которому привязана операция
		CASE op_type
	        WHEN 1 THEN 'Начисление'
	        WHEN 2 THEN 'Оплата'
	        WHEN 3 THEN 'Зачет предоплаты'
	        ELSE 'Неизвестный тип (' || op_type || ')'
	    END AS op_type_name,
        op_amount, -- сумма операции
        -- balance_before, -- накопленный долг за услуги по плательщику перед операцией
        charge_amount, -- начислено за услуги
        payment_amount, -- оплачено за услуги
        -- balance_after, -- накопленный долг за услуги по плательщику после операции
		-balance_after AS debt_after,
        
        -- prepayment_before, -- накопленная предоплата по плательщику перед операцией
        prepayment_added, -- зачислена предоплата
        prepayment_applied, -- зачтена предоплата
        prepayment_after -- накопленная предоплата по плательщику после операции
        
        -- inv_debt_before, -- долг по счету перед операцией
        -- inv_debt_after -- долг по счету после операции
        
        -- debt_invoices_before, -- список всех счетов плательщика с долгами перед операцией
        -- debt_invoices_after -- список всех счетов плательщика с долгами после операции
FROM public.get_operations(51158, 3) op
JOIN berg."XInvoices" inv ON op.id_invoice = inv."ID"

