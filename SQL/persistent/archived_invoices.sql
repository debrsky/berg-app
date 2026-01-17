CREATE SCHEMA IF NOT EXISTS berg_persistent; 

CREATE TABLE IF NOT EXISTS berg_persistent.archived_invoices (
    id_archived_invoice SERIAL PRIMARY KEY,
    id_invoice INTEGER NOT NULL,
    id_seller INTEGER NOT NULL,
    id_payer INTEGER NOT NULL,
    nomer INTEGER NOT NULL,
    inv_date DATE NOT NULL,
    invoice JSONB NOT NULL,
    reason INTEGER NOT NULL,
    archived_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id_invoice, archived_at)
);


CREATE TABLE IF NOT EXISTS berg_persistent.document_transmissions (
    id_transmission SERIAL PRIMARY KEY,
    id_archived_invoice INTEGER NOT NULL,
    channel INTEGER NOT NULL DEFAULT 1, -- email
    destination TEXT, -- email или tel
    status INTEGER NOT NULL DEFAULT 1, -- 1) sent 2) delivered 3) failed
    sent_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'
)
