DROP FUNCTION IF EXISTS generate_confirm_invoice(integer);
CREATE OR REPLACE FUNCTION public.generate_confirm_invoice(invid integer)
RETURNS void
LANGUAGE plpgsql
AS $$
    DECLARE
        invoice record;
        invoiceLine record;
        inv_name varchar;
    BEGIN
        SELECT * FROM account_move WHERE id = inv_id INTO invoice;
        SELECT * FROM account_move_line WHERE move_id = inv_id ORDER BY id INTO invoiceLine;
        SELECT 'INV/' + date_part('year', now()) + '/' INTO inv_name;

        -- invoice_date = now()
        UPDATE account_invoice SET
            invoice_date = now(),
            sequence_prefix = 'INV/' + date_part('year', now()) + '/',
            name = inv_name,
            payment_reference = inv_name,
            state = 'posted'
        WHERE id = invoice.id;

    END;
$$;