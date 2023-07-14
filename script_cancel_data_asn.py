import psycopg2

conn = psycopg2.connect(
    user='odoo', 
    password='USMHOdoostore2021',
    host='10.130.0.2',
    port='5432',
    database='usmh_middleware'
)
cr = conn.cursor()
cr.execute("""SELECT odoo.store_code
            FROM asn_aggregation_processed d 
            JOIN odoo_server odoo ON odoo.store_code = d.store_code
            GROUP BY odoo.store_code, url, port, username, password, database_name
            ORDER BY odoo.store_code""")
stores = cr.fetchall()
print (stores)
conn.close()
for st in stores:
    print (st[0])
    conn = psycopg2.connect(
        user='odoo', 
        password='USMHOdoostore2021',
        host='10.130.0.2',
        port='5432',
        database='usmh_middleware'
    )
    cr = conn.cursor()
    cr.execute("""SELECT url, port, username, password, database_name FROM odoo_server WHERE store_code = '%s' LIMIT 1""" % st[0])
    store = cr.fetchall()[0]
    conn.close()

    database = store[4]
    host = store[0]
    password = store[3]
    user = store[2]
    port = store[1]

    print ('START')
    conn = psycopg2.connect(
        user=user, 
        password=password,
        host=host,
        port=port,
        database=database
    )
    cr = conn.cursor()
    cr.execute("""
    DROP FUNCTION IF EXISTS set_asn_moves_as_cancel_23nov(varchar);
    CREATE OR REPLACE FUNCTION set_asn_moves_as_cancel_23nov(_store_code character varying)
    RETURNS void
    LANGUAGE plpgsql
    AS $$
        DECLARE
            locations record;
        BEGIN
            RAISE notice 'TRIGER STATUS: Running cancel ASN moves';
            
            SELECT w.receipt_oc_picking_type_id as picking_type_id, default_location_src_id as location_id, default_location_dest_id as location_dest_id
            FROM stock_picking_type t JOIN stock_warehouse w ON t.id=w.receipt_oc_picking_type_id WHERE w.active=true AND w.store_code=_store_code LIMIT 1
            INTO locations;
            IF locations IS NULL THEN
                RAISE exception 'Can not find related warehouse %', _store_code;
            END IF;

            -- 
            DROP MATERIALIZED VIEW IF EXISTS stock_quant_asn_temp_cancel;
            execute format('CREATE MATERIALIZED VIEW stock_quant_asn_temp_cancel as 
                SELECT * FROM stock_move WHERE origin=''ASN_DATA'' AND location_dest_id=%L AND (date+interval ''9 hours'')::DATE<(now() + interval ''9 hours'')::date and state = ''assigned'' WITH DATA', locations.location_dest_id);

            -- update state stock move
            UPDATE stock_move SET state='cancel' WHERE origin='ASN_DATA' AND location_dest_id=locations.location_dest_id AND (date+interval '9 hours')::DATE<(now() + interval '9 hours')::date and state = 'assigned';

            -- Update state stock move line
            UPDATE stock_move_line SET state='cancel' WHERE reference='ASN_DATA' AND location_dest_id=locations.location_dest_id AND (date+interval '9 hours')::DATE<(now() + interval '9 hours')::date and state = 'assigned';

            TRUNCATE master_closing_asn_aggregation;

            INSERT INTO master_closing_asn_aggregation (middleware_id)
            SELECT middleware_id 
            FROM stock_quant_asn_temp_cancel 
            WHERE origin='ASN_DATA' 
                AND (date+interval '9 hours')::DATE<(now() + interval '9 hours')::date
                AND state = 'cancel';

            UPDATE master_closing_asn_aggregation SET store_code = _store_code;

        EXCEPTION
            WHEN OTHERS THEN
            RAISE exception 'Encountered an issue when updating stock move, stock move line, or stock quant';
        END;
    $$;
    """)
    conn.commit()
    print ('Function created')
    cr.execute("""SELECT set_asn_moves_as_cancel_23nov('%s')""" % st[0])
    conn.commit()
    print ('Function finish')
    conn.close()

    conn = psycopg2.connect(
        user='odoo', 
        password='USMHOdoostore2021',
        host='10.130.0.2',
        port='5432',
        database='usmh_middleware'
    )
    cr = conn.cursor()
    cr.execute("""SELECT generate_asn_cancel_data('%s', '%s', '%s', '%s', '%s')""" % (database, host, port, user, password))
    conn.commit()
    conn.close()
    print ('FINISH')
