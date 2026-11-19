DO $$
BEGIN
    PERFORM pg_logical_emit_message(
        true, 
        'ergo_meta', 
        '"user_id": "42", "ip": "192.168.1.50"'
    );
    
    INSERT INTO addresses (id, address_line_1, postal_code, city, country) 
    OVERRIDING SYSTEM VALUE VALUES (1, '1 Apple Park Way', '95014', 'Cupertino', 'US'); 

    UPDATE addresses 
    SET address_line_1 = 'Googleplex', city = 'Mountain View', postal_code = '94043'
    WHERE id = 1;

    DELETE FROM addresses WHERE id = 1;

    INSERT INTO addresses (id, address_line_1, postal_code, city, country) 
    OVERRIDING SYSTEM VALUE VALUES (2, 'Googleplex', '94043', 'Mountain View', 'US');

    UPDATE addresses 
    SET address_line_1 = '1 Apple Park Way', city = 'Cupertino', postal_code = '95014'
    WHERE id = 2;

    DELETE FROM addresses WHERE id = 2;
END $$;
