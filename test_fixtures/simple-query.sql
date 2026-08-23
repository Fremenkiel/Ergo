DO $$
BEGIN
    PERFORM pg_logical_emit_message(
        true, 
        'ergo_meta', 
        '"user_id": "42", "ip": "192.168.1.50"'
    );
    
    INSERT INTO addresses (address_line_1, postal_code, city, country) 
    VALUES ('1 Apple Park Way', '95014', 'Cupertino', 'US');
END $$;
