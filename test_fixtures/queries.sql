-- wal_insert_single.bin
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

-- wal_update_single.bin
DO $$
BEGIN
    PERFORM pg_logical_emit_message(
        true, 
        'ergo_meta', 
        '"user_id": "42", "ip": "192.168.1.50"'
    );
    
	UPDATE addresses SET address_line_1 = 'Googleplex', city = 'Mountain View', postal_code = '94043' WHERE id = 1;
END $$;

-- wal_delete_single.bin
DO $$
BEGIN
    PERFORM pg_logical_emit_message(
        true, 
        'ergo_meta', 
        '"user_id": "42", "ip": "192.168.1.50"'
    );
    
	DELETE FROM addresses WHERE id = 1;
END $$;

-- wal_insert_multiple.bin
DO $$
BEGIN
    PERFORM pg_logical_emit_message(
        true, 
        'ergo_meta', 
        '"user_id": "42", "ip": "192.168.1.50"'
    );
    
    INSERT INTO addresses (address_line_1, postal_code, city, country) 
    VALUES ('1 Apple Park Way', '95014', 'Cupertino', 'US');
    INSERT INTO addresses (address_line_1, postal_code, city, country) 
    VALUES ('Googleplex', '94043', 'Mountain View', 'US');
END $$;

-- wal_update_multiple.bin
DO $$
BEGIN
    PERFORM pg_logical_emit_message(
        true, 
        'ergo_meta', 
        '"user_id": "42", "ip": "192.168.1.50"'
    );
    
	UPDATE addresses 
	SET address_line_1 = 'Googleplex', city = 'Mountain View', postal_code = '94043'
	WHERE id = 1;
	UPDATE addresses
	SET address_line_1 = '1 Apple Park Way', city = 'Cupertino', postal_code = '95014'
	WHERE id = 2;
END $$;

-- wal_delete_multiple.bin
DO $$
BEGIN
    PERFORM pg_logical_emit_message(
        true, 
        'ergo_meta', 
        '"user_id": "42", "ip": "192.168.1.50"'
    );
    
	DELETE FROM addresses WHERE id = 1;
	DELETE FROM addresses WHERE id = 2;
END $$;

-- wal_mixed_single.bin
DO $$
DECLARE
    new_id INTEGER;
BEGIN
    PERFORM pg_logical_emit_message(
        true, 
        'ergo_meta', 
        '"user_id": "42", "ip": "192.168.1.50"'
    );
    
    INSERT INTO addresses (address_line_1, postal_code, city, country) 
    VALUES ('1 Apple Park Way', '95014', 'Cupertino', 'US') 
    RETURNING id INTO new_id;

    UPDATE addresses 
    SET address_line_1 = 'Googleplex', city = 'Mountain View', postal_code = '94043'
    WHERE id = new_id;

	DELETE FROM addresses WHERE id = new_id;
END $$;

-- wal_mixed_multiple.bin
DO $$
DECLARE
    new_id_1 INTEGER;
    new_id_2 INTEGER;
BEGIN
    PERFORM pg_logical_emit_message(
        true, 
        'ergo_meta', 
        '"user_id": "42", "ip": "192.168.1.50"'
    );
    
    INSERT INTO addresses (address_line_1, postal_code, city, country) 
    VALUES ('1 Apple Park Way', '95014', 'Cupertino', 'US') 
    RETURNING id INTO new_id_1;
    INSERT INTO addresses (address_line_1, postal_code, city, country) 
    VALUES ('Googleplex', '94043', 'Mountain View', 'US')
    RETURNING id INTO new_id_2;

    UPDATE addresses 
    SET address_line_1 = 'Googleplex', city = 'Mountain View', postal_code = '94043'
    WHERE id = new_id_1;
    UPDATE addresses 
    SET address_line_1 = '1 Apple Park Way', city = 'Cupertino', postal_code = '95014'
    WHERE id = new_id_2;

	DELETE FROM addresses WHERE id = new_id_1;
	DELETE FROM addresses WHERE id = new_id_2;
END $$;
