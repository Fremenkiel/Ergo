CREATE OR REPLACE FUNCTION auto_set_replica_identity_full()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    obj record;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
        IF obj.object_type = 'table' THEN
            EXECUTE 'ALTER TABLE ' || obj.objid::regclass || ' REPLICA IDENTITY FULL';
        END IF;
    END LOOP;
END;
$$;

CREATE EVENT TRIGGER trigger_auto_replica_identity
ON ddl_command_end
WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS')
EXECUTE FUNCTION auto_set_replica_identity_full();
