DROP ROLE IF EXISTS Administrator;
DROP ROLE IF EXISTS Visitor;

CREATE ROLE Administrator;
GRANT pg_read_all_settings TO Administrator;
GRANT pg_signal_backend TO Administrator;
GRANT pg_read_all_data TO Administrator;
GRANT pg_write_all_data TO Administrator;

CREATE ROLE Visitor;
GRANT pg_read_all_data TO Visitor;

-- SET ROLE Visitor;
-- SELECT * FROM Checks;
-- SELECT * FROM personal_data;
-- DELETE FROM Checks WHERE transaction_id = 1; -- permission denied!
-- SELECT current_user;