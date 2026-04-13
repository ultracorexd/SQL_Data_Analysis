--Задание 1. BEGIN / COMMIT / ROLLBACK (простое)
BEGIN;
INSERT INTO fact_production (
    production_id,
    date_id,
    shift_id,
    mine_id,
    shaft_id,
    equipment_id,
    operator_id,
    location_id,
    ore_grade_id,
    tons_mined,
    tons_transported,
    trips_count,
    distance_km,
    fuel_consumed_l,
    operating_hours,
    loaded_at
) VALUES 
    (1001, 20250310, 1, 1, 3, 1, 1, 1, 1, 85.50, 92.30, 7, 10.50, 125.40, 11.20, CURRENT_TIMESTAMP),
    (1002, 20250310, 1, 1, 3, 2, 2, 2, 1, 78.20, 85.10, 6, 9.80, 118.30, 10.50, CURRENT_TIMESTAMP),
    (1003, 20250310, 1, 1, 4, 3, 10, 6, 2, 68.75, 72.40, 5, 8.90, 105.60, 9.80, CURRENT_TIMESTAMP),
    (1004, 20250310, 1, 1, 4, 4, 3, 7, 3, 72.40, 79.20, 6, 9.20, 112.80, 10.20, CURRENT_TIMESTAMP),
    (1005, 20250310, 1, 1, 5, 5, 4, 8, 1, 81.30, 88.50, 7, 10.10, 120.90, 10.80, CURRENT_TIMESTAMP);

SELECT * FROM fact_production 
WHERE date_id = 20250310 AND shift_id = 1;
COMMIT;
SELECT COUNT(*) as saved_records_count 
FROM fact_production 
WHERE date_id = 20250310 AND shift_id = 1;
BEGIN;
INSERT INTO fact_production (
    production_id,
    date_id,
    shift_id,
    mine_id,
    shaft_id,
    equipment_id,
    operator_id,
    location_id,
    ore_grade_id,
    tons_mined,
    tons_transported,
    trips_count,
    distance_km,
    fuel_consumed_l,
    operating_hours,
    loaded_at
) VALUES 
    (2001, 20250310, 2, 1, 3, 1, 5, 1, 2, 72.30, 78.90, 6, 9.50, 108.70, 10.30, CURRENT_TIMESTAMP),
    (2002, 20250310, 2, 1, 3, 2, 6, 2, 3, 65.80, 70.20, 5, 8.70, 98.40, 9.50, CURRENT_TIMESTAMP),
    (2003, 20250310, 2, 1, 4, 3, 7, 6, 1, 83.60, 90.10, 7, 10.80, 132.50, 11.40, CURRENT_TIMESTAMP),
    (2004, 20250310, 2, 1, 4, 4, 8, 7, 2, 70.15, 75.80, 5, 9.00, 102.30, 9.90, CURRENT_TIMESTAMP),
    (2005, 20250310, 2, 1, 5, 5, 9, 8, 3, 67.45, 71.60, 5, 8.50, 95.70, 9.20, CURRENT_TIMESTAMP);
SELECT * FROM fact_production 
WHERE date_id = 20250310 AND shift_id = 2;
ROLLBACK;
SELECT COUNT(*) as rolled_back_records_count 
FROM fact_production 
WHERE date_id = 20250310 AND shift_id = 2;
SELECT 
    shift_id,
    COUNT(*) as records_count
FROM fact_production 
WHERE date_id = 20250310
GROUP BY shift_id;
SELECT 
    'После COMMIT' as status,
    shift_id,
    COUNT(*) as total_records,
    SUM(tons_mined) as total_tons_mined
FROM fact_production 
WHERE date_id = 20250310
GROUP BY shift_id
UNION ALL
SELECT 
    'После ROLLBACK',
    shift_id,
    COUNT(*),
    SUM(tons_mined)
FROM fact_production 
WHERE date_id = 20250310
GROUP BY shift_id;
--Задание 2. SAVEPOINT — частичная загрузка (простое)
BEGIN;
INSERT INTO fact_production (
    production_id,
    date_id,
    shift_id,
    mine_id,
    shaft_id,
    equipment_id,
    operator_id,
    location_id,
    ore_grade_id,
    tons_mined,
    tons_transported,
    trips_count,
    distance_km,
    fuel_consumed_l,
    operating_hours,
    loaded_at
) VALUES (
    3001,
    20250311,
    1,
    1,
    3,
    1,
    1,
    1,
    1,
    95.75,
    102.40,
    8,
    11.20,
    145.30,
    12.50,
    CURRENT_TIMESTAMP
);
SAVEPOINT sp_after_production;
INSERT INTO fact_ore_quality (
    quality_id,
    date_id,
    time_id,
    shift_id,
    mine_id,
    shaft_id,
    location_id,
    ore_grade_id,
    sample_number,
    fe_content,
    sio2_content,
    al2o3_content,
    moisture,
    density,
    sample_weight_kg,
    loaded_at
) VALUES (
    5001,
    20250311,
    820,
    1,
    1,
    3,
    1,
    1,
    'PRB-20250311-N480-1',
    64.28,
    17.45,
    1.98,
    4.87,
    4.012,
    1.75,
    CURRENT_TIMESTAMP
);
SAVEPOINT sp_after_quality;
INSERT INTO fact_equipment_telemetry (
    telemetry_id,
    date_id,
    time_id,
    equipment_id,
    sensor_id,
    location_id,
    sensor_value,
    is_alarm,
    quality_flag,
    loaded_at
) VALUES (
    10001,
    20250311,
    800,
    1,
    99999,
    1,
    88.45,
    FALSE,
    'OK',
    CURRENT_TIMESTAMP
);
ROLLBACK TO sp_after_quality;
SELECT 'После ROLLBACK TO sp_after_quality' as check_point;
SELECT COUNT(*) as production_exists FROM fact_production WHERE production_id = 3001;
SELECT COUNT(*) as quality_exists FROM fact_ore_quality WHERE quality_id = 5001;
COMMIT;
SELECT '=== ИТОГОВЫЕ РЕЗУЛЬТАТЫ ===' as result;
SELECT 
    production_id,
    date_id,
    shift_id,
    equipment_id,
    tons_mined,
    tons_transported
FROM fact_production 
WHERE production_id = 3001;
SELECT 
    quality_id,
    date_id,
    shift_id,
    location_id,
    ore_grade_id,
    fe_content,
    moisture
FROM fact_ore_quality 
WHERE quality_id = 5001;
SELECT 
    COUNT(*) as telemetry_records_count
FROM fact_equipment_telemetry 
WHERE telemetry_id = 10001;
--Задание 3. ACID на практике (простое)
CREATE TABLE equipment_balance (
    equipment_id INT PRIMARY KEY,
    balance_tons NUMERIC DEFAULT 0,
    CHECK (balance_tons >= 0) 
);
INSERT INTO equipment_balance VALUES (1, 1000), (2, 500);
SELECT * FROM equipment_balance ORDER BY equipment_id;
BEGIN;
UPDATE equipment_balance 
SET balance_tons = balance_tons - 200 
WHERE equipment_id = 1;
UPDATE equipment_balance 
SET balance_tons = balance_tons + 200 
WHERE equipment_id = 2;
SELECT 'Промежуточное состояние (внутри транзакции)' as status, * 
FROM equipment_balance 
ORDER BY equipment_id;
COMMIT;
SELECT 'Финальное состояние после успешного перевода' as status, * 
FROM equipment_balance 
ORDER BY equipment_id;
SELECT 'Состояние перед ошибочным переводом' as status, * 
FROM equipment_balance 
ORDER BY equipment_id;
BEGIN;
UPDATE equipment_balance 
SET balance_tons = balance_tons - 1500 
WHERE equipment_id = 2;
UPDATE equipment_balance 
SET balance_tons = balance_tons + 1500 
WHERE equipment_id = 1;
COMMIT;
SELECT 'Состояние ПОСЛЕ ошибочного перевода (должно быть неизменным)' as status, * 
FROM equipment_balance 
ORDER BY equipment_id;
DO $$
DECLARE
    v_balance_2 NUMERIC;
    v_error_message TEXT;
BEGIN
    SELECT balance_tons INTO v_balance_2 FROM equipment_balance WHERE equipment_id = 2;
    RAISE NOTICE 'Начальный баланс оборудования 2: % тонн', v_balance_2;
    RAISE NOTICE 'Пытаемся перевести 1500 тонн с оборудования 2 на оборудование 1...';
    BEGIN
    UPDATE equipment_balance 
    SET balance_tons = balance_tons - 1500 
    WHERE equipment_id = 2;
    UPDATE equipment_balance 
    SET balance_tons = balance_tons + 1500 
    WHERE equipment_id = 1;
    COMMIT;
    RAISE NOTICE 'Перевод успешно выполнен!';
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
    RAISE NOTICE 'ОШИБКА: %', v_error_message;
    RAISE NOTICE 'Транзакция автоматически откачена. Балансы не изменились.';
    
    PERFORM * FROM equipment_balance WHERE equipment_id = 2 AND balance_tons = v_balance_2;
    IF FOUND THEN
        RAISE NOTICE 'Атомарность подтверждена: баланс оборудования 2 остался = % тонн', v_balance_2;
    END IF;
END $$;
SELECT 'Финальная проверка после всех операций' as status, * 
FROM equipment_balance 
ORDER BY equipment_id;
--Задание 5. Обработка конфликтов блокировок (среднее)
CREATE OR REPLACE FUNCTION safe_update_production(
    p_production_id INT,
    p_new_tons NUMERIC,
    p_timeout_ms INT DEFAULT 5000
) RETURNS VARCHAR AS $$
DECLARE
    v_current_tons NUMERIC;
    v_result VARCHAR;
BEGIN
    EXECUTE format('SET LOCAL lock_timeout = %s', p_timeout_ms);
    BEGIN
        SELECT tons_mined INTO v_current_tons
        FROM fact_production
        WHERE production_id = p_production_id
        FOR UPDATE;
        IF NOT FOUND THEN
            RETURN 'ОШИБКА: запись не найдена';
        END IF;
        UPDATE fact_production 
        SET tons_mined = p_new_tons,
            loaded_at = CURRENT_TIMESTAMP
        WHERE production_id = p_production_id;
        v_result := format('OK: tons_mined изменен с %s на %s', 
                          v_current_tons::TEXT, p_new_tons::TEXT);
        RETURN v_result;
    EXCEPTION 
        WHEN lock_not_available THEN
            RETURN format('ЗАБЛОКИРОВАНО: запись %s недоступна, попробуйте позже (таймаут %s мс)', 
                         p_production_id, p_timeout_ms);
        WHEN deadlock_detected THEN
            RETURN format('DEADLOCK: обнаружен взаимоблокировка при обновлении записи %s, повторите операцию', 
                         p_production_id);
        WHEN OTHERS THEN
            RETURN format('ОШИБКА: %s', SQLERRM);
    END;
END;
$$ LANGUAGE plpgsql;
INSERT INTO fact_production (
    production_id, date_id, shift_id, mine_id, shaft_id,
    equipment_id, operator_id, location_id, ore_grade_id,
    tons_mined, tons_transported, trips_count, distance_km,
    fuel_consumed_l, operating_hours, loaded_at
) VALUES (
    99999, 20250315, 1, 1, 3, 1, 1, 1, 1,
    100.50, 110.20, 7, 10.50, 135.60, 11.20, CURRENT_TIMESTAMP
);
SELECT production_id, tons_mined, loaded_at 
FROM fact_production 
WHERE production_id = 99999;
BEGIN;
SELECT production_id, tons_mined, loaded_at 
FROM fact_production 
WHERE production_id = 9999 
FOR UPDATE;
SELECT 'Запись 9999 заблокирована в сессии A' as status, 
       production_id, tons_mined 
FROM fact_production 
WHERE production_id = 9999;
SELECT pg_sleep(10);
COMMIT;
SELECT 'Сессия A: блокировка снята, транзакция зафиксирована' as status;
SELECT safe_update_production(99999, 150.75, 3000);
