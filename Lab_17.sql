--Задание 1. Безопасное деление (простое)
CREATE OR REPLACE FUNCTION safe_production_rate(p_tons NUMERIC, p_hours NUMERIC)
RETURNS NUMERIC AS $$
BEGIN
    IF p_tons IS NULL OR p_hours IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN p_tons / p_hours;
EXCEPTION 
    WHEN division_by_zero THEN
        RAISE WARNING 'Попытка деления на ноль: тонн %, часов %. Возвращен 0.', p_tons, p_hours;
        RETURN 0;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
SELECT safe_production_rate(150, 8) AS test_normal;
SELECT safe_production_rate(150, 0) AS test_zero;
SELECT safe_production_rate(NULL, 8) AS test_null_1;
SELECT safe_production_rate(150, NULL) AS test_null_2;
--Задание 2. Валидация данных телеметрии (простое)
CREATE OR REPLACE FUNCTION validate_sensor_reading(p_sensor_type VARCHAR, p_value NUMERIC)
RETURNS VARCHAR AS $$
DECLARE
    v_min NUMERIC;
    v_max NUMERIC;
    v_unit VARCHAR;
BEGIN
    CASE p_sensor_type
        WHEN 'Температура' THEN v_min := -40; v_max := 200; v_unit := '°C';
        WHEN 'Давление'    THEN v_min := 0;   v_max := 500; v_unit := 'бар';
        WHEN 'Вибрация'    THEN v_min := 0;   v_max := 100; v_unit := 'мм/с';
        WHEN 'Скорость'    THEN v_min := 0;   v_max := 50;  v_unit := 'км/ч';
        ELSE
            RAISE EXCEPTION 'Неизвестный тип датчика: %', p_sensor_type
                USING ERRCODE = 'S0001';
    END CASE;
    IF p_value < v_min OR p_value > v_max THEN
        RAISE EXCEPTION 'Значение % вне диапазона для типа %', p_value, p_sensor_type
            USING ERRCODE = 'S0002',
                  HINT = format('Допустимый диапазон: %s..%s %s', v_min, v_max, v_unit);
    END IF;
    RETURN 'OK';
END;
$$ LANGUAGE plpgsql IMMUTABLE;
SELECT validate_sensor_reading('Температура', 25.5);
SELECT validate_sensor_reading('Давление', 350);
SELECT validate_sensor_reading('Скорость', 120); 
SELECT validate_sensor_reading('Влажность', 50);
SELECT validate_sensor_reading('Температура', -40);
SELECT validate_sensor_reading('Вибрация', 100);
--Задание 3. Обработка ошибок при вставке (среднее)
DO $$ 
DECLARE
    v_success_cnt INT := 0;
    v_error_cnt   INT := 0;
    v_log_id      INT;
    v_msg TEXT; v_state TEXT; v_ctx TEXT;
BEGIN
    RAISE NOTICE '=== Полное заполнение fact_equipment_downtime ===';
    FOR i IN 1..10 LOOP
        BEGIN
            CASE i
                WHEN 3 THEN 
                    INSERT INTO fact_equipment_downtime (
                        date_id, shift_id, equipment_id, reason_id, operator_id, 
                        location_id, start_time, end_time, duration_min, 
                        is_planned, comment, loaded_at
                    ) VALUES (
                        20250125, 1, 999999, 1, 10, 
                        5, '2025-01-25 08:00:00', '2025-01-25 09:00:00', 60, 
                        FALSE, 'Ошибка: нет такого оборудования', NOW()
                    );
                
                WHEN 5 THEN 
                    INSERT INTO fact_equipment_downtime (
                        date_id, shift_id, equipment_id, reason_id, operator_id, 
                        location_id, start_time, end_time, duration_min, 
                        is_planned, comment, loaded_at
                    ) VALUES (
                        20250125, 2, 1, 1, 10, 
                        5, '2025-01-25 10:00:00', NULL, 30, 
                        FALSE, 'Ошибка: end_time is NULL', NOW()
                    );
                
                ELSE
                    INSERT INTO fact_equipment_downtime (
                        date_id, shift_id, equipment_id, reason_id, operator_id, 
                        location_id, start_time, end_time, duration_min, 
                        is_planned, comment, loaded_at
                    ) VALUES (
                        20250125, 
                        (i % 2) + 1,          -- shift_id (1 или 2)
                        1,                    -- equipment_id
                        (i % 3) + 1,          -- reason_id
                        10,                   -- operator_id
                        1,                    -- location_id
                        '2025-01-25 12:00:00'::timestamp + (i || ' hours')::interval, -- start_time
                        '2025-01-25 12:45:00'::timestamp + (i || ' hours')::interval, -- end_time
                        45,                   -- duration_min
                        CASE WHEN i > 8 THEN TRUE ELSE FALSE END, -- is_planned
                        'Плановая загрузка. Запись №' || i,       -- comment
                        NOW()                 -- loaded_at
                    );
            END CASE;
            v_success_cnt := v_success_cnt + 1;
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS 
                v_msg   = MESSAGE_TEXT,
                v_state = RETURNED_SQLSTATE,
                v_ctx   = PG_EXCEPTION_CONTEXT;
            v_error_cnt := v_error_cnt + 1;
            v_log_id := log_error(
                'ОШИБКА', 'Full_Insert_Test', v_state, v_msg, NULL, NULL, v_ctx, 
                jsonb_build_object('row_index', i)
            );

            RAISE WARNING 'Строка %: Сбой (%). См. лог ID: %', i, v_state, v_log_id;
        END;
    END LOOP;
    RAISE NOTICE 'ИТОГО: Успешно: %, Ошибок: %', v_success_cnt, v_error_cnt;
END $$;
--Задание 4. GET STACKED DIAGNOSTICS — детальный отчёт (среднее)
CREATE OR REPLACE FUNCTION test_error_diagnostics(p_error_type INT)
RETURNS TABLE (field_name VARCHAR, field_value TEXT) AS $$
DECLARE
    v_msg      TEXT; v_detail   TEXT; v_hint     TEXT;
    v_sqlstate TEXT; v_ctx      TEXT; v_schema   TEXT;
    v_table    TEXT; v_column   TEXT; v_datatype TEXT;
    v_constraint TEXT;
BEGIN
    CASE p_error_type
        WHEN 1 THEN 
            PERFORM 1 / 0;
        WHEN 2 THEN 
            INSERT INTO dim_mine (mine_id, mine_name) VALUES (1, 'Дубль');
        WHEN 3 THEN 
            INSERT INTO fact_production (mine_id, date_id) VALUES (999999, 20250101);
        WHEN 4 THEN 
            PERFORM 'abc'::INT;
        WHEN 5 THEN 
            RAISE EXCEPTION 'Моя ошибка' USING HINT = 'Проверьте параметры', DETAIL = 'Тут подробности';
    END CASE;
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS 
        v_msg        = MESSAGE_TEXT,
        v_detail     = PG_EXCEPTION_DETAIL,
        v_hint       = PG_EXCEPTION_HINT,
        v_sqlstate   = RETURNED_SQLSTATE,
        v_ctx        = PG_EXCEPTION_CONTEXT,
        v_schema     = SCHEMA_NAME,
        v_table      = TABLE_NAME,
        v_column     = COLUMN_NAME,
        v_datatype   = PG_DATATYPE_NAME,
        v_constraint = CONSTRAINT_NAME;
    field_name := 'MESSAGE';    field_value := v_msg;      RETURN NEXT;
    field_name := 'SQLSTATE';   field_value := v_sqlstate; RETURN NEXT;
    field_name := 'DETAIL';     field_value := v_detail;   RETURN NEXT;
    field_name := 'HINT';       field_value := v_hint;     RETURN NEXT;
    field_name := 'SCHEMA';     field_value := v_schema;   RETURN NEXT;
    field_name := 'TABLE';      field_value := v_table;    RETURN NEXT;
    field_name := 'COLUMN';     field_value := v_column;   RETURN NEXT;
    field_name := 'CONSTRAINT'; field_value := v_constraint; RETURN NEXT;
    field_name := 'DATATYPE';   field_value := v_datatype; RETURN NEXT;
    field_name := 'CONTEXT';    field_value := v_ctx;      RETURN NEXT;
END;
$$ LANGUAGE plpgsql;
SELECT * FROM test_error_diagnostics(1);
--Задание 5. Безопасный импорт с логированием (среднее)
CREATE TABLE IF NOT EXISTS staging_lab_results (
    row_id       SERIAL PRIMARY KEY,
    mine_name    TEXT,
    sample_date  TEXT,
    fe_content   TEXT,
    moisture     TEXT,
    status       VARCHAR(20) DEFAULT 'NEW',
    error_msg    TEXT
);
TRUNCATE staging_lab_results;
INSERT INTO staging_lab_results (mine_name, sample_date, fe_content, moisture) VALUES
('Северная', '2025-01-10', '62.5', '5.2'),      
('Несуществующая', '2025-01-11', '60.0', '4.0'),  
('Западная', '32-01-2025', '58.0', '3.5'),      
('Южная', '2025-01-12', 'N/A', '4.1'),           
('Восточная', '2025-01-13', '150', '2.0'),       
('Северная', '2025-01-14', '61.2', 'NULL'),      
('Центральная', '2025-01-15', '59.8', '4.8'),    
('Западная', '2025-01-16', '-10', '5.0'),        
('Восточная', '2025-01-17', '63.0', 'invalid'),  
('Южная', '2025-01-18', '55.5', '3.3');         


CREATE OR REPLACE FUNCTION process_lab_import()
RETURNS TABLE (total INT, valid INT, errors INT) AS $$
DECLARE
    v_rec RECORD;
    v_mine_id INT;
    v_fe NUMERIC;
    v_date DATE;
    v_moist NUMERIC;
    v_total INT := 0;
    v_valid INT := 0;
    v_errors INT := 0;
BEGIN
    FOR v_rec IN SELECT * FROM staging_lab_results WHERE status = 'NEW' LOOP
        v_total := v_total + 1;
        BEGIN
            SELECT mine_id INTO v_mine_id FROM dim_mine WHERE mine_name = v_rec.mine_name;
            IF v_mine_id IS NULL THEN
                RAISE EXCEPTION 'Шахта "%" не найдена в справочнике', v_rec.mine_name;
            END IF;
            BEGIN
                v_date := v_rec.sample_date::DATE;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'Некорректный формат даты: %', v_rec.sample_date;
            END;
            BEGIN
                v_fe := v_rec.fe_content::NUMERIC;
                IF v_fe < 0 OR v_fe > 100 THEN
                    RAISE EXCEPTION 'Содержание Fe (%) вне диапазона 0-100', v_fe;
                END IF;
            EXCEPTION WHEN invalid_text_representation THEN
                RAISE EXCEPTION 'Значение Fe не является числом: %', v_rec.fe_content;
            END;
            IF v_rec.moisture <> 'NULL' THEN
                v_moist := v_rec.moisture::NUMERIC;
            END IF;
            UPDATE staging_lab_results 
            SET status = 'VALID', error_msg = NULL 
            WHERE row_id = v_rec.row_id;
            v_valid := v_valid + 1;
        EXCEPTION WHEN OTHERS THEN
            UPDATE staging_lab_results 
            SET status = 'ERROR', error_msg = SQLERRM 
            WHERE row_id = v_rec.row_id;
            PERFORM log_error(
                'ОШИБКА', 'process_lab_import', SQLSTATE, SQLERRM, 
                NULL, NULL, NULL, jsonb_build_object('row_id', v_rec.row_id)
            ); 
            v_errors := v_errors + 1;
        END;
    END LOOP;
    RETURN QUERY SELECT v_total, v_valid, v_errors;
END;
$$ LANGUAGE plpgsql;
SELECT * FROM process_lab_import();
SELECT * FROM staging_lab_results ORDER BY row_id;
SELECT * FROM error_log WHERE source = 'process_lab_import' ORDER BY log_id DESC;
--Задание 6. Комплексная функция с иерархией обработки ошибок (сложное)
CREATE TABLE IF NOT EXISTS daily_kpi (
    kpi_id         SERIAL PRIMARY KEY,
    mine_id        INT,
    date_id        INT,
    tons_mined     NUMERIC(12,2),
    oee_percent    NUMERIC(5,2),
    downtime_hours NUMERIC(10,2),
    quality_score  NUMERIC(5,2),
    status         VARCHAR(20),
    error_detail   TEXT,
    calculated_at  TIMESTAMP DEFAULT NOW(),
    UNIQUE (mine_id, date_id)
);
CREATE OR REPLACE FUNCTION recalculate_daily_kpi(p_date_id INT)
RETURNS TABLE (mines_processed INT, mines_ok INT, mines_error INT) AS $$
DECLARE
    v_mine         RECORD;
    v_tons         NUMERIC;
    v_oee          NUMERIC;
    v_downtime     NUMERIC;
    v_quality      NUMERIC;
    v_proc         INT := 0;
    v_ok           INT := 0;
    v_err          INT := 0;
BEGIN
    RAISE NOTICE 'Запуск пересчета KPI за дату: %', p_date_id;
    FOR v_mine IN SELECT mine_id, mine_name FROM dim_mine LOOP
        v_proc := v_proc + 1;
        BEGIN
            SELECT COALESCE(SUM(tons_mined), 0) INTO v_tons 
            FROM fact_production WHERE mine_id = v_mine.mine_id AND date_id = p_date_id;
            SELECT ROUND((SUM(operating_hours) / 24.0) * 100, 2) INTO v_oee 
            FROM fact_production WHERE mine_id = v_mine.mine_id AND date_id = p_date_id;
            SELECT ROUND(COALESCE(SUM(duration_min), 0) / 60.0, 2) INTO v_downtime 
            FROM fact_equipment_downtime WHERE mine_id = v_mine.mine_id AND date_id = p_date_id;
            SELECT ROUND(AVG(fe_percent), 2) INTO v_quality 
            FROM fact_ore_quality WHERE mine_id = v_mine.mine_id AND date_id = p_date_id;
            INSERT INTO daily_kpi (mine_id, date_id, tons_mined, oee_percent, downtime_hours, quality_score, status, error_detail)
            VALUES (v_mine.mine_id, p_date_id, v_tons, v_oee, v_downtime, v_quality, 'SUCCESS', NULL)
            ON CONFLICT (mine_id, date_id) DO UPDATE SET
                tons_mined     = EXCLUDED.tons_mined,
                oee_percent    = EXCLUDED.oee_percent,
                downtime_hours = EXCLUDED.downtime_hours,
                quality_score  = EXCLUDED.quality_score,
                status         = 'SUCCESS',
                error_detail   = NULL,
                calculated_at  = NOW();
            v_ok := v_ok + 1;
        EXCEPTION WHEN OTHERS THEN
            v_err := v_err + 1;
            INSERT INTO daily_kpi (mine_id, date_id, status, error_detail)
            VALUES (v_mine.mine_id, p_date_id, 'ERROR', SQLERRM)
            ON CONFLICT (mine_id, date_id) DO UPDATE SET
                status       = 'ERROR',
                error_detail = SQLERRM,
                calculated_at = NOW();
            PERFORM log_error(
                'КРИТИЧНО', 'recalculate_daily_kpi', SQLSTATE, SQLERRM, 
                NULL, NULL, NULL, jsonb_build_object('mine_id', v_mine.mine_id, 'date_id', p_date_id)
            );
            RAISE WARNING 'Ошибка при расчете шахты % (ID: %): %', v_mine.mine_name, v_mine.mine_id, SQLERRM;
        END;
    END LOOP;
    RETURN QUERY SELECT v_proc, v_ok, v_err;
END;
$$ LANGUAGE plpgsql;
SELECT * FROM recalculate_daily_kpi(20250115);
SELECT 
    k.date_id, 
    m.mine_name, 
    k.tons_mined, 
    k.status, 
    k.error_detail 
FROM daily_kpi k
JOIN dim_mine m ON k.mine_id = m.mine_id
WHERE k.date_id = 20250115 
ORDER BY k.status DESC, m.mine_name;
