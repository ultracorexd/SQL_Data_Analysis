--Задание 1. Анонимный блок — статистика по шахтам (простое)
DO $$ 
DECLARE
    v_mine_count int;
    v_total_production_jan_2025 numeric;
    v_avg_fe_percent numeric;
    v_downtime_count int;
BEGIN
    SELECT COUNT(*) INTO v_mine_count FROM dim_mine;
    SELECT COALESCE(SUM(fp.tons_mined), 0) INTO v_total_production_jan_2025
    FROM fact_production fp
    JOIN dim_date dd ON fp.date_id = dd.date_id
    WHERE dd.year = 2025 AND dd.month = 1;
    SELECT ROUND(AVG(fe_content), 1) INTO v_avg_fe_percent
    FROM fact_ore_quality foq
    JOIN dim_date dd ON foq.date_id = dd.date_id
    WHERE dd.year = 2025 AND dd.month = 1;
    SELECT COUNT(*) INTO v_downtime_count
    FROM fact_equipment_downtime fd
    JOIN dim_date dd ON fd.date_id = dd.date_id
    WHERE dd.year = 2025 AND dd.month = 1;
    RAISE NOTICE '===== Сводка по предприятию «Руда+» =====';
    RAISE NOTICE 'Количество шахт: %', v_mine_count;
    RAISE NOTICE 'Добыча за январь 2025: % т', v_total_production_jan_2025;
    RAISE NOTICE 'Среднее содержание Fe: %', v_avg_fe_percent;
    RAISE NOTICE 'Количество простоев: %', v_downtime_count;
    RAISE NOTICE '==========================================';
END $$;
--Задание 2. Переменные и классификация — категории оборудования (простое)
DO $$ 
DECLARE
    v_rec RECORD;
    v_age_years INT;
    v_category TEXT;

    v_cnt_new INT := 0;
    v_cnt_working INT := 0;
    v_cnt_attention INT := 0;
    v_cnt_replace INT := 0;
BEGIN
    RAISE NOTICE '=== Отчет по классификации оборудования ===';
    RAISE NOTICE '% | % | % | %', 
        RPAD('Название', 20), RPAD('Тип', 15), RPAD('Возраст', 8), 'Категория';
    RAISE NOTICE '------------------------------------------------------------';
    FOR v_rec IN (
        SELECT 
            equipment_name, 
            COALESCE(commissioning_date, CURRENT_DATE - (random() * 4000)::INT) as c_date 
        FROM dim_equipment
    ) 
    LOOP
        v_age_years := EXTRACT(YEAR FROM AGE(CURRENT_DATE, v_rec.c_date));
        IF v_age_years < 2 THEN
            v_category := 'Новое';
            v_cnt_new := v_cnt_new + 1;
        ELSIF v_age_years BETWEEN 2 AND 5 THEN
            v_category := 'Рабочее';
            v_cnt_working := v_cnt_working + 1;
        ELSIF v_age_years > 5 AND v_age_years <= 10 THEN
            v_category := 'Требует внимания';
            v_cnt_attention := v_cnt_attention + 1;
        ELSE
            v_category := 'На замену';
            v_cnt_replace := v_cnt_replace + 1;
        END IF;
        RAISE NOTICE '% | % | % лет | %', 
            RPAD(v_rec.equipment_name, 20), 
            RPAD('Оборудование', 15), 
            LPAD(v_age_years::text, 7), 
            v_category;
    END LOOP;
    RAISE NOTICE '------------------------------------------------------------';
    RAISE NOTICE 'ИТОГОВАЯ СВОДКА:';
    RAISE NOTICE '- Новое: %', v_cnt_new;
--Задание 3. Циклы — подневной анализ добычи (простое)
DO $$ 
DECLARE
    v_day INT:=0;
    v_total_tons numeric;
    v_cumulative_tons numeric:=0.0;
	v_avg_prev_tons numeric;
	v_ach TEXT;
	v_best_day INT;
	v_best_prod numeric:=0;
	v_rec RECORD;
BEGIN
	RAISE NOTICE '=============================================';
	FOR v_rec IN (
		SELECT
			fp.date_id,SUM(tons_mined) AS total_tons,
			AVG(SUM(tons_mined)) over (ORDER BY fp.date_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_prev
			FROM fact_production fp
			WHERE fp.date_id BETWEEN 20240101 AND 20240114
			GROUP BY fp.date_id
			ORDER BY fp.date_id
	) LOOP
		v_day:=v_day+1;
		v_total_tons:=v_rec.total_tons;
		v_cumulative_tons:=v_cumulative_tons+v_total_tons;
		v_avg_prev_tons:=v_rec.avg_prev;
		IF v_total_tons > v_rec.avg_prev THEN 
			v_ach:='РЕКОРД';
		ELSE
			v_ach:='';
		END IF; 
		IF v_total_tons>v_best_prod THEN
			v_best_prod=v_total_tons;
			v_best_day=v_day;
		END IF;
		RAISE NOTICE 'День %: % т.| Нарастающий: % т. |%', 
            v_day,
			v_total_tons,
			v_cumulative_tons,
			v_ach;
	END LOOP;
	RAISE NOTICE 'ИТОГО % т.| СРЕДНЕЕ В ДЕНЬ % т. |ЛУЧШИЙ ДЕНЬ - %',
			v_cumulative_tons,
			ROUND(v_avg_prev_tons,2),
			v_best_day;
end $$;
--Задание 4. WHILE — мониторинг порога простоев (среднее)
DO $$ 
DECLARE
    v_threshold numeric := 500.0;
    v_current_date date := '2025-01-01';
    v_end_date date := '2025-01-31';
    v_daily_hours numeric := 0;
    v_cumulative_hours numeric := 0;
    v_date_id int;
    v_threshold_reached boolean := false;
BEGIN
    RAISE NOTICE '=== Анализ критического порога простоев (Порог: % ч.) ===', v_threshold;
    WHILE v_current_date <= v_end_date LOOP
        v_date_id := to_char(v_current_date, 'YYYYMMDD')::int;
        SELECT COALESCE(SUM(duration_min), 0) / 60.0 INTO v_daily_hours
        FROM fact_equipment_downtime
        WHERE date_id = v_date_id;
        v_cumulative_hours := v_cumulative_hours + v_daily_hours;
        IF v_cumulative_hours >= v_threshold THEN
            RAISE NOTICE 'КРИТИЧЕСКИЙ ПОРОГ ДОСТИГНУТ!';
            RAISE NOTICE 'Дата: % | Накоплено: % ч.', v_current_date, ROUND(v_cumulative_hours, 1);
            v_threshold_reached := true;
            EXIT;
        END IF;
        v_current_date := v_current_date + 1;
        CONTINUE; 
    END LOOP;
--Задание 5. CASE и FOREACH — анализ датчиков (среднее)
DO $$ 
DECLARE
    v_type_ids int[];
    v_current_id int;
    v_type_name text;
    v_sensor_count int;
    v_telemetry_count int;
    v_avg_readings numeric;
    v_status text;
BEGIN
    SELECT array_agg(sensor_type_id) INTO v_type_ids FROM dim_sensor_type;
    RAISE NOTICE '=== Отчет по активности типов датчиков (Январь 2025) ===';
    FOREACH v_current_id IN ARRAY v_type_ids LOOP
        SELECT type_name INTO v_type_name FROM dim_sensor_type WHERE sensor_type_id = v_current_id;
        SELECT COUNT(*) INTO v_sensor_count FROM dim_sensor WHERE sensor_type_id = v_current_id;
        SELECT COUNT(*) INTO v_telemetry_count
        FROM fact_equipment_telemetry fet
        JOIN dim_sensor ds ON fet.sensor_id = ds.sensor_id
        JOIN dim_date dd ON fet.date_id = dd.date_id
        WHERE ds.sensor_type_id = v_current_id 
          AND dd.year = 2025 AND dd.month = 1;
        v_avg_readings := v_telemetry_count / NULLIF(v_sensor_count, 0);
        v_status := CASE 
            WHEN v_telemetry_count = 0 THEN 'Нет данных'
            WHEN v_avg_readings > 1000 THEN 'Активно работает'
            WHEN v_avg_readings BETWEEN 100 AND 1000 THEN 'Нормальная работа'
            ELSE 'Редкие показания'
        END;
        RAISE NOTICE 'Тип: % | Датчиков: % | Показаний: % | Статус: %', 
            RPAD(v_type_name, 15), 
            LPAD(v_sensor_count::text, 3), 
            LPAD(v_telemetry_count::text, 7), 
            v_status;
    END LOOP;
    RAISE NOTICE '=======================================================';
END $$;
    IF NOT v_threshold_reached THEN
        RAISE NOTICE 'За январь порог в % ч. достигнут не был. Итог: % ч.', 
                     v_threshold, ROUND(v_cumulative_hours, 1);
    END IF;
END $$;
--Задание 6. Курсор — пакетное формирование отчёта по сменам (среднее)
DO $$ 
DECLARE
    cur_dates CURSOR FOR 
        SELECT full_date, date_id 
        FROM dim_date 
        WHERE full_date BETWEEN '2025-01-01' AND '2025-01-15'
        ORDER BY full_date;
    v_date_rec RECORD;
    v_inserted_rows INT := 0;
    v_total_inserted INT := 0;
BEGIN
    RAISE NOTICE '=== Начало формирования отчетов по сменам ===';
    FOR v_date_rec IN cur_dates LOOP
        INSERT INTO "Pechersky".report_shift_summary (
            report_date, shift_name, mine_name, total_tons, equipment_used, efficiency
        )
        SELECT 
            v_date_rec.full_date,
            ds.shift_name,
            dm.mine_name,
            SUM(fp.tons_mined) as total_tons,
            COUNT(DISTINCT fp.equipment_id) as equipment_used,
            ROUND(
                (SUM(fp.operating_hours) / NULLIF(COUNT(DISTINCT fp.equipment_id) * 8, 0)) * 100, 
                1
            ) as efficiency
        FROM fact_production fp
        JOIN dim_mine dm ON fp.mine_id = dm.mine_id
		join dim_shift ds on fp.shift_id=ds.shift_id
        WHERE fp.date_id = v_date_rec.date_id
        GROUP BY ds.shift_name, dm.mine_name;
        GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;
        v_total_inserted := v_total_inserted + v_inserted_rows;
        RAISE NOTICE 'Дата % обработана. Добавлено записей: %', v_date_rec.full_date, v_inserted_rows;
    END LOOP;
    RAISE NOTICE '=== Завершено. Всего создано строк: % ===', v_total_inserted;
END $$;
SELECT * FROM "Pechersky".report_shift_summary ORDER BY report_date, shift_name, mine_name;
--Задание 7. RETURN NEXT — функция генерации отчёта (сложное)
CREATE OR REPLACE FUNCTION get_quality_trend(p_year INT, p_mine_id INT DEFAULT NULL)
RETURNS TABLE (
    month_num      INT,
    month_name     VARCHAR,
    samples_count  BIGINT,
    avg_fe         NUMERIC,
    min_fe         NUMERIC,
    max_fe         NUMERIC,
    running_avg_fe NUMERIC,
    trend          VARCHAR
) AS $$
DECLARE
    v_total_sum_fe NUMERIC := 0;
    v_total_samples BIGINT := 0;
    v_prev_avg_fe  NUMERIC := NULL;
BEGIN
    FOR i IN 1..12 LOOP
        month_num := i;
        month_name := to_char(to_date(i::text, 'MM'), 'TMMonth');
        SELECT 
            COUNT(foq.fe_content),
            AVG(foq.fe_content),
            MIN(foq.fe_content),
            MAX(foq.fe_content)
        INTO 
            samples_count,
            avg_fe,
            min_fe,
            max_fe
        FROM "public".fact_ore_quality foq
        JOIN "public".dim_date dd ON foq.date_id = dd.date_id
        WHERE dd.year = p_year 
          AND dd.month = i
          AND (p_mine_id IS NULL OR foq.mine_id = p_mine_id);
        avg_fe := ROUND(avg_fe, 2);
        min_fe := ROUND(min_fe, 2);
        max_fe := ROUND(max_fe, 2);
        IF samples_count > 0 THEN
            v_total_sum_fe := v_total_sum_fe + (avg_fe * samples_count);
            v_total_samples := v_total_samples + samples_count;
            running_avg_fe := ROUND(v_total_sum_fe / v_total_samples, 2);
        ELSE
            running_avg_fe := v_prev_avg_fe;
        END IF;
        trend := CASE 
            WHEN v_prev_avg_fe IS NULL OR avg_fe IS NULL THEN 'Стабильно'
            WHEN avg_fe > v_prev_avg_fe + 0.1 THEN 'Улучшение'
            WHEN avg_fe < v_prev_avg_fe - 0.1 THEN 'Ухудшение'
            ELSE 'Стабильно'
        END;
        IF avg_fe IS NOT NULL THEN
            v_prev_avg_fe := avg_fe;
        END IF;
        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
SELECT * FROM get_quality_trend(2025);
SELECT * FROM get_quality_trend(2025, 1);
