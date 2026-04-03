--Задание 1. Представление — сводка по добыче (простое)
CREATE OR REPLACE VIEW v_daily_production_summary AS 
SELECT dd.full_date,dm.mine_name,ds.shift_name, COUNT(*), SUM(fp.tons_mined) AS tonnage,SUM(fp.fuel_consumed_l) AS fuel_consumed, ROUND(AVG(fp.trips_count),2) AS avg_trips
FROM "public".fact_production fp
JOIN "public".dim_date dd ON fp.date_id=dd.date_id
JOIN "public".dim_mine dm ON fp.mine_id=dm.mine_id
JOIN "public".dim_shift ds ON fp.shift_id=ds.shift_id
GROUP BY dd.full_date,dm.mine_name,ds.shift_name;
SELECT * FROM v_daily_production_summary WHERE full_date>='2024-03-01' AND full_date<='2024-03-31' AND COUNT>=5 AND mine_name='Шахта "Северная"';
--Задание 2. Представление с ограничением обновления (простое)
CREATE OR REPLACE VIEW v_unplanned_downtime AS 
SELECT * FROM "public".fact_equipment_downtime  
WHERE is_planned = false
with check option;
SELECT 'from source',COUNT(*) FROM "public".fact_equipment_downtime
UNION ALL
SELECT 'from view',COUNT(*) FROM v_unplanned_downtime;
--Задание 3. Материализованное представление для качества руды (среднее)
CREATE MATERIALIZED VIEW mv_monthly_ore_quality AS
SELECT dm.mine_name,dd.year_month, COUNT(*) 
  AS count_prob, ROUND(AVG (foq.fe_content),2)
  AS avg_fe_content, MIN(foq.fe_content)
  AS min_fe_content,MAX(foq.fe_content)
  AS max_fe_content,ROUND(AVG(sio2_content),2)
  AS avg_sio2_content ,ROUND(AVG(moisture),2)
  AS avg_moisture
FROM "public".fact_ore_quality foq
JOIN "public".dim_mine dm ON foq.mine_id=dm.mine_id
JOIN "public".dim_date dd ON foq.date_id=dd.date_id
GROUP BY dm.mine_name,dd.year_month;
CREATE INDEX mine_index ON mv_monthly_ore_quality(mine_name);
CREATE INDEX year_month_index ON mv_monthly_ore_quality(year_month);
REFRESH MATERIALIZED VIEW mv_monthly_ore_quality;
EXPLAIN analyze 
SELECT * FROM mv_monthly_ore_quality;
EXPLAIN analyze 
SELECT dm.mine_name,dd.year_month, COUNT(*) 
  AS count_prob, ROUND(AVG (foq.fe_content),2) 
  AS avg_fe_content, MIN(foq.fe_content) 
  AS min_fe_content,MAX(foq.fe_content) 
  AS max_fe_content,ROUND(AVG(sio2_content),2) 
  AS avg_sio2_content ,ROUND(AVG(moisture),2) 
  AS avg_moisture
FROM "public".fact_ore_quality foq
JOIN "public".dim_mine dm ON foq.mine_id=dm.mine_id
JOIN "public".dim_date dd ON foq.date_id=dd.date_id
GROUP BY dm.mine_name,dd.year_month;
--Задание 4. Производная таблица — ранжирование операторов (среднее)
SELECT 
    shift_name,
    operator_name,
    total_production
FROM (
    SELECT 
        s.shift_name AS shift_name,
        CONCAT(o.last_name, ' ', o.first_name, ' ', COALESCE(o.middle_name, '')) AS operator_name,
        SUM(f.tons_mined) AS total_production,
        ROW_NUMBER() OVER (
            PARTITION BY s.shift_id 
            ORDER BY SUM(f.tons_mined) DESC
        ) AS rn
    FROM fact_production f
    INNER JOIN dim_operator o ON f.operator_id = o.operator_id
    INNER JOIN dim_shift s ON f.shift_id = s.shift_id
    INNER JOIN dim_date d ON f.date_id = d.date_id
    WHERE d.full_date BETWEEN '2024-01-01' AND '2024-03-31'
    GROUP BY s.shift_id, s.shift_name, o.operator_id, o.last_name, o.first_name, o.middle_name
) sub
WHERE rn = 1
ORDER BY shift_name;
--Задание 5. CTE — комплексный отчёт по эффективности (среднее)
WITH production_cte AS (
SELECT mine_id,SUM(operating_hours) AS sum_operating_hours, SUM(tons_mined) AS sum_tones_mined
FROM fact_production fp
GROUP BY mine_id
), downtime_cte AS (
SELECT de.mine_id,ROUND(SUM(fed.duration_min)/60,1) AS sum_downtime_hours
FROM fact_equipment_downtime fed
JOIN dim_equipment de ON fed.equipment_id=de.equipment_id
GROUP BY de.mine_id
)
SELECT dm.mine_name, p_cte.sum_operating_hours, d_cte.sum_downtime_hours, p_cte.sum_tones_mined, round(100*p_cte.sum_operating_hours/d_cte.sum_downtime_hours,1) AS "availbility%"
FROM downtime_cte d_cte
JOIN production_cte p_cte ON d_cte.mine_id=p_cte.mine_id
JOIN dim_mine dm ON d_cte.mine_id=dm.mine_id
ORDER BY "availbility%";
--Задание 6. Табличная функция — отчёт по простоям (среднее)
CREATE OR REPLACE FUNCTION fn_equipment_downtime_report(p_equipment_id INT, p_date_from INT, p_date_to INT)
RETURNS TABLE(full_date date, reason_name varchar(200),category varchar(50), duration_min numeric,duration_hour numeric,is_planned bool, comment text) AS $$
BEGIN
	RETURN QUERY
		SELECT dd.full_date, ddr.reason_name, ddr.category, fed.duration_min, ROUND(fed.duration_min/60,1) AS duration_hours,fed.is_planned,fed.comment
		FROM fact_equipment_downtime fed
		JOIN dim_date dd ON fed.date_id=dd.date_id
		JOIN dim_downtime_reason ddr ON fed.reason_id=ddr.reason_id
		WHERE fed.equipment_id=p_equipment_id
		AND fed.date_id>=p_date_from
	  AND fed.date_id<=p_date_to
	;
END;
$$ LANGUAGE plpgsql;
SELECT * FROM fn_equipment_downtime_report(3,20240101,20240131);
SELECT de.equipment_name,fned.* FROM dim_equipment de CROSS JOIN LATERAL (SELECT * FROM fn_equipment_downtime_report(de.equipment_id,20000101,21000101)) AS fned WHERE de.mine_id=1;
--Задание 7. Рекурсивный CTE — иерархия локаций (сложное)
WITH RECURSIVE mine_tree AS (
    -- 1. Базовая часть (Anchor)
    SELECT location_id,LPAD(location_name,LENGTH(location_name) +depth_level,'  ') AS hierarchy,location_type,depth_level,CAST(location_name AS text) AS full_path 
    FROM dim_location_hierarchy dlh
    WHERE parent_id IS NULL
    UNION ALL
    -- 2. Рекурсивная часть (Recursive step)
    SELECT child.location_id,LPAD(location_name,LENGTH(location_name) +child.depth_level,'  ') AS hierarchy,child.location_type,child.depth_level,concat(parent.full_path,'->',location_name) AS full_path
    FROM dim_location_hierarchy child
    INNER JOIN mine_tree parent ON child.parent_id = parent.location_id
)
SELECT * FROM mine_tree ORDER BY full_path;
--Задание 8. Рекурсивный CTE — генерация календаря и заполнение пропусков (сложное)
WITH RECURSIVE date_series AS (
	SELECT DATE '2024-02-01' AS date
    UNION ALL
    SELECT cast(ds.date + INTERVAL '1 day' as date)
    FROM date_series ds
    WHERE date < DATE '2024-02-29'
)
SELECT 'days without production',COUNT(DISTINCT d_s.date)
FROM date_series d_s
LEFT JOIN dim_date dd ON d_s.date=dd.full_date
LEFT JOIN fact_production fp ON fp.date_id=dd.date_id
WHERE fp.production_id IS NULL
UNION ALL
SELECT 'work days without production',COUNT(DISTINCT d_s.date)
FROM date_series d_s
LEFT JOIN dim_date dd ON d_s.date=dd.full_date
LEFT JOIN fact_production fp ON fp.date_id=dd.date_id
WHERE fp.production_id IS NULL
AND EXTRACT(DOW FROM d_s.date) NOT IN (0, 6)
;
WITH RECURSIVE date_series AS (
	SELECT DATE '2024-02-01' AS date
    UNION ALL
    SELECT cast(ds.date + INTERVAL '1 day' AS date)
    FROM date_series ds
    WHERE date < DATE '2024-02-29'
)
SELECT d_s.date,
		TO_CHAR(d_s.date, 'Day') AS day_of_week,
        CASE 
            WHEN EXTRACT(DOW FROM d_s.date) IN (0, 6) THEN 'выходной'
            ELSE 'рабочий'
        END AS day_type 
FROM date_series d_s 
ORDER BY d_s.date;
--Задание 9. CTE для скользящего среднего (сложное)
WITH daily_production AS (
	SELECT fp.date_id, sum(fp.tons_mined) AS tonnage
	FROM fact_production fp
	WHERE fp.mine_id=1 AND fp.date_id>20240101 AND fp.date_id<20240401
	GROUP BY fp.date_id
),
sliding_stat AS (
	SELECT d_p.date_id,
			d_p.tonnage,
			ROUND(AVG(d_p.tonnage) OVER (ORDER BY date_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),2) AS avg_7days,
			MAX(d_p.tonnage) OVER (ORDER BY date_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS max_7days,
			ABS(ROUND(100*d_p.tonnage/(AVG(d_p.tonnage) OVER (ORDER BY date_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW))-100,1)) AS otkl
	FROM daily_production d_p
)
SELECT dd.full_date,
		s_s.tonnage,
		s_s.avg_7days,
		s_s.max_7days,
		s_s.otkl,
		CASE
			WHEN s_s.otkl>20 THEN true
			ELSE false
		END AS anomaly
FROM sliding_stat s_s
JOIN dim_date dd ON s_s.date_id=dd.date_id
ORDER BY dd.full_date;
--Задание 10. Комплексное задание: VIEW + CTE + функция (продвинутое)
CREATE OR REPLACE VIEW v_ore_quality_detail AS
SELECT 
    f.*,
    dd.full_date,
    m.mine_name,
    s.shift_name,
    og.grade_name AS ore_grade_name,
    CASE 
        WHEN f.fe_content >= 65 THEN 'Богатая'
        WHEN f.fe_content >= 55 AND f.fe_content < 65 THEN 'Средняя'
        WHEN f.fe_content >= 45 AND f.fe_content < 55 THEN 'Бедная'
        WHEN f.fe_content < 45 THEN 'Очень бедная'
        ELSE 'Не определено'
    END AS quality_category
FROM fact_ore_quality f
LEFT JOIN dim_mine m ON f.mine_id = m.mine_id
LEFT JOIN dim_shift s ON f.shift_id = s.shift_id
LEFT JOIN dim_ore_grade og ON f.ore_grade_id = og.ore_grade_id
left join dim_date dd on dd.date_id=f.date_id;

CREATE OR REPLACE FUNCTION fn_ore_quality_stats(
    p_mine_id INT,
    p_year INT,
    p_month INT
)
RETURNS TABLE (
    total_samples BIGINT,
    avg_fe_content NUMERIC(10,2),
    stddev_fe_content NUMERIC(10,2),
    rich_ore_share NUMERIC(5,2)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) AS total_samples,
        ROUND(AVG(fe_content), 2) AS avg_fe_content,
        ROUND(STDDEV(fe_content), 2) AS stddev_fe_content,
        ROUND(
            (COUNT(CASE WHEN fe_content >= 55 THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC) * 100, 
            2
        ) AS rich_ore_share
    FROM fact_ore_quality f
	join dim_date dd on f.date_id=dd.date_id
    WHERE f.mine_id = p_mine_id
         AND EXTRACT(YEAR FROM dd.full_date) = p_year
         AND EXTRACT(MONTH FROM dd.full_date) = p_month;
END;
$$ LANGUAGE plpgsql;

SELECT 
    m.mine_name,
    stats.total_samples,
    stats.avg_fe_content,
    stats.stddev_fe_content,
    stats.rich_ore_share
FROM dim_mine m
CROSS JOIN LATERAL fn_ore_quality_stats(m.mine_id, 2024, 3) stats
WHERE m.status = 'active'
ORDER BY stats.avg_fe_content DESC;

WITH monthly_stats AS (
    SELECT 
        DATE_TRUNC('month', full_date) AS month,
        ROUND(AVG(fe_content), 2) AS avg_fe_content,
        COUNT(*) AS sample_count
    FROM v_ore_quality_detail
    WHERE full_date >= '2024-01-01' 
        AND full_date < '2025-01-01'
    GROUP BY DATE_TRUNC('month', full_date)
),
moving_average AS (
    SELECT 
        month,
        avg_fe_content,
        sample_count,
        ROUND(
            AVG(avg_fe_content) OVER (
                ORDER BY month 
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ), 
            2
        ) AS moving_avg_3m
    FROM monthly_stats
)
SELECT 
    TO_CHAR(month, 'YYYY-MM') AS month,
    avg_fe_content,
    moving_avg_3m,
    CASE 
        WHEN avg_fe_content > LAG(avg_fe_content) OVER (ORDER BY month) THEN 'рост'
        WHEN avg_fe_content < LAG(avg_fe_content) OVER (ORDER BY month) THEN 'снижение'
        WHEN avg_fe_content = LAG(avg_fe_content) OVER (ORDER BY month) THEN '➡стабильно'
        ELSE 'нет данных'
    END AS trend
FROM moving_average
ORDER BY month;
