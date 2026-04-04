--Задание 1. ROLLUP — сменный рапорт с подитогами (простое)
SELECT CASE WHEN GROUPING(dm.mine_name)=1 THEN 'Общая' ELSE dm.mine_name END, CASE WHEN GROUPING(ds.shift_name)=1 THEN '-' ELSE ds.shift_name END, SUM(fp.tons_mined ), COUNT(DISTINCT fp.equipment_id )
FROM fact_production fp 
JOIN dim_mine dm ON fp.mine_id=dm.mine_id 
JOIN dim_shift ds ON fp.shift_id =ds.shift_id 
WHERE fp.date_id =20240115
GROUP BY ROLLUP(dm.mine_name, ds.shift_name)
ORDER BY dm.mine_name, ds.shift_name;
--Задание 2. CUBE — матрица «шахта x тип оборудования» (простое)
SELECT CASE WHEN GROUPING(dm.mine_name)=1 THEN 'Все шахты' ELSE dm.mine_name END,
	   CASE WHEN GROUPING(det.type_name)=1 THEN 'Все типы' ELSE det.type_name END,
	   SUM(fp.tons_mined ) AS sum_tons, 
	   ROUND(SUM(fp.tons_mined )/COUNT(DISTINCT fp.equipment_id),1) AS avg_per_equipment,
	   GROUPING(mine_name, type_name) AS grouping_level
FROM fact_production fp 
JOIN dim_mine dm ON dm.mine_id=fp.mine_id 
JOIN dim_equipment de ON de.equipment_id=fp.equipment_id
JOIN dim_equipment_type det ON de.equipment_type_id=det.equipment_type_id 
WHERE fp.date_id BETWEEN 20240101 AND 20240331
GROUP BY CUBE(dm.mine_name, det.type_name)
ORDER BY grouping_level,mine_name, type_name;
--Задание 3. GROUPING SETS — сводка KPI по нескольким срезам (среднее)
SELECT
    CASE
        WHEN GROUPING(m.mine_name) = 0 THEN 'Шахта'
        WHEN GROUPING(s.shift_name) = 0 THEN 'Смена'
        WHEN GROUPING(et.type_name) = 0 THEN 'Тип оборудования'
        ELSE 'ИТОГО'
    END AS dimension,
    COALESCE(m.mine_name, s.shift_name, et.type_name, 'Все') AS dimension_value,
    SUM(fp.tons_mined) AS total_tons,
    sum(fp.trips_count) AS total_trips,
    ROUND(SUM(fp.tons_mined)/sum(fp.trips_count),2) AS avg_tons_per_trip
FROM fact_production fp 
JOIN dim_mine m ON fp.mine_id=m.mine_id 
JOIN dim_shift s ON fp.shift_id=s.shift_id 
JOIN dim_equipment de ON fp.equipment_id=de.equipment_id 
JOIN dim_equipment_type et ON de.equipment_type_id=et.equipment_type_id 
WHERE fp.date_id BETWEEN 20240101 AND 20240131
GROUP BY GROUPING SETS (
	(m.mine_name),
	(s.shift_name),
	(et.type_name),
	()
)
ORDER BY dimension,dimension_value
--Задание 4. Условная агрегация — PIVOT (среднее)
SELECT 
    CASE 
        WHEN GROUPING(dm.mine_name) = 1 THEN 'ИТОГО' 
        ELSE dm.mine_name 
    END AS mine_name,
    ROUND(AVG(CASE WHEN EXTRACT(MONTH FROM dd.full_date) = 1 THEN foq.fe_content END), 2) AS "Янв",
    ROUND(AVG(CASE WHEN EXTRACT(MONTH FROM dd.full_date) = 2 THEN foq.fe_content END), 2) AS "Фев",
    ROUND(AVG(CASE WHEN EXTRACT(MONTH FROM dd.full_date) = 3 THEN foq.fe_content END), 2) AS "Мар",
    ROUND(AVG(CASE WHEN EXTRACT(MONTH FROM dd.full_date) = 4 THEN foq.fe_content END), 2) AS "Апр",
    ROUND(AVG(CASE WHEN EXTRACT(MONTH FROM dd.full_date) = 5 THEN foq.fe_content END), 2) AS "Май",
    ROUND(AVG(CASE WHEN EXTRACT(MONTH FROM dd.full_date) = 6 THEN foq.fe_content END), 2) AS "Июн",
    ROUND(AVG(foq.fe_content), 2) AS "Среднее за период"
FROM fact_ore_quality foq
JOIN dim_mine dm ON foq.mine_id = dm.mine_id
JOIN dim_date dd ON foq.date_id = dd.date_id
WHERE dd.full_date >= '2024-01-01' AND dd.full_date <= '2024-06-30'
GROUP BY GROUPING SETS (
    (dm.mine_name), 
    ()
)
ORDER BY GROUPING(dm.mine_name), dm.mine_name;
--Задание 5. crosstab — динамический разворот (среднее)
SELECT * FROM crosstab(
    $$
    SELECT 
        de.equipment_name,
        dr.reason_name,
        ROUND(SUM(fd.duration_min) / 60.0, 1) AS duration_hours
    FROM fact_equipment_downtime fd
    JOIN dim_equipment de ON fd.equipment_id = de.equipment_id
    JOIN dim_downtime_reason dr ON fd.reason_id = dr.reason_id
    WHERE fd.date_id BETWEEN 20240101 AND 20240331
    GROUP BY 1, 2
    ORDER BY 1, 2
    $$,
    $$
    SELECT dr.reason_name 
    FROM fact_equipment_downtime fd
    JOIN dim_downtime_reason dr ON fd.reason_id = dr.reason_id
    WHERE fd.date_id BETWEEN 20240101 AND 20240331
    GROUP BY dr.reason_name
    ORDER BY SUM(fd.duration_min) DESC
    LIMIT 5
    $$
) AS ct (
    "Оборудование" TEXT,
    "Плановое техническое обслуживание" numeric,
	"Заправка топливом" numeric,
	"Ожидание транспорта" numeric,
	"Отсутствие оператора" numeric,
	"Ожидание погрузки" numeric        
);
--Задание 6. Комплексный отчёт — ROLLUP + PIVOT + итоги (сложное)
WITH RawReport AS (
    SELECT 
        m.mine_name,
        'Добыча (тонн)' AS metric,
        SUM(CASE WHEN d.month = 1 THEN fp.tons_mined ELSE 0 END) AS jan,
        SUM(CASE WHEN d.month = 2 THEN fp.tons_mined ELSE 0 END) AS feb,
        SUM(CASE WHEN d.month = 3 THEN fp.tons_mined ELSE 0 END) AS mar,
        SUM(fp.tons_mined) AS q1_total,
        GROUPING(m.mine_name) AS is_total
    FROM fact_production fp
    JOIN dim_mine m ON fp.mine_id = m.mine_id
    JOIN dim_date d ON fp.date_id = d.date_id
    WHERE d.year = 2024 AND d.quarter = 1
    GROUP BY ROLLUP(m.mine_name)
    UNION ALL
    SELECT 
        m.mine_name,
        'Простои (час)' AS metric,
        SUM(CASE WHEN d.month = 1 THEN fd.duration_min ELSE 0 END) / 60.0 AS jan,
        SUM(CASE WHEN d.month = 2 THEN fd.duration_min ELSE 0 END) / 60.0 AS feb,
        SUM(CASE WHEN d.month = 3 THEN fd.duration_min ELSE 0 END) / 60.0 AS mar,
        SUM(fd.duration_min) / 60.0 AS q1_total,
        GROUPING(m.mine_name) AS is_total
    FROM fact_equipment_downtime fd
    JOIN dim_equipment de on fd.equipment_id=de.equipment_id
    JOIN dim_mine m ON de.mine_id = m.mine_id
    JOIN dim_date d ON fd.date_id = d.date_id
    WHERE d.year = 2024 AND d.quarter = 1
    GROUP BY ROLLUP(m.mine_name)
)
SELECT 
    CASE WHEN is_total = 1 THEN 'ИТОГО' ELSE mine_name END AS "Шахта",
    metric AS "Метрика",
    ROUND(jan, 1) AS "Янв",
    ROUND(feb, 1) AS "Фев",
    ROUND(mar, 1) AS "Мар",
    ROUND(q1_total, 1) AS "Q1 Итого",
    CASE WHEN jan > 0 
         THEN ROUND(((feb - jan) / jan) * 100, 1) 
         ELSE 0 END AS "diff_feb_jan_%",
    CASE WHEN feb > 0 
         THEN ROUND(((mar - feb) / feb) * 100, 1) 
         ELSE 0 END AS "diff_mar_feb_%",
    CASE 
        WHEN feb = 0 THEN 'нет данных'
        WHEN ((mar - feb) / feb) * 100 > 5 THEN 'рост'
        WHEN ((mar - feb) / feb) * 100 < -5 THEN 'снижение'
        ELSE 'стабильно'
    END AS "Тренд"
FROM RawReport
ORDER BY metric, is_total, mine_name;
