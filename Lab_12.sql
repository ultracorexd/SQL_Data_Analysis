--Задание 1. UNION ALL — объединённый журнал событий (простое)
SELECT 'Добыча' AS event_type,de.equipment_name ,fp.tons_mined AS VALUE, 'тонны' AS unit
FROM fact_production fp 
JOIN dim_equipment de ON fp.equipment_id =de.equipment_id 
WHERE fp.date_id=20240315
UNION ALL
SELECT 'Простой' AS event_type,de1.equipment_name ,fed.duration_min  AS VALUE, 'мин' AS unit
FROM fact_equipment_downtime fed  
JOIN dim_equipment de1 ON fed.equipment_id =de1.equipment_id 
WHERE fed.date_id=20240315;
--Задание 2. UNION — уникальные шахты с активностью (простое)
WITH all_events AS (
SELECT fp.mine_id AS mine_id
FROM fact_production fp 
UNION
SELECT de.mine_id AS mine_id
FROM fact_equipment_downtime fed 
JOIN dim_equipment de ON fed.equipment_id=de.equipment_id 
WHERE fed.date_id BETWEEN 20240101 AND 20240331
)
SELECT COUNT(DISTINCT dm.mine_name) FROM all_events JOIN dim_mine dm ON all_events.mine_id=dm.mine_id;
--Задание 3. EXCEPT — оборудование без данных о качестве (среднее)
WITH unique_equipment AS (SELECT DISTINCT equipment_id
FROM fact_production fp 
WHERE fp.date_id BETWEEN 20240101 AND 20240331
EXCEPT
SELECT DISTINCT equipment_id
FROM fact_ore_quality foq 
LEFT JOIN fact_production fp ON foq.mine_id=fp.mine_id AND foq.shaft_id=fp.shaft_id AND foq.date_id=fp.date_id
WHERE foq.date_id BETWEEN 20240101 AND 20240331)
SELECT equipment_name,type_name
FROM unique_equipment ue
JOIN dim_equipment de ON ue.equipment_id=de.equipment_id 
JOIN dim_equipment_type det ON de.equipment_type_id =det.equipment_type_id ;
--Задание 4. INTERSECT — операторы на нескольких типах оборудования (среднее)
WITH MultiSkillOps AS (
    SELECT fp.operator_id
    FROM fact_production fp
    JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    WHERE et.type_code = 'LHD'
    INTERSECT
    SELECT fp.operator_id
    FROM fact_production fp
    JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    WHERE et.type_code = 'TRUCK'
)
SELECT 
    d_o.last_name || ' ' || LEFT(d_o.first_name, 1) || '.' AS fio,
    d_o.position,
    d_o.qualification,
    COUNT(*) OVER() AS universal_count,
    (SELECT COUNT(DISTINCT operator_id) FROM dim_operator) AS total_operators,
    ROUND(
        (COUNT(*) OVER() * 100.0 / (SELECT COUNT(DISTINCT operator_id) FROM dim_operator))::numeric, 
        2
    ) AS pct_of_all
FROM dim_operator d_o
JOIN MultiSkillOps mso ON d_o.operator_id = mso.operator_id;
--Задание 5. Диаграмма Венна: комплексный анализ (среднее)
WITH MultiSkillOps AS (
    SELECT fp.operator_id
    FROM fact_production fp
    JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    WHERE et.type_code = 'LHD'
    INTERSECT
    SELECT fp.operator_id
    FROM fact_production fp
    JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    WHERE et.type_code = 'TRUCK'
),
OnlyLHD AS (
    SELECT fp.operator_id
    FROM fact_production fp
    JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    WHERE et.type_code = 'LHD'
    EXCEPT
    SELECT fp.operator_id
    FROM fact_production fp
    JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    WHERE et.type_code = 'TRUCK'
),
OnlyTruck as (
	SELECT fp.operator_id
    FROM fact_production fp
    JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    WHERE et.type_code = 'TRUCK'
    EXCEPT
	SELECT fp.operator_id
    FROM fact_production fp
    JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    WHERE et.type_code = 'LHD'
),
otchet AS (
SELECT operator_id, 'Оба типа' AS operator_type
FROM MultiSkillOps
UNION ALL
SELECT operator_id, 'Только ПДМ' AS operator_type
FROM OnlyLHD
UNION ALL
SELECT operator_id, 'Только самосвал' AS operator_type
FROM OnlyTruck
)
SELECT operator_type,COUNT(*), ROUND(100*COUNT(*)/(SELECT COUNT(*) FROM otchet),1) FROM otchet GROUP BY operator_type;
--Задание 6. LATERAL — топ-N записей для каждой группы (среднее)
SELECT m.mine_name, top5.*
FROM dim_mine m
CROSS JOIN LATERAL (
    SELECT dd.full_date, e.equipment_name, ddr.reason_name, fd.duration_min, ROUND(fd.duration_min/60,0) as duration_hours
    FROM fact_equipment_downtime fd
    JOIN dim_equipment e on fd.equipment_id =e.equipment_id 
    JOIN dim_date dd on fd.date_id=dd.date_id
    JOIN dim_downtime_reason ddr on fd.reason_id=ddr.reason_id 
    WHERE e.mine_id = m.mine_id 
      AND fd.is_planned = FALSE
      AND fd.date_id BETWEEN 20240101 AND 20240331
    ORDER BY fd.duration_min DESC
    LIMIT 5
) top5
WHERE m.status = 'active'
ORDER BY m.mine_name,top5.duration_min;
--Задание 7. LEFT JOIN LATERAL — последнее показание для каждого датчика (сложное)
SELECT ds.sensor_code,
	   dst.type_name,
	   de.equipment_name,
	   last_sensor_value.full_date,
	   last_sensor_value.time_id,
	   last_sensor_value.sensor_value,
	   last_sensor_value.is_alarm
FROM dim_sensor ds
JOIN dim_sensor_type dst ON ds.sensor_type_id=dst.sensor_type_id 
JOIN dim_equipment de ON de.equipment_id=ds.equipment_id
LEFT JOIN LATERAL (
			SELECT dd.full_date,fet.time_id,fet.sensor_value,fet.is_alarm 
			FROM fact_equipment_telemetry fet 
			JOIN dim_date dd ON fet.date_id=dd.date_id 
			WHERE fet.sensor_id=ds.sensor_id
			ORDER BY fet.date_id DESC
			LIMIT 1
		) last_sensor_value ON true
ORDER BY last_sensor_value.full_date,last_sensor_value.time_id;
