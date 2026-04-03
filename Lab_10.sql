--Задание 1. Скалярный подзапрос — фильтрация (простое)
WITH base AS
(SELECT Concat(d_o.last_name,' ',
  LEFT(first_name,1),'. ',
  LEFT(coalesce(middle_name,''),1),'.') AS full_name,SUM(fp.tons_mined) AS tonnage
  FROM fact_production fp
  JOIN dim_operator d_o ON fp.operator_id=d_o.operator_id
  WHERE date_id BETWEEN 20240301 AND 20240331 GROUP BY full_name)
SELECT full_name,tonnage, (select ROUND(AVG(tonnage),2) FROM base) AS avg_tonnage FROM base WHERE tonnage>(SELECT ROUND(AVG(tonnage),2) FROM base);
--Задание 2. Многозначный подзапрос с IN (простое)
SELECT ds.sensor_code, dst.type_name, de.equipment_name, ds.status 
FROM dim_sensor ds 
JOIN dim_sensor_type dst ON ds.sensor_type_id=dst.sensor_type_id 
JOIN dim_equipment de ON ds.equipment_id =de.equipment_id
WHERE de.equipment_id IN (SELECT DISTINCT fp.equipment_id 
							FROM fact_production fp  
							WHERE fp.date_id BETWEEN 20240101 AND 20240331);
--Задание 3. NOT IN и ловушка с NULL (среднее)
SELECT de.equipment_name,dm.mine_name,det.type_name,de.status 
FROM dim_equipment de 
JOIN dim_mine dm ON de.mine_id=dm.mine_id 
JOIN dim_equipment_type det ON de.equipment_type_id=det.equipment_type_id 
WHERE de.equipment_id NOT IN 
  (SELECT DISTINCT fp.equipment_id FROM fact_production fp WHERE fp.equipment_id IS NOT NULL);
--Задание 4. Коррелированный подзапрос — сравнение внутри группы (среднее)
SELECT dm.mine_name,dd.full_date,de.equipment_name,fp.tons_mined,ROUND((SELECT avg(tons_mined) FROM fact_production fp2 WHERE fp.mine_id=fp2.mine_id),2)
FROM fact_production fp 
JOIN dim_date dd ON fp.date_id=dd.date_id 
JOIN dim_mine dm ON dm.mine_id=fp.mine_id 
JOIN dim_equipment de ON fp.equipment_id=de.equipment_id
WHERE fp.tons_mined<(SELECT avg(tons_mined) FROM fact_production fp2 WHERE fp.mine_id=fp2.mine_id) 
ORDER BY abs(fp.tons_mined-(SELECT avg(tons_mined) FROM fact_production fp2 WHERE fp.mine_id=fp2.mine_id)) DESC
LIMIT 20;
--Задание 5. EXISTS — оборудование с тревожными показаниями (среднее)
SELECT de.equipment_name,det.type_name,dm.mine_name,
  (SELECT COUNT(*) FROM fact_equipment_telemetry fet 
  WHERE fet.date_id BETWEEN 20240101 AND 20240331
  AND fet.equipment_id=de.equipment_id AND fet.is_alarm=true) AS alarm_count
FROM dim_equipment de 
JOIN dim_equipment_type det on det.equipment_type_id=de.equipment_type_id 
JOIN dim_mine dm ON dm.mine_id=de.mine_id 
WHERE
  EXISTS(SELECT * FROM fact_equipment_telemetry fet WHERE (fet.date_id BETWEEN 20240101 AND 20240331)
  AND fet.equipment_id=de.equipment_id
  AND fet.is_alarm=true)
ORDER BY alarm_count DESC;
--Задание 6. NOT EXISTS — поиск «пробелов» в данных (среднее)
SELECT dd.full_date, dd.day_of_week, dd.is_weekend
FROM dim_date dd 
WHERE NOT(EXISTS
  (
  SELECT * FROM fact_production fp
  WHERE fp.date_id=dd.date_id AND fp.equipment_id=5))
  AND CAST(dd.date_id AS integer) BETWEEN 20240301 AND 20240331
ORDER BY dd.full_date;
--Задание 7. Подзапрос с ANY/ALL (среднее)
--7.1. ALL
SELECT
    e.equipment_name,
    et.type_name,
    d.full_date,
    s.shift_name,
    fp.tons_mined
FROM fact_production AS fp
JOIN dim_equipment AS e ON fp.equipment_id = e.equipment_id 
JOIN dim_equipment_type AS et ON e.equipment_type_id = et.equipment_type_id
JOIN dim_date AS d ON fp.date_id = d.date_id  
JOIN dim_shift AS s ON fp.shift_id = s.shift_id  
WHERE
    fp.tons_mined > ALL (
        SELECT fp2.tons_mined
        FROM fact_production AS fp2
        JOIN dim_equipment AS e2 ON fp2.equipment_id = e2.equipment_id
        JOIN dim_equipment_type AS et2 ON e2.equipment_type_id = et2.equipment_type_id
        WHERE et2.type_code = 'TRUCK'
    )
ORDER BY fp.tons_mined DESC;
--7.2. (SELECT MAX(...)) 
SELECT
    e.equipment_name,
    et.type_name,
    d.full_date,
    s.shift_name,
    fp.tons_mined
FROM fact_production AS fp 
JOIN dim_equipment AS e ON fp.equipment_id = e.equipment_id
JOIN dim_equipment_type AS et ON e.equipment_type_id = et.equipment_type_id
JOIN dim_date AS d ON fp.date_id = d.date_id 
JOIN dim_shift AS s ON fp.shift_id = s.shift_id
WHERE
    fp.tons_mined > (
        SELECT MAX(fp2.tons_mined)
        FROM fact_production AS fp2
        JOIN dim_equipment AS e2 ON fp2.equipment_id = e2.equipment_id
        JOIN dim_equipment_type AS et2 ON e2.equipment_type_id = et2.equipment_type_id
        WHERE et2.type_code = 'TRUCK'
    )
ORDER BY fp.tons_mined DESC;
--7.3. ANY (SELECT MIN(...))
SELECT
    e.equipment_name,
    et.type_name,
    d.full_date,
    s.shift_name,
    fp.tons_mined
FROM fact_production AS fp
JOIN dim_equipment AS e ON fp.equipment_id = e.equipment_id
JOIN dim_equipment_type AS et ON e.equipment_type_id = et.equipment_type_id
JOIN dim_date AS d ON fp.date_id = d.date_id
JOIN dim_shift AS s ON fp.shift_id = s.shift_id
WHERE
    fp.tons_mined > ANY (
        SELECT fp2.tons_mined
        FROM fact_production AS fp2
        JOIN dim_equipment AS e2 ON fp2.equipment_id = e2.equipment_id
        JOIN dim_equipment_type AS et2 ON e2.equipment_type_id = et2.equipment_type_id
        WHERE et2.type_code = 'TRUCK'
    )
ORDER BY fp.tons_mined DESC;
