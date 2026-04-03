--Задание 1. Анализ длины строковых полей
SELECT 
    *,
    LENGTH(COALESCE(equipment_name, '')) AS name_len,
    LENGTH(COALESCE(inventory_number, '')) AS inv_len,
    LENGTH(COALESCE(model, '')) AS model_len,
    LENGTH(COALESCE(manufacturer, '')) AS manuf_len,
    LENGTH(COALESCE(equipment_name, '')) + LENGTH(COALESCE(inventory_number, '')) + 
    LENGTH(COALESCE(model, '')) + LENGTH(COALESCE(manufacturer, '')) AS total_text_length
FROM 
    dim_equipment
ORDER BY 
    total_text_length DESC;
--Задание 2. Разбор инвентарного номера
SELECT split_part(de.inventory_number,'-',1) as prefix,
	   split_part(de.inventory_number,'-',2) as type_code,
	   cast( split_part(de.inventory_number,'-',3) as integer) as serial_number,
	   (CASE split_part(de.inventory_number,'-',2)
    	WHEN 'LHD' THEN 'Погрузочно-доставочная машина'
    	WHEN 'TRK' THEN 'Шахтный самосвал'
    	WHEN 'CRT' THEN 'Вагонетка'
    	WHEN 'SKP' THEN 'Скиповой подъёмник'
    	ELSE 'Unknown'
		END) as type_code_def 
		from dim_equipment de 
		order by type_code,serial_number;
--Задание 3. Формирование краткого имени оператора
SELECT
    o.last_name || ' ' || 
    UPPER(LEFT(o.first_name, 1)) || '.' || 
    COALESCE(UPPER(LEFT(o.middle_name, 1)) || '.', '') AS short_name_fio,
    UPPER(LEFT(o.first_name, 1)) || '.' || 
    COALESCE(UPPER(LEFT(o.middle_name, 1)) || '.', '') || ' ' || o.last_name AS short_name_ifo,
    UPPER(o.last_name) AS last_name_upper,
    LOWER(o.position) AS position_lower
FROM
    dim_operator AS o
ORDER BY
    o.last_name;
--Задание 4. Поиск оборудования по шаблону
SELECT 'оборудование, в названии которого есть слово «ПДМ»',
   COUNT(*) FROM dim_equipment de WHERE de.equipment_name LIKE '%ПДМ%'
UNION ALL
SELECT 'оборудование производителей, начинающихся на «S»',
   COUNT(*) FROM dim_equipment de WHERE de.manufacturer  LIKE 'S%'
UNION ALL
SELECT 'все шахты, в описании названия которых есть кавычки',
   COUNT(*) FROM dim_mine WHERE mine_name ~ '.*\".+\"'
UNION ALL
SELECT 'инвентарные номера, серийная часть которых содержит только цифры от 001 до 010', 
   COUNT(*) FROM dim_equipment WHERE inventory_number ~ '^.+\-.+\-0(0[1-9]|10)$';
--Задание 5. Список оборудования по шахтам (STRING_AGG)
SELECT dm.mine_name,
	   COUNT(de.equipment_id),
       string_agg(de.equipment_name,', '),
       string_agg(DISTINCT de.manufacturer,', ')
FROM dim_mine dm 
JOIN dim_equipment de ON dm.mine_id=de.mine_id 
GROUP BY dm.mine_name;
--Задание 6. Возраст оборудования
SELECT 
    equipment_name AS "Оборудование",
    commissioning_date AS "Дата ввода",
    AGE(CURRENT_DATE, commissioning_date) AS "Возраст",
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, commissioning_date))::INTEGER AS "Лет",
    EXTRACT(DAY FROM AGE(CURRENT_DATE, commissioning_date))::INTEGER AS "Дней",
    CASE 
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, commissioning_date)) < 2 THEN 'Новое'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, commissioning_date)) <= 4 THEN 'Рабочее'
        ELSE 'Требует внимания'
    END AS "Категория"
FROM 
    dim_equipment
ORDER BY 
    commissioning_date ASC;
--Задание 7. Форматирование дат для отчётов
SELECT to_char(commissioning_date,'DD.MM.YYYY') AS Russian, 
	   to_char(commissioning_date,'DD FMMonth YYYY "year"') AS FULL,
       to_char(commissioning_date,'YYYY-MM-DD') AS ISO,
       to_char(commissioning_date,'YYYY-"Q"Q') AS YearQ,
       to_char(commissioning_date,'DAY') AS DayOfWeek,
       to_char(commissioning_date,'YYYY-MM') AS year_month
FROM dim_equipment de;
--Задание 8. Анализ простоев по дням недели и часам
SELECT 
    TO_CHAR(start_time, 'Day') AS DayOfWeek,
    COUNT(*) AS Count,
    ROUND(AVG(duration_min), 1) AS AverageDuration
FROM fact_equipment_downtime
GROUP BY TO_CHAR(start_time, 'Day'), EXTRACT(DOW FROM start_time)
ORDER BY EXTRACT(DOW FROM start_time);

SELECT 
    EXTRACT(HOUR FROM fad.start_time) as H,
    COUNT(*) AS Count
FROM fact_equipment_downtime fad
GROUP BY EXTRACT(HOUR FROM fad.start_time)
ORDER BY H;

SELECT 
    EXTRACT(HOUR FROM fad.start_time) AS PeakHour,
    COUNT(*) AS Count
FROM fact_equipment_downtime fad
GROUP BY EXTRACT(HOUR FROM fad.start_time)
ORDER BY Count DESC
LIMIT 1;
--Задание 9. Расчёт графика калибровки датчиков
SELECT ds.sensor_code, ds.calibration_date, 
       EXTRACT(day FROM NOW()-ds.calibration_date) AS daysfrompreviouscalibration,
       DATE(ds.calibration_date+interval '180' day) AS nextdateofcalibration,
       (CASE WHEN EXTRACT(day FROM NOW()-ds.calibration_date)>180 THEN 'Просрочено'
       WHEN EXTRACT(day from NOW()-ds.calibration_date)>150 THEN 'Скоро'
       ELSE 'В норме'
       END) AS status
FROM dim_sensor ds;
--Задание 10. Комплексный отчёт: карточка оборудования
SELECT CONCAT(
		CONCAT('[',split_part(de.equipment_name,'-',1),'] '),
		de.equipment_name,' ',
		CONCAT('(',de.manufacturer,' ',de.model,') | '),
		CONCAT(dm.mine_name,' |'),
		CONCAT('Введён: ',to_char(de.commissioning_date,'DD.MM.YYYY'),' | '),
		CONCAT('Возраст: ', CAST(extract(days FROM NOW()-de.commissioning_date)/365 AS integer),' лет | '),
		CONCAT('Статус: ', CASE de.status WHEN 'active' THEN 'АКТИВЕН'
										  WHEN 'maintance' THEN 'НА ТО'
										  WHEN 'decommissioned' THEN 'СПИСАН' END, ' | '),
		CONCAT('Видеорег.: ',CASE de.has_video_recorder WHEN true THEN 'ДА' ELSE 'НЕТ' END,' | '),
		CONCAT('Навигация: ',CASE de.has_navigation WHEN true THEN 'ДА' ELSE 'НЕТ' END)
		)
FROM dim_equipment de JOIN dim_mine dm ON de.mine_id=dm.mine_id;
