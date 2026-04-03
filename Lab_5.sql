--Задание 1. Добавление нового оборудования
INSERT INTO practice_dim_equipment(
  equipment_id,equipment_type_id,
  mine_id,equipment_name,
  inventory_number,manufacturer,
  model,year_manufactured,
  commissioning_date,status,
  has_video_recorder,has_navigation
  ) 
            VALUES(200,2,2,'Самосвал МоАЗ-7529','INV-TRK-200','МоАЗ','7529',2025,'2025-03-15','active',true,true);
SELECT * FROM practice_dim_equipment WHERE equipment_id=200;
--Задание 2. Массовая вставка операторов
INSERT INTO practice_dim_operator (
    operator_id,
    tab_number,
    last_name,
    first_name,
    middle_name,
    position,
    qualification,
    hire_date,
    mine_id
)
VALUES 
    (200, 'TAB-200', 'Сидоров', 'Михаил', 'Иванович', 'Машинист ПДМ', '4 разряд', '2025-03-01', 1),
    (201, 'TAB-201', 'Петрова', 'Елена', 'Сергеевна', 'Оператор скипа', '3 разряд', '2025-03-01', 2),
    (202, 'TAB-202', 'Волков', 'Дмитрий', 'Алексеевич', 'Водитель самосвала', '5 разряд', '2025-03-10', 2);

SELECT 
    operator_id,
    tab_number,
    last_name,
    first_name,
    middle_name,
    position,
    qualification,
    hire_date,
    mine_id
FROM practice_dim_operator
WHERE operator_id >= 200
ORDER BY operator_id;
--Задание 3. Загрузка из staging
SELECT COUNT(*) FROM practice_fact_production pfp;
INSERT INTO practice_fact_production
(production_id,
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
loaded_at)
SELECT 3000+staging_id,
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
FROM staging_production WHERE NOT EXISTS (
    SELECT 1 
    FROM practice_fact_production pfp
    WHERE pfp.date_id = staging_production.date_id
        AND pfp.shift_id = staging_production.shift_id
        AND pfp.shaft_id=staging_production.shaft_id
        AND pfp.equipment_id = staging_production.equipment_id
        AND pfp.operator_id = staging_production.operator_id
) AND is_validated=true;

SELECT COUNT(*) FROM practice_fact_production pfp;
--Задание 4. INSERT ... RETURNING с логированием
WITH new_ore_grade AS (
    INSERT INTO practice_dim_ore_grade (
        ore_grade_id,
        grade_name,
        grade_code,
        fe_content_min,
        fe_content_max,
        description
    )
    VALUES (
        300,
        'Экспортный',
        'EXPORT',
        63.00,
        68.00,
        'Руда для экспортных поставок'
    )
    RETURNING ore_grade_id, grade_name, grade_code
)
INSERT INTO practice_equipment_log (
    equipment_id,
    action,
    details
)
SELECT 
    0 AS equipment_id,
    'INSERT' AS action,
    'Добавлен сорт руды: ' || grade_name || ' (' || grade_code || ')' AS details
FROM new_ore_grade;

SELECT * FROM practice_dim_ore_grade WHERE ore_grade_id = 300;
SELECT * FROM practice_equipment_log WHERE details LIKE '%Экспортный%';
--Задание 5. Обновление статуса оборудования (UPDATE)
UPDATE practice_dim_equipment pde 
  SET status='maintenance' 
  WHERE pde.year_manufactured<=2018 
  AND pde.mine_id=1 returning equipment_id,equipment_name,year_manufactured,status;
--Задание 6. UPDATE с подзапросом
UPDATE practice_dim_equipment pde
  SET has_navigation=true
  WHERE pde.equipment_id IN
  (SELECT equipment_id FROM "public".dim_sensor ds JOIN "public".dim_sensor_type dst ON ds.sensor_type_id=dst.sensor_type_id WHERE dst.type_code LIKE '%NAV%')
  AND pde.has_navigation=false;
--Задание 7. DELETE с условием и архивированием
WITH deleted_telemetry AS (
    DELETE FROM practice_fact_telemetry
    WHERE date_id = 20240315
        AND is_alarm = TRUE
    RETURNING *
)
INSERT INTO practice_archive_telemetry (
    telemetry_id,
    date_id,
    time_id,
    equipment_id,
    sensor_id,
    location_id,
    sensor_value,
    is_alarm,
    quality_flag,
    loaded_at,
    archived_at
)
SELECT 
    telemetry_id,
    date_id,
    time_id,
    equipment_id,
    sensor_id,
    location_id,
    sensor_value,
    is_alarm,
    quality_flag,
    loaded_at,
    CURRENT_TIMESTAMP AS archived_at
FROM deleted_telemetry;
--Задание 8. MERGE — синхронизация справочника (PostgreSQL 15+)
WITH max_id AS (
    SELECT COALESCE(MAX(reason_id), 0) AS max_reason_id 
    FROM practice_dim_downtime_reason
)
MERGE INTO practice_dim_downtime_reason AS target
USING (
    SELECT 
        s.reason_name,
        s.reason_code,
        s.category,
        s.description,
        ROW_NUMBER() OVER (ORDER BY s.reason_code) + (SELECT max_reason_id FROM max_id) AS new_id
    FROM staging_downtime_reasons s
) AS source
ON target.reason_code = source.reason_code
WHEN MATCHED THEN
    UPDATE SET
        reason_name = source.reason_name,
        category = source.category,
        description = source.description
WHEN NOT MATCHED THEN
    INSERT (reason_id, reason_name, reason_code, category, description)
    VALUES (source.new_id, source.reason_name, source.reason_code, source.category, source.description);
--Задание 9. UPSERT — идемпотентная загрузка (INSERT ... ON CONFLICT)
INSERT INTO practice_dim_operator (
	operator_id,
    tab_number,
    last_name,
    first_name,
    middle_name,
    position,
    qualification,
    hire_date,
    mine_id,
    status
)
VALUES 
    (100,'TAB-200', 'Сидоров', 'Михаил', 'Иванович', 'Машинист ПДМ', '4 разряд', '2025-03-01', 1, 'active'),
    (101,'TAB-201', 'Петрова', 'Елена', 'Сергеевна', 'Оператор скипа', '3 разряд', '2025-03-01', 2, 'active'),
    (102,'TAB-NEW', 'Волков', 'Дмитрий', 'Алексеевич', 'Водитель самосвала', '5 разряд', '2025-03-10', 2, 'active')
ON CONFLICT (tab_number) 
DO UPDATE SET
    position = EXCLUDED.position,
    qualification = EXCLUDED.qualification;
