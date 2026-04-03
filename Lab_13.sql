--Задание 1. Доля оборудования в общей добыче (простое)
WITH filtered_data AS (
    SELECT 
        fp.date_id,
        fp.shift_id,
        fp.equipment_id,
        fp.tons_mined,
        de.equipment_name
    FROM fact_production fp
    INNER JOIN dim_equipment de ON fp.equipment_id = de.equipment_id
    WHERE fp.date_id = 20240115 
      AND fp.shift_id = 1
),
equipment_tons AS (
    SELECT 
        equipment_name,
        SUM(tons_mined) AS tons_mined
    FROM filtered_data
    GROUP BY equipment_name
),
total_tons_calc AS (
    SELECT SUM(tons_mined) AS total_tons
    FROM equipment_tons
)
SELECT 
    et.equipment_name,
    et.tons_mined AS tons,
    ttc.total_tons,
    ROUND((et.tons_mined / NULLIF(ttc.total_tons, 0)) * 100, 2) AS pct
FROM equipment_tons et
CROSS JOIN total_tons_calc ttc
ORDER BY et.tons_mined DESC;
--Задание 2. Нарастающий итог по шахтам (простое)
WITH daily_tons AS (
    SELECT 
        dm.mine_name,
        dd.full_date,
        SUM(fp.tons_mined) AS daily_tons
    FROM fact_production fp
    INNER JOIN dim_mine dm ON fp.mine_id = dm.mine_id
    INNER JOIN dim_date dd ON fp.date_id = dd.date_id
    WHERE EXTRACT(YEAR FROM dd.full_date) = 2024 
      AND EXTRACT(MONTH FROM dd.full_date) = 1
    GROUP BY dm.mine_name, dd.full_date
),
running_total_calc AS (
    SELECT 
        mine_name,
        full_date,
        daily_tons,
        SUM(daily_tons) OVER (
            PARTITION BY mine_name 
            ORDER BY full_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_total
    FROM daily_tons
)
SELECT 
    mine_name,
    full_date,
    ROUND(daily_tons, 2) AS daily_tons,
    ROUND(running_total, 2) AS running_total
FROM running_total_calc
ORDER BY mine_name, full_date;
--Задание 3. Скользящее среднее расхода ГСМ
WITH daily_fuel AS (
    SELECT 
        dd.full_date,
        SUM(fp.fuel_consumed_l) AS daily_fuel
    FROM fact_production fp
    INNER JOIN dim_date dd ON fp.date_id = dd.date_id
    INNER JOIN dim_mine dm ON fp.mine_id = dm.mine_id
    WHERE EXTRACT(YEAR FROM dd.full_date) = 2024 
      AND dd.quarter = 1
      AND dm.mine_id = 1
    GROUP BY dd.full_date
),
moving_averages AS (
    SELECT 
        full_date,
        daily_fuel,
        AVG(daily_fuel) OVER (
            ORDER BY full_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS ma_7,
        AVG(daily_fuel) OVER (
            ORDER BY full_date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS ma_14
    FROM daily_fuel
)
SELECT 
    full_date,
    ROUND(daily_fuel, 2) AS daily_fuel,
    ROUND(ma_7, 2) AS ma_7,
    ROUND(ma_14, 2) AS ma_14
FROM moving_averages
ORDER BY full_date;
--Задание 4. Рейтинг операторов по типам оборудования (среднее)
WITH operator_stats AS (
    SELECT 
        do.full_name AS operator_name,
        det.type_name AS equipment_type,
        SUM(fp.tons_mined) AS total_tons
    FROM fact_production fp
    INNER JOIN dim_date dd ON fp.date_id = dd.date_id
    INNER JOIN dim_operator do ON fp.operator_id = do.operator_id
    INNER JOIN dim_equipment det ON fp.equipment_id = det.equipment_id
    WHERE EXTRACT(YEAR FROM dd.full_date) = 2024 
      AND EXTRACT(MONTH FROM dd.full_date) <= 6
    GROUP BY do.full_name, det.type_name
),
ranked AS (
    SELECT 
        operator_name,
        equipment_type,
        total_tons,
        DENSE_RANK() OVER (
            PARTITION BY equipment_type 
            ORDER BY total_tons DESC
        ) AS rnk
    FROM operator_stats
)
SELECT 
    operator_name,
    equipment_type,
    ROUND(total_tons, 2) AS total_tons,
    rnk
FROM ranked
WHERE rnk <= 5
ORDER BY equipment_type, rnk;
--Задание 5. Сравнение дневной и ночной смены
WITH shift_data AS (
    SELECT 
        dd.full_date,
        ds.shift_name,
        fp.shift_id,
        SUM(fp.tons_mined) AS shift_tons,
        SUM(SUM(fp.tons_mined)) OVER (
            PARTITION BY fp.date_id
        ) AS daily_tons
    FROM fact_production fp
    INNER JOIN dim_mine dm ON fp.mine_id = dm.mine_id
    INNER JOIN dim_date dd ON fp.date_id = dd.date_id
    INNER JOIN dim_shift ds ON fp.shift_id = ds.shift_id
    WHERE dm.mine_id = 1
      AND EXTRACT(YEAR FROM dd.full_date) = 2024
      AND EXTRACT(MONTH FROM dd.full_date) = 1
    GROUP BY dd.full_date, ds.shift_name, fp.shift_id, fp.date_id
)
SELECT 
    full_date,
    shift_name,
    shift_id,
    ROUND(shift_tons, 2) AS shift_tons,
    ROUND((shift_tons * 100.0) / daily_tons, 1) AS pct_of_day
FROM shift_data
ORDER BY full_date, shift_id;
--Задание 8. ТОП-3 рекордных дня для каждой единицы оборудования (среднее)
WITH daily_by_equip AS (
    SELECT 
        de.equipment_name,
        de.type_name,
        dd.full_date,
        SUM(fp.tons_mined) AS daily_tons
    FROM fact_production fp
    INNER JOIN dim_date dd ON fp.date_id = dd.date_id
    INNER JOIN dim_equipment de ON fp.equipment_id = de.equipment_id
    WHERE EXTRACT(YEAR FROM dd.full_date) = 2024
    GROUP BY de.equipment_name, de.type_name, dd.full_date
),
ranked AS (
    SELECT 
        equipment_name,
        type_name,
        full_date,
        daily_tons,
        ROW_NUMBER() OVER (
            PARTITION BY equipment_name 
            ORDER BY daily_tons DESC
        ) AS record_num
    FROM daily_by_equip
),
top3 AS (
    SELECT 
        equipment_name,
        type_name,
        full_date,
        daily_tons,
        record_num,
        MAX(daily_tons) OVER (
            PARTITION BY equipment_name
        ) AS top1_tons
    FROM ranked
    WHERE record_num <= 3
)
SELECT 
    equipment_name,
    type_name,
    full_date,
    ROUND(daily_tons, 2) AS daily_tons,
    record_num,
    ROUND(top1_tons - daily_tons, 2) AS diff_from_top1
FROM top3
ORDER BY equipment_name, record_num;
--Задание 9. Парето-анализ причин простоев (сложное)
WITH reason_totals AS (
    SELECT 
        ddr.reason_name,
        SUM(fed.duration_min) / 60.0 AS total_hours,
        SUM(fed.duration_min) AS total_minutes
    FROM fact_equipment_downtime fed
    INNER JOIN dim_date dd ON fed.date_id = dd.date_id
    INNER JOIN dim_downtime_reason ddr ON fed.reason_id = ddr.reason_id
    WHERE EXTRACT(YEAR FROM dd.full_date) = 2024 
      AND EXTRACT(MONTH FROM dd.full_date) <= 6
    GROUP BY ddr.reason_name
),
total_sum AS (
    SELECT SUM(total_minutes) AS grand_total_minutes
    FROM reason_totals
),
with_pct AS (
    SELECT 
        rt.reason_name,
        rt.total_hours,
        (rt.total_minutes * 100.0) / ts.grand_total_minutes AS pct
    FROM reason_totals rt
    CROSS JOIN total_sum ts
),
with_cumulative AS (
    SELECT 
        reason_name,
        total_hours,
        pct,
        SUM(pct) OVER (
            ORDER BY total_hours DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_pct
    FROM with_pct
)
SELECT 
    reason_name,
    ROUND(total_hours, 2) AS total_hours,
    ROUND(pct, 1) AS pct,
    ROUND(cumulative_pct, 1) AS cumulative_pct,
    CASE 
        WHEN cumulative_pct <= 80 THEN 'A'
        WHEN cumulative_pct <= 95 THEN 'B'
        ELSE 'C'
    END AS pareto_category
FROM with_cumulative
ORDER BY total_hours DESC;
