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
