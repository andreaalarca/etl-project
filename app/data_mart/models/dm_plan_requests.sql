SELECT
    pr.request_id,
    c.customer_id,
    c.customer_name,
    c.contact_person,
    c.phone_number,
    c.email,
    c.city,
    c.subcontractor_category,

    pt.plan_type_id,
    pt.plan_type,
    pt.plan_category,
    pt.required_input,
    pt.output_format,
    pt.complexity_level,
    pt.base_price,

    pr.request_date,
    pr.target_date,
    pr.completed_date,
    pr.status,
    pr.priority,
    pr.assigned_coordinator,
    pr.floor_area_sqm,
    pr.revision_count

FROM data_lake.plan_requests pr
INNER JOIN data_lake.customers c
    ON pr.customer_id = c.customer_id
INNER JOIN data_lake.construction_plan_types pt
    ON pr.plan_type_id = pt.plan_type_id
WHERE pr.request_date = CAST('{{ process_date }}' AS DATE)