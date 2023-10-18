1.
SELECT td.animal_type, COUNT(distinct af.animal_dim_key) as animal_count
FROM outcomes_fact af
JOIN type_dim td ON af.type_id = td.type_id
GROUP BY td.animal_type;

2.
select COUNT(*) as animals_with_multiple_outcomes
FROM (
    SELECT af.animal_dim_key
    FROM outcomes_fact af
    GROUP BY af.animal_dim_key
    HAVING COUNT(*) > 1
) 


3.
SELECT
    month AS month_name,
    COUNT(*) AS outcome_count
FROM Date_dim
JOIN outcomes_fact ON Date_dim.date_id = outcomes_fact.date_id
GROUP BY month
ORDER BY outcome_count DESC
LIMIT 5;

4.
SELECT
    CASE
        WHEN EXTRACT(YEAR FROM AGE(dd.ts, ad.dob)) < 1 THEN 'Kitten'
        WHEN EXTRACT(YEAR FROM AGE(dd.ts, ad.dob)) >= 1 AND EXTRACT(YEAR FROM AGE(dd.ts, ad.dob)) <= 10 THEN 'Adult'
        WHEN EXTRACT(YEAR FROM AGE(dd.ts, ad.dob)) > 10 THEN 'Senior'
    END AS age_category,
    COUNT(*) AS count
FROM outcomes_fact AS of
JOIN animal_dim AS ad ON of.animal_dim_key = ad.animal_dim_key
JOIN date_dim AS dd ON of.date_id = dd.date_id
JOIN outcome_dim AS od ON of.outcome_id = od.outcome_id
WHERE od.outcome_type = 'Adopted'
GROUP BY age_category;

5.
SELECT
    dd.ts AS date,
    od.outcome_type,
    SUM(COUNT(*)) OVER (PARTITION BY od.outcome_type ORDER BY dd.ts) AS cumulative_total
FROM Date_dim AS dd
JOIN outcomes_fact AS of ON dd.date_id = of.date_id
JOIN outcome_dim AS od ON of.outcome_id = od.outcome_id
GROUP BY dd.ts, od.outcome_type
ORDER BY dd.ts, od.outcome_type;