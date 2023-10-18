--How many animals of each type have outcomes--
SELECT td.animal_type, COUNT(distinct af.animal_dim_key) as animal_count
FROM outcomes_fact af
JOIN type_dim td ON af.type_id = td.type_id
GROUP BY td.animal_type;




--How many animals are there with more than 1 outcome--
select COUNT(*) as animals_with_multiple_outcomes
FROM (
    SELECT af.animal_dim_key
    FROM outcomes_fact af
    GROUP BY af.animal_dim_key
    HAVING COUNT(*) > 1
) 




--What are the top 5 months for outcomes--
SELECT
    month AS month_name,
    COUNT(*) AS outcome_count
FROM Date_dim
JOIN outcomes_fact ON Date_dim.date_id = outcomes_fact.date_id
GROUP BY month
ORDER BY outcome_count DESC
LIMIT 5;



--Kitten Senior--

SELECT
    CASE
        WHEN EXTRACT(YEAR FROM AGE(dd.ts, ad.dob)) < 1 THEN 'Kitten'
        WHEN EXTRACT(YEAR FROM AGE(dd.ts, ad.dob)) >= 1 AND EXTRACT(YEAR FROM AGE(dd.ts, ad.dob)) <= 10 THEN 'Adult'
        WHEN EXTRACT(YEAR FROM AGE(dd.ts, ad.dob)) > 10 THEN 'Senior'
    END AS cat_age_group,
    COUNT(*) AS outcome_count
FROM Outcomes_Fact of
JOIN Animal_dim ad ON of.animal_dim_key = ad.animal_dim_key
JOIN Outcome_dim od ON of.outcome_id = od.outcome_id
JOIN Type_dim td ON of.type_id = td.type_id
JOIN Date_dim dd ON of.date_id = dd.date_id
WHERE od.outcome_type = 'Adoption' AND td.animal_type = 'Cat'
GROUP BY cat_age_group;





-- cumulative total of outcomes up to and including this date--
SELECT
    dd.ts AS date,
    od.outcome_type,
    SUM(COUNT(*)) OVER (PARTITION BY od.outcome_type ORDER BY dd.ts) AS cumulative_total
FROM Date_dim AS dd
JOIN outcomes_fact AS of ON dd.date_id = of.date_id
JOIN outcome_dim AS od ON of.outcome_id = od.outcome_id
GROUP BY dd.ts, od.outcome_type
ORDER BY dd.ts, od.outcome_type;