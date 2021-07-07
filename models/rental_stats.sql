
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

WITH enriched_data as (
    SELECT *,
        date_part('week', to_date(r.rental_date, 'YYYY/MM/DD')) AS rental_week,
        date_part('year', to_date(r.rental_date, 'YYYY/MM/DD'))  AS rental_year,
        date_part('week', to_date(r.return_date, 'YYYY/MM/DD')) AS return_week,
        date_part('year', to_date(r.return_date, 'YYYY/MM/DD')) AS return_year
    FROM rental r 
), 
weekly_return_count as (
	    SELECT date_trunc('week', return_date::timestamp)::date AS WeekBeginning, count(*) AS ReturnedRentals
	    FROM enriched_data er 
	    WHERE rental_week = return_week and rental_year = return_year 
	    GROUP BY date_trunc('week', return_date::timestamp)::date
	UNION ALL
	    SELECT date_trunc('week', return_date::timestamp)::date AS WeekBeginning, count(*) AS ReturnedRentals
	    FROM enriched_data er 
	    WHERE rental_week != return_week
	    GROUP BY date_trunc('week', return_date::timestamp)::date
), 
aggregate_returns AS (
    SELECT WeekBeginning, SUM(ReturnedRentals) AS ReturnedRentals
    FROM weekly_return_count
    GROUP BY WeekBeginning
), 
aggregate_outstanding_rentals AS (
    SELECT date_trunc('week', rental_date ::timestamp)::date AS WeekBeginning, count(*) AS OutstandingRentals
    FROM enriched_data er 
    WHERE rental_week != return_week or return_week ISNULL
    GROUP BY date_trunc('week', rental_date ::timestamp)::date
), 
rental_stats as (
    SELECT 
        CASE 
            WHEN aggregate_returns.WeekBeginning ISNULL THEN open_rentals.WeekBeginning
            ELSE aggregate_returns.WeekBeginning
        END as WeekBeginning,
        aggregate_returns.ReturnedRentals,
        open_rentals.OutstandingRentals

    FROM aggregate_returns 
    FULL JOIN aggregate_outstanding_rentals AS open_rentals ON open_rentals.WeekBeginning = aggregate_returns.WeekBeginning
    ORDER BY WeekBeginning
)


SELECT * 
FROM rental_stats