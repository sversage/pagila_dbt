

  create  table "source_analytics"."public"."rental_stats__dbt_tmp"
  as (
    /*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/



WITH enriched_data as (
    SELECT *,
        date_part('week', to_date(r.rental_date, 'YYYY/MM/DD')) AS rental_week,
        date_part('year', to_date(r.rental_date, 'YYYY/MM/DD'))  AS rental_year,
        date_part('week', to_date(r.return_date, 'YYYY/MM/DD')) AS return_week,
        date_part('year', to_date(r.return_date, 'YYYY/MM/DD')) AS return_year
    FROM rental r 
), rental_stats as (
    SELECT 
        CASE 
            WHEN agg_returns.WeekBeginning ISNULL THEN open_rentals.WeekBeginning
            ELSE agg_returns.WeekBeginning
        END as WeekBeginning,
        agg_returns.ReturnedRentals,
        open_rentals.OutstandingRentals

    FROM (
        SELECT returned_info.WeekBeginning, SUM(returned_info.ReturnedRentals) AS ReturnedRentals
        FROM(
                SELECT date_trunc('week', return_date::timestamp)::date AS WeekBeginning, count(*) AS ReturnedRentals
                FROM enriched_data er 
                WHERE rental_week = return_week and rental_year = return_year 
                GROUP BY date_trunc('week', return_date::timestamp)::date
            UNION ALL
                SELECT date_trunc('week', return_date::timestamp)::date AS WeekBeginning, count(*) AS ReturnedRentals
                FROM enriched_data er 
                WHERE rental_week != return_week
                GROUP BY date_trunc('week', return_date::timestamp)::date
                ) returned_info
        GROUP BY returned_info.WeekBeginning ) AS agg_returns
    FULL JOIN (
        SELECT date_trunc('week', rental_date ::timestamp)::date AS WeekBeginning, count(*) AS OutstandingRentals
        FROM enriched_data er 
        WHERE rental_week != return_week or return_week ISNULL
        GROUP BY date_trunc('week', rental_date ::timestamp)::date
    ) AS open_rentals ON open_rentals.WeekBeginning = agg_returns.WeekBeginning
    ORDER BY WeekBeginning
)


SELECT * 
FROM rental_stats
  );