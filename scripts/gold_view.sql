    /*
    ===============================================================================
    DDL Script: Create Gold Views
    ===============================================================================
    Script Purpose:
        This script creates views for the Gold layer in the data warehouse. 
        The Gold layer represents the final dimension and fact tables (Star Schema)

        Each view performs transformations and combines data from the Silver layer 
        to produce a clean, enriched, and business-ready dataset.

    Usage:
        - These views can be queried directly for analytics and reporting.
    ===============================================================================
    */

    -- =============================================================================
    -- Create Dimension: gold.fact_tickets
    -- =============================================================================

    IF OBJECT_ID('gold.fact_tickets', 'V') IS NOT NULL
        DROP VIEW gold.fact_tickets;
    GO

    CREATE VIEW gold.fact_tickets AS
    SELECT 
    p.payment_id,
    p.ticket_id,
    t.flight_id,
    t.passenger_id,
    p.payment_method,
    t.purchase_date,
    p.payment_status,
    t.ticket_class,
    t.price
    FROM silver.payments p
    LEFT JOIN silver.tickets t
    ON p.ticket_id = t.ticket_id

    -- ====================================================================
    -- Create Dimension Table: gold.dim_passengers
    -- ====================================================================

    IF OBJECT_ID(gold.dim_passengers 'V') IS NOT NULL 
        DROP VIEW gold.dim_passengers;

    CREATE VIEW gold.dim_passengers AS
    SELECT
        ROW_NUMBER() OVER(ORDER BY passenger_id) AS passenger_key,
        passenger_id,
        first_name,
        last_name,
        birth_date,
        gender,
        nationality
    FROM silver.passengers


    -- ====================================================================
    -- Create Dimension Table: gold.dim_operations
    -- ====================================================================

    IF OBJECT_ID(gold.dim_operations 'V') IS NOT NULL 
        DROP VIEW gold.dim_operations;

    CREATE VIEW gold.dim_operations AS
    SELECT 
        ROW_NUMBER() OVER(ORDER BY operation_id) AS operation_key,
        operation_id,
        flight_id,
        delay_minutes,
        cancellation_flag,
        reason
    FROM silver.operations



    -- ====================================================================
    -- Create Dimension Table: gold.dim_flights
    -- ====================================================================

    IF OBJECT_ID(gold.dim_flights 'V') IS NOT NULL 
        DROP VIEW gold.dim_flights;
    GO
    CREATE VIEW gold.dim_flights AS
    SELECT 
        ROW_NUMBER() OVER(ORDER BY flight_id) AS flight_key,
        f.flight_id,
        f.airline_id,
        a.airline_name,
        f.departure_airport,
        dep.city AS departure_city,
        dep.country AS departure_country,
        f.arrival_airport,
        arr.city AS arrival_city,
        arr.country AS arrival_country,
        a.country AS airline_country,
        f.scheduled_departure,
        f.scheduled_arrival
    FROM silver.flights f
    LEFT JOIN silver.airports dep 
        ON f.departure_airport = dep.airport_code
    LEFT JOIN silver. airports arr 
        ON f.arrival_airport = arr.airport_code
    LEFT JOIN silver.airlines a
        ON a.airline_id = f.airline_id


 



