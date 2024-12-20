
-- req10
WITH route_summary AS (
    SELECT 
        transport_type, 
        route_id, 
        COUNT(passenger_name) AS passenger_count
    FROM results_trips
    GROUP BY transport_type, route_id
)
SELECT 
    transport_type, 
    COUNT(DISTINCT route_id) AS total_routes, 
    AVG(passenger_count) AS average_passengers_per_route
FROM route_summary
GROUP BY transport_type;
