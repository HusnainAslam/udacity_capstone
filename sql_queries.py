def I94_data_fact_query():
    return """
            SELECT
                ids.cicid AS cicid,
                ids.i94yr AS entry_year,
                ids.i94mon AS entry_month,
                cs.country_code AS origin_country_code,
                ps.port_code AS port_code,
                ids.arrdate AS arrival_date,
                tms.mode_id AS travel_mode_code,
                ss.state_code AS us_state_code,
                ids.depdate AS departure_date,
                ids.i94bir AS age,
                vs.visa_type_id AS visa_type_code,
                ids.occup AS occupation,
                ids.gender AS gender,
                ids.biryear AS birth_year,
                ids.dtaddto AS entry_date,
                ids.airline AS airline,
                ids.admnum AS admission_number,
                ids.fltno AS flight_number,
                ids.visatype AS visa_type
            FROM tbl_I94_data_stage ids
                LEFT JOIN tbl_county_stage cs ON cs.country_code = ids.i94res
                LEFT JOIN tbl_port_stage ps ON ps.port_code = ids.i94port
                LEFT JOIN tbl_state_stage ss ON ss.state_code = ids.i94addr
                LEFT JOIN tbl_visa_stage vs ON vs.visa_type_id = ids.i94visa
                LEFT JOIN tbl_travel_mode_stage tms ON tms.mode_id = ids.i94mode
            WHERE 
                cs.country_code IS NOT NULL AND
                ps.port_code IS NOT NULL AND
                ss.state_code IS NOT NULL AND
                tms.mode_id IS NOT NULL AND
                vs.visa_type_id IS NOT NULL
        """


def city_demographic_dim_query():
    return """
        with aggcds AS (
            SELECT
                cds.city,
                cds.state_code,
                SUM(cds.male_population) AS male_population,
                SUM(cds.female_population) AS female_population,
                SUM(cds.total_population) AS total_population,
                SUM(cds.number_of_veterans) AS number_of_veterans,
                SUM(cds.foreign_born) AS num_foreign_born
            FROM tbl_cities_demographics_stage cds
            GROUP BY cds.city, cds.state_code
        )
        SELECT tps.port_code, aggcds.male_population, aggcds.female_population, aggcds.total_population, 
            aggcds.number_of_veterans, aggcds.num_foreign_born 
        FROM aggcds 
        JOIN tbl_port_stage tps 
            ON lower(tps.city_name)=lower(aggcds.city) 
            AND tps.state_code=aggcds.state_code
        """
