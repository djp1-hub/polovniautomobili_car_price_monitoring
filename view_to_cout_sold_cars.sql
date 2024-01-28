-- public.308_sold source

CREATE OR REPLACE VIEW public.308_sold
AS WITH maxrequestdatetime AS (
         SELECT max("308_prices"."RequestDateTime") AS maxdatetime
           FROM "308_prices"
        )
 SELECT car_id,
    cabin_type,
    engine_type,
    mileage,
    engine_power,
    "Clean_price",
    "City",
    min("RequestDateTime") AS "PublicationDate",
    date_part('day'::text, max("RequestDateTime") - min("RequestDateTime"))::integer AS "AdvertisementDurationInDays",
    st_distancesphere(st_makepoint(20.463682678383943::double precision, 44.73660096582136::double precision), st_geomfromtext(((('POINT('::text || split_part(split_part(coord, ','::text, 2), ']'::text, 1)) || ' '::text) || split_part(split_part(coord, '['::text, 2), ','::text, 1)) || ')'::text, 4326)) / 1000::double precision AS "DistanceFromMeInKm",
        CASE
            WHEN min("RequestDateTime") = (( SELECT maxrequestdatetime.maxdatetime
               FROM maxrequestdatetime)) THEN 1
            ELSE 0
        END AS sold
   FROM "308_prices" p
  GROUP BY car_id, cabin_type, engine_type, mileage, engine_power, "Clean_price", "City", coord;