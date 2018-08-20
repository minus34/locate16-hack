
DROP TABLE IF EXISTS hex.{0};
CREATE TABLE hex.{0} (
  x integer NOT NULL,
  y integer NOT NULL,
  count_2012 integer NOT NULL,
  count_2018 integer NOT NULL,
  percent integer NOT NULL,
  geom geometry(Polygon,4326)
) WITH (OIDS=FALSE);
ALTER TABLE hex.{0} OWNER TO postgres;

INSERT INTO hex.{0}
SELECT CASE WHEN left(hex_pid, 1) = '-' THEN split_part(hex_pid, '-', 2)::integer * -1 ELSE split_part(hex_pid, '-', 1)::integer END AS x,
       CASE WHEN left(hex_pid, 1) = '-' THEN split_part(hex_pid, '-', 3)::integer * -1 ELSE split_part(hex_pid, '-', 2)::integer * -1 END AS y,
       count_2012,
       count_2018,
       (count_2018::float / (count_2012 + count_2018)::float * 100.0)::integer AS percent,
       null::geometry(polygon,4326) AS geom
  FROM (
         SELECT hex_pid_0_3 AS hex_pid,
         SUM(CASE WHEN year_added <= 2012 THEN 1 ELSE 0 END) AS count_2012,
         SUM(CASE WHEN year_added > 2012 THEN 1 ELSE 0 END) AS count_2018
         FROM hex.address_hexes
         GROUP BY hex_pid_0_3
  ) AS sqt
  WHERE count_2018 > 4;
  
UPDATE hex.{0} SET geom = get_hex_by_coords(0.3, x, y);

ALTER TABLE hex.{0} ADD CONSTRAINT {0}_pk PRIMARY KEY (x, y);
CREATE INDEX {0}_geom_idx ON hex.{0} USING gist(geom);
ALTER TABLE hex.{0} CLUSTER ON {0}_geom_idx;








-- 
-- 
-- 
-- SELECT * FROM hex.temp limit 1000;
-- 
-- 
-- SELECT * FROM hex.temp WHERE x = 1172329 and y = -1172329;
-- 
-- 1172329, -1172329