
CREATE OR REPLACE FUNCTION get_hex_pid(widthkm FLOAT, long FLOAT, lat FLOAT)
  RETURNS TEXT AS
$BODY$

DECLARE
  refpnt GEOMETRY;
  refx INTEGER;
  refy INTEGER;
  dx INTEGER;
  dy INTEGER;
  gridx FLOAT;
  gridy FLOAT;
  hex1startx FLOAT;
  hex1starty FLOAT;
  hex2startx FLOAT;
  hex2starty FLOAT;
  offsetx FLOAT;
  offsety FLOAT;

  pnt GEOMETRY;
  x INTEGER;
  y INTEGER;

  x1 INTEGER;
  y1 INTEGER;
  x2 INTEGER;
  y2 INTEGER;

  qtrwidthfloat FLOAT;
  qtrwidth INTEGER;
  halfheight INTEGER;

  output_pid TEXT;

BEGIN

  -- Get height and width parameters of hexagon (in metres) - FLOOR and CEILING are used to get the hexagon size closer to the requested input area
  qtrwidthfloat := widthkm * 1000.0 / 4.0;
  
  qtrwidth := FLOOR(qtrwidthfloat);
  halfheight := CEILING(qtrwidthfloat * sqrt(3.0));

  -- get the reference grid start point
  refpnt = ST_Transform(ST_SetSRID(ST_MakePoint(84.0, -44.0), 4326), 3577);
  refx = ST_X(refpnt)::INTEGER;
  refy = ST_Y(refpnt)::INTEGER;

  -- Convert input coords to points in the working SRID
  pnt = ST_Transform(ST_SetSRID(ST_MakePoint(long, lat), 4326), 3577);

  -- Get input coords in working SRID coords
  x = ST_X(pnt)::INTEGER;
  y = ST_Y(pnt)::INTEGER;

  -- adjust start X/Y to snap to grid (required to ensure hex PIDs are 100% repeatable and hence persistant)
  dx = x - refx;
  dy = y - refy;

  gridx = dx::float / (qtrwidth * 6)::float;
  gridy = dy::float / (halfheight * 2)::float;

  x1 = refx + ((FLOOR(gridx) - 1) * qtrwidth * 6);
  y1 = refy + ((FLOOR(gridy) - 1) * halfheight * 2);

  x2 = refx + ((FLOOR(gridx) + 1) * qtrwidth * 6);
  y2 = refy + ((FLOOR(gridy) + 1) * halfheight * 2);

  -- get the pid that contains the input coordinates
  SELECT (x_series + x_offset)::TEXT || (y_series + y_offset)::TEXT AS pid
    FROM generate_series(x1, x2, (qtrwidth * 6)) AS x_series,
         generate_series(y1, y2, (halfheight * 2)) AS y_series,
         (
           SELECT 0 AS x_offset, 0 AS y_offset, ST_GeomFromText(
             format('POLYGON((0 0, %s %s, %s %s, %s %s, %s %s, %s %s, 0 0))',
               qtrwidth, halfheight,
               qtrwidth * 3, halfheight,
               qtrwidth * 4, 0,
               qtrwidth * 3, halfheight * -1,
               qtrwidth, halfheight * -1
             )
           ) AS geom
           UNION
           SELECT qtrwidth * 3 AS x_offset, halfheight AS y_offset, ST_Translate(
             ST_GeomFromText(
               format('POLYGON((0 0, %s %s, %s %s, %s %s, %s %s, %s %s, 0 0))',
                 qtrwidth, halfheight,
                 qtrwidth * 3, halfheight,
                 qtrwidth * 4, 0,
                 qtrwidth * 3, halfheight * -1,
                 qtrwidth, halfheight * -1
               )
             )
           , qtrwidth * 3, halfheight) as geom
         ) AS sqt
    WHERE ST_Within(pnt, ST_SetSRID(ST_Translate(geom, x_series::FLOAT, y_series::FLOAT), 3577)) INTO output_pid;

  RETURN output_pid;

END$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
