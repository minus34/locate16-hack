
CREATE OR REPLACE FUNCTION get_hex_by_coords(widthkm FLOAT, x INTEGER, y INTEGER)
  RETURNS geometry AS
$BODY$

DECLARE

  qtrwidthfloat FLOAT;
  qtrwidth INTEGER;
  halfheight INTEGER;


BEGIN

  -- Get height and width parameters of hexagon (in metres) - FLOOR and CEILING are used to get the hexagon size closer to the requested input area
  qtrwidthfloat := widthkm * 1000.0 / 4.0;
  
  qtrwidth := FLOOR(qtrwidthfloat);
  halfheight := CEILING(qtrwidthfloat * sqrt(3.0));

  -- Return the hexagon
  RETURN (
    SELECT ST_Transform(ST_SetSRID(ST_Translate(geom, x::FLOAT, y::FLOAT), 3577), 4326) AS hex
      FROM (
             SELECT ST_GeomFromText(
               format('POLYGON((0 0, %s %s, %s %s, %s %s, %s %s, %s %s, 0 0))',
                 qtrwidth, halfheight,
                 qtrwidth * 3, halfheight,
                 qtrwidth * 4, 0,
                 qtrwidth * 3, halfheight * -1,
                 qtrwidth, halfheight * -1
               )
             ) AS geom
           ) AS two_hex
  );

END$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
