-- SELECT * FROM gnaf.addresses
--   where address = '11 DAVIES STREET'
--   AND locality_name = 'LEICHHARDT';
-- 

drop table if exists gnaf.temp_address_hex_tags;
SELECT gnaf_pid, alias_principal, address, get_hex_pid(0.15, longitude, latitude), latitude, longitude
  INTO gnaf.temp_address_hex_tags
  FROM gnaf.address_principals
  WHERE locality_name = 'LEICHHARDT';

-- select * from gnaf.temp_address_hex_tags order by address limit 100;


select Count(*) from gnaf.temp_address_hex_tags where get_hex_pid is null;





SELECT gnaf_pid, alias_principal, address, get_hex_pid(0.3, longitude, latitude), latitude, longitude FROM gnaf.addresses
  WHERE gnaf_pid = 'GANSW707534024';

