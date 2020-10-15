SELECT id, CONCAT(chr,':',pos) FROM positions WHERE build = '38' AND id NOT LIKE 'rs%' AND chr <> '0';
