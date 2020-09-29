# mysql chip_comp < exflank/getrsids.sql > exflank/rsids.txt
SELECT id FROM flank
WHERE id NOT IN
    (
    SELECT DISTINCT(id) from flank where flank_seq like '%N%' or flank_seq like '%K%' or flank_seq like '%M%' or flank_seq like '%R%' or flank_seq like '%Y%' or flank_seq like '%S%' or flank_seq like '%W%'
    )
AND id like 'rs%'
GROUP BY id 
HAVING count(*) > 1
ORDER BY id ASC;
