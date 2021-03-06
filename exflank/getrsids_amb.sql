# mysql chip_comp < exflank/getrsids_amb.sql > exflank/rsids.txt
SELECT id FROM flank
WHERE id IN
        (
        SELECT DISTINCT(id) from flank where flank_seq like '%N%' or flank_seq like '%K%' or flank_seq like '%M%' or flank_seq like '%R%' or flank_seq like '%Y%' or flank_seq like '%S%' or flank_seq like '%W%'
        )
AND id NOT IN
        (
        SELECT DISTINCT(id) FROM flank WHERE chosen = 1
        )
AND id like 'rs%'
GROUP BY id 
HAVING count(*) > 1
ORDER BY id ASC;
