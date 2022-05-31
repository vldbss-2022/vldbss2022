CREATE TABLE `title` (                    
  `id` int(11) NOT NULL,                  
  `title` varchar(512) NOT NULL,          
  `imdb_index` varchar(5) DEFAULT NULL,   
  `kind_id` int(11) NOT NULL,             
  `production_year` int(11) DEFAULT NULL, 
  `imdb_id` int(11) DEFAULT NULL,         
  `phonetic_code` varchar(5) DEFAULT NULL,
  `episode_of_id` int(11) DEFAULT NULL,   
  `season_nr` int(11) DEFAULT NULL,       
  `episode_nr` int(11) DEFAULT NULL,      
  `series_years` varchar(49) DEFAULT NULL,
  `md5sum` varchar(32) DEFAULT NULL,      
  PRIMARY KEY (`id`),                     
  KEY `kind_id_title` (`kind_id`),        
  KEY `idx_year` (`production_year`));
['kind_id', 'production_year', 'episode_of_id', 'season_nr', 'episode_nr']

TableScan
    SELECT * FROM imdb.title USE INDEX(primary) WHERE id>? AND id<? AND predicates;

IndexScan
    SELECT production_year FROM imdb.title USE INDEX(idx_year) WHERE production_year>? AND production_year<?;
    SELECT kind_id FROM imdb.title USE INDEX(kind_id_title) WHERE kind_id>? AND kind_id<?;

IndexLookup
    SELECT * FROM imdb.title USE INDEX(idx_year) WHERE production_year>? AND production_year<? AND predicates;
    SELECT * FROM imdb.title USE INDEX(kind_id_title) WHERE kind_id>? AND kind_id<? AND predicates;

HashAgg
    SELECT production_year, count(*) FROM imdb.title WHERE production_year>? AND production_year<? AND predicates;
    
Sort
    SELECT * FROM imdb.title USE INDEX(idx_year) WHERE production_year>? AND production_year<? AND predicates ORDER BY production_year;

HashJoin
    SELECT /*+ HASH_JOIN(t1, t2) */ * FROM imdb.title t1, imdb.title t2 WHERE t1.production_year = t2.production_year AND predicates(t1) AND predicates(t2);

IndexJoin
    SELECT /*+ INL_JOIN(t1, t2) */ * FROM imdb.title t1, imdb.title t2 WHERE t1.production_year = t2.production_year AND predicates(t1) AND predicates(t2);


Considered Operators:
    TableScan       ✅
    TableReader     ✅
    IndexScan
    IndexReader
    Selection       ✅
    Projection      ✅
    HashJoin
    IndexJoin
    HashAgg         
    IndexLookup
    Sort
