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
    KEY `idx_year` (`production_year`),
    KEY `idx1` (`production_year`,`kind_id`,`season_nr`),
    KEY `idx2` (`kind_id`,`production_year`,`episode_nr`)
);