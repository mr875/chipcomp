DROP TABLE IF EXISTS consensus;
CREATE TABLE consensus (
  id char(38) NOT NULL,
  snp_id char(38) DEFAULT NULL,
  chr char(4) DEFAULT NULL,
  GRCh37_pos int unsigned DEFAULT NULL,
  GRCh38_pos int unsigned DEFAULT NULL,
  flank_seq varchar(1040) DEFAULT NULL,
  flank_strand char(5) DEFAULT NULL,
  probe_seq varchar(1040) DEFAULT NULL,
  probe_strand char(5) DEFAULT NULL,
  uid_datasource varchar(40) NOT NULL,
  PRIMARY KEY (id)
);
DROP TABLE IF EXISTS match_count;
CREATE TABLE match_count(
    id char(38) NOT NULL,
    tabl char(20) NOT NULL,
    match_value varchar(1040) NOT NULL,
    datasource varchar(40) NOT NULL,
    PRIMARY KEY (id,match_value,datasource)
);

DROP TABLE IF EXISTS alt_ids;
CREATE TABLE alt_ids(
	id char(38) NOT NULL,
	alt_id char(86) NOT NULL,
	datasource varchar(40) NOT NULL,
	PRIMARY KEY (id,alt_id,datasource)
);
DROP TABLE IF EXISTS positions;
CREATE TABLE positions(
	id char(38) NOT NULL,
	chr char(4) NOT NULL,
	pos int unsigned NOT NULL,
	build char(6) NOT NULL,
	datasource varchar(40) NOT NULL,
	match_count tinyint unsigned DEFAULT 1,
	chosen boolean default false,
	PRIMARY KEY(id,chr,pos,build,datasource)
);
DROP TABLE IF EXISTS probes;
CREATE TABLE probes(
	id char(38) NOT NULL,
	colname varchar(40) NOT NULL,
	datasource varchar (40) NOT NULL,
	probe_seq varchar(1040) DEFAULT NULL,
	probe_strand char(5) DEFAULT NULL,
	multiple boolean default true,
	match_count tinyint unsigned DEFAULT 1,
	chosen boolean default false,
	PRIMARY KEY(id,colname,datasource)
);
DROP TABLE IF EXISTS flank;
CREATE TABLE flank(
	id char(38) NOT NULL,
	colname varchar(40) NOT NULL,
	datasource varchar(40) NOT NULL,
	flank_seq varchar(1040) DEFAULT NULL,
	flank_strand char(5) DEFAULT NULL,
	multiple boolean default true,
	match_count tinyint unsigned DEFAULT 1,
	chosen boolean default false,
	PRIMARY KEY(id,colname,datasource)
);
DROP TABLE IF EXISTS snp_present;
CREATE TABLE snp_present(
	id char(38) NOT NULL,
	datasource varchar(40) NOT NULL,
	chipname varchar(40) DEFAULT NULL,
    match_crit char(10) DEFAULT NULL,
	PRIMARY KEY (id,datasource)
);	
