# pydata global 2020 

Experiments for conference talk - note this is not meant for others to read...

Conda Environment: pydata2020global

# Experiments

* can I load raw CSV unchanged into pandas? YES, need to specify column headers, takes 22GB!
  * RAM usage if I specify dtypes?
* can I load CSV into Parquet
  * using 4 cpu does it load faster?


# Fields

* pt detached, semidetached, terraced, flat/maisonette, other
* new build, old build
* type freehold leasehold
* transaction category standard or additional (?)


# how quickly can i do value counts in pandas? df.county.value_counts() (raw data) circa 4s.  for df.groupby(['county', 'pt'])['price'].count() circa 3s
# get nbr rows DONE
# get min/max price
# calculate a postal area column
# get min/max price by postal area
# make a year column
# get min/max/median price by year or by county

# $ du -hs parq/ # show disk usage
# $ ls -lta parq/ | wc # show e.g. 78 files made



# CSV export

https://sqlite.org/cli.html
could do sqlite3 .import

CREATE TABLE landreg (
	"index" BIGINT, 
	tin TEXT, 
	price BIGINT, 
	date DATETIME, 
	postcode TEXT, 
	pt TEXT, 
	new TEXT, 
	duration TEXT, 
	paon TEXT, 
	saon TEXT, 
	street TEXT, 
	locality TEXT, 
	town TEXT, 
	district TEXT, 
	county TEXT, 
	ppd_cat TEXT, 
	status TEXT
)

CREATE INDEX county_idx on landreg(county);

CREATE INDEX date_idx on landreg(date);

select * from landreg where town='BROMLEY' limit 5;
select avg(price) from landreg group by 'county'; # single result, 10sec or so
select avg(price) from landreg group by 'county' order by county;
select county, avg(price) from landreg group by county; # many results, circa 19s no index, circa 60s (!) with index
# e.g. YORK|181369.597938144
create index county_idx on landreg(county);
CREATE INDEX county_idx on landreg(county);

pragma index_list(landreg); # shows county_idx and ix_landreg_index

with index
select distinct(county) from landreg;
is noticeably faster
note sqlite3 stays at 9MB (?)

.schema landreg # show e.g. bigint for price


select county, count(county) from landreg group by county; # circa 3 sec 
# e.g. YORK|95060

drop index county_idx;
select county, count(county) from landreg group by county; # circa 14sec no index


select pt, count(pt) from landreg group by pt; # circa 14s no index
create index pt_index on landreg(pt);
select pt, count(pt) from landreg group by pt; # circa 3s with index

create index county_pt_index on landreg(county,pt);
select county, pt, count(pt) from landreg group by county, pt;  # circa 4 secs with dual index
drop index county_pt_index;
# note we still have both individual indexes
select county, pt, count(pt) from landreg group by county, pt;  # circa 60sec and peak at 400MB
drop index county_idx;
drop index pt_index;
select county, pt, count(pt) from landreg group by county, pt; # circa 45sec, peak 400MB 
