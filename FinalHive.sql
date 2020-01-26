drop database if exists group5 cascade;
create database group5;
use group5;

set hive.cli.print.header=true;
set hive.resultset.use.unique.column.names;
set hive.resultset.use.unique.column.names=false;
set hive.resultset.use.unique.column.names;

create external table link( movieid int,imdbid int,tmdbid int ) stored as orc LOCATION  "s3a://project555/sss/links_clean.orc/" ;

create external table genome_scores(movieid int, tagid int, relevance float) Stored as orc LOCATION  "s3a://project555/sss/genome_scores_clean.orc/";

create external table genome_tags(tagid int, tag string) stored as orc LOCATION  "s3a://project555/sss/genome_tags_clean.orc/" ;

create external table movies( movieid int, title string, genres string, year int ) stored as orc LOCATION  "s3a://project555/sss/movies_clean.orc/";

create external table ratings(userid int, movieid int, rating float,`date` string, `time` string ) stored as orc LOCATION  "s3a://project555/sss/ratings_clean.orc/" ;z

create external table tags(userid int, movieid string, tag string,`date` string,`time` string ) stored as orc LOCATION  "s3a://project555/sss/tags_clean.orc/" ;
Create external table top20_rated (movieid int,title string,rtg float,cntuserid int) stored as orc LOCATION "s3a://projectgroup5/FinalKPI/kpi_tables/top20_rated/";

Insert into top20_rated
SELECT m.movieid,m.title,AVG(r.rating) as rtg,COUNT(DISTINCT r.userid) as views
FROM movies m JOIN ratings r ON (m.movieid = r.movieid)
GROUP BY m.movieid,m.title
HAVING views >= 40
ORDER BY rtg DESC;
create external table top10_movies (movieid int,title string,cntuserid int) stored as orc LOCATION "s3a://projectgroup5/FinalKPI/kpi_tables/top10_movies/";

 Insert into top10_movies Select m.movieid,m.title,count(distinct r.userid) as view from movies m join ratings r on (m.movieid=r.movieid)
 group by m.movieid,m.title order by view desc limit 10;
Create external table distinct_genres (title_count int,genres string) stored as orc  LOCATION "s3a://projectgroup5/FinalKPI/kpi_tables/distinct_genres/";

Insert into distinct_genres Select count(title) as title_count, genres from movies group by genres;
create external table rateperyear (title string,year int,rating float) stored as orc  LOCATION "s3a://projectgroup5/FinalKPI/kpi_tables/rateperyear/";

Insert into rateperyear Select m.title,m.year,max(r.rating) as rating from movies m join ratings r on (m.movieid=r.movieid) group by year,title;

Create external table dist_tags (movieid int,title string,tag string) stored as orc  LOCATION "s3a://projectgroup5/FinalKPI/kpi_tables/dist_tags/";

Insert into dist_tags Select distinct m.movieid,m.title,t.tag from movies m inner join tags t on (m.movieid=t.movieid);
Create external table top30_marvel (movieid int,title string,tag string,rating float) stored as orc  LOCATION "s3a://projectgroup5/FinalKPI/kpi_tables/top30_marvel/";

Insert into top30_marvel Select m.movieid,m.title,t.tag,r.rating from movies m inner join tags t on (m.movieid=t.movieid)
inner join ratings r on (m.movieid=r.movieid) where t.tag LIKE "%marvel%" order by rating limit 10;






