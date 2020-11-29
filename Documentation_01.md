# Tasks Completed


## 1. Data Extraction from SEDE.

Data was fetched from stack exchage using the SQL. Top 200,000 posts were fetched in four csv files, since stackexchange only allows 50000 posts to be fetched at once.

The following query was used to check the top 200,000 posts.

```
select count(*) from posts where posts.ViewCount > 36779
```

Futher the ViewCount was taken as a reference to fetch the first, second, third and fourth 50000 queries

``` 
select top 50000 * from posts where ViewCount > 36779 order by posts.ViewCount desc
```

For the next 50000 posts we use the last row as the limit

```
select top 50000 * from posts where ViewCount <= 112523 order by posts.ViewCount desc
select top 50000 * from posts where ViewCount <= 66243 order by posts.ViewCount desc
select top 50000 * from posts where ViewCount <= 47290 order by posts.ViewCount desc
```


## 2. Loading Data

For loading the data, I created a storage bucket on my Google Cloud Platform (GCP) console and then uploaded all the files extracted from Stack Exchange Data Explorer (SEDE) from my local machine.
Futhermore, used this bucket's path for creating tables in HIVE.


## 3. Querying with HIVE

--TOP 10 POSTS BY SCORE

```select Id, Title FROM data_analysis_stg ORDER BY Score DESC LIMIT 10;```

--TOP 10 USERS BY POST SCORE
```
select OwnerUserId, SUM(Score)AS FullScore
from data_analysis_stg
Order BY OwnerUserId
Order BY FullScore DESC LIMIT 10;
```

--The number of distinct users, who used the word “Hadoop” in one of their posts
```
select COUNT (DISTINCT OwnerUserId)
from data_analysis_stg
WHERE (Body LIKE '%hadoop%' OR Title LIKE '%hadoop%' OR Tags LIKE '%hadoop%');
```

## 4. Calculating the per user TF-IDF for top 10 users using HIVE

Hive mall is used to calculate TF-IDF. The documentaion of hivemall TF-IDF Term Weighting Hivemall User Manual and TFIDF Calculation has been used to write the code.

```
create temporary macro max2(x INT, y INT) if(x>y,x,y);

create temporary macro tfidf(tf FLOAT, df_t INT, n_docs INT) tf * (log(10, CAST(n_docs as FLOAT)/max2(1,df_t)) + 1.0);

create table Qtable as select OwnerUserId, Title,Score from buz order by Score desc limit 10;

create or replace view view1_Explode as select OwnerUserId, eachword from table1 LATERAL VIEW explode(tokenize(Title, True)) t as eachword where not is_stopword(eachword);

create or replace view view2 as select OwnerUserid, eachword, freq from (select OwnerUserId, tf(eachword) as word2freq from view1_Explode group by OwnerUserId) t LATERAL VIEW explode(word2freq) t2 as eachword, freq;

create or replace view view_final as select * from (select OwnerUserId, eachword, freq, rank() over (partition by OwnerUserId order by freq desc) as rank from view2 ) t where rank < 10;

select * from view_final;
```
