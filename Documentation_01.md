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

```select Id, Title FROM data_analysis_posts_stg order BY Score DESC limit 10;```

--TOP 10 USERS BY POST SCORE
```
create table table2 as select OwnerUserId as A, SUM(Score) as B from data_analysis_posts_stg 
group BY OwnerUserId;
select * from table2 order by B DESC limit 10;
```

--The number of distinct users, who used the word “Hadoop” in one of their posts
```
select COUNT (DISTINCT OwnerUserId) from data_analysis_posts_stg where bodylike '%Hadoop%';
```

## 4. Calculating the per user TF-IDF for top 10 users using HIVE

Hive mall is used to calculate TF-IDF. The documentaion of hivemall TF-IDF Term Weighting Hivemall User Manual and TFIDF Calculation has been used to write the code.

```
create temporary macro max2(x INT, y INT) if(x>y,x,y);

create temporary macro tfidf(tf FLOAT, df_t INT, n_docs INT) tf * (log(10, CAST(n_docs as FLOAT)/max2(1,df_t)) + 1.0);

create table tb1 as select OwnerUserId, Title,Score from data_analysis_posts_stg order by Score desc limit 10;

create view Expld as select OwnerUserId, word from tb1 LATERAL VIEW explode(tokenize(Title, True)) t as word where not is_stopword(word);

create view term_freq as select OwnerUserid, word, freq from (select OwnerUserId, tf(word) as word2freq from Expld group by OwnerUserId) t LATERAL VIEW explode(word2freq) t2 as word, freq;

create or replace view tf_idf as select word, COUNT(distinct OwnerUserId) docs from Expld group by word;

select COUNT(ownerUserId) from tf_idf;

set hivevar:n_docs=10;

create or replace view tf_idf as select tf.OwnerUserId, tf.word, tfidf(tf.freq, df.docs,${n_docs}) as tf_idf from term_freq tf JOIN document_frequency dfON(tf.word=df.word) order by tf_idf desc;

select * from tf_idf;
```
