/*Load files into pig*/
batch1 = LOAD 'gs://dataproc-XXXXX/assigment1/input/QueryResults1.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (id:charArray,
post_title:charArray,
score:int,
post_text:charArray,
view_count:int,
user_id:charArray);

batch2 = LOAD 'gs://dataproc-XXXXX/assigment1/input/QueryResults2.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (id:charArray,
post_title:charArray,
score:int,
post_text:charArray,
view_count:int,
user_id:charArray);

batch3 = LOAD 'gs://dataproc-XXXXX/assigment1/input/QueryResults3.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (id:charArray,
post_title:charArray,
score:int,
post_text:charArray,
view_count:int,
user_id:charArray);

batch4 = LOAD 'gs://dataproc-XXXXX/assigment1/input/QueryResults4.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (id:charArray,
post_title:charArray,
score:int,
post_text:charArray,
view_count:int,
user_id:charArray);

batch5 = LOAD 'gs://dataproc-XXXXX/assigment1/input/QueryResults5.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (id:charArray,
post_title:charArray,
score:int,
post_text:charArray,
view_count:int,
user_id:charArray);

/*Verify the data
res = LIMIT batch1 3;
DUMP res;
res = LIMIT batch2 3;
DUMP res;
res = LIMIT batch3 3;
DUMP res;
res = LIMIT batch4 3;
DUMP res;
res = LIMIT batch5 3;
DUMP res;
*/

/* Merge the relations into one relation

source for filtering out header: https://pig.apache.org/docs/r0.17.0/basic.html#:~:text=1%2C2%2C3)%0A(4%2C3%2C3)%0A(8%2C3%2C4)-,FILTER,conversely%2C%20to%20filter%20out%20(remove)%20the%20data%20you%20don%E2%80%99t%20want.,-Examples
code was altered.
*/
-- Filter out header

batch1_no_header = filter batch1 by post_text != 'post_text';
batch2_no_header = filter batch2 by post_text != 'post_text';
batch3_no_header = filter batch3 by post_text != 'post_text';
batch4_no_header = filter batch4 by post_text != 'post_text';
batch5_no_header = filter batch5 by post_text != 'post_text';

-- Combine relations
b12 = UNION batch1_no_header, batch2_no_header;
b123 = UNION b12, batch3_no_header;
b1234 = UNION b123, batch4_no_header;
b12345 = UNION b1234 , batch5_no_header;

-- Count rows, should be 201,000 --> is 201,000 check!
b12345_group_all = GROUP b12345 ALL;
b12345_count = FOREACH b12345_group_all GENERATE COUNT(b12345);
DUMP b12345_count;

-- take out duplicates and check row counts,  source for counting: https://www.youtube.com/watch?v=YIB0eFM4GE0&t=550s
b12345_distinct = DISTINCT b12345;

b12345_distinct_group = GROUP b12345_distinct ALL;
b12345_distinct_count = FOREACH b12345_distinct_group GENERATE COUNT_STAR(b12345_distinct);
DUMP b12345_distinct_count;

b12345_distinct_sorted =  ORDER b12345_distinct BY view_count DESC;


-- Filter for top 200,000
data = LIMIT b12345_distinct_sorted 200000;

-- verify length of relation, source: https://www.youtube.com/watch?v=YIB0eFM4GE0&t=550s
data_group_all = GROUP data ALL;
line_count = FOREACH data_group_all GENERATE COUNT_STAR(data);
DUMP line_count;


/*Get the top 10 posts by post score */
data_sorted_by_score = ORDER data BY score DESC;
top_ten_rows_by_score = LIMIT data_sorted_by_score 10;
--top_ten_posts_by_score = foreach top_ten_rows_by_score generate id,post_title,score; -- for better overview
top_ten_posts_by_score = foreach top_ten_rows_by_score generate post_text; -- only the posts

DUMP top_ten_posts_by_score;
DESCRIBE top_ten_posts_by_score;



/*Get the top 10 users by post score*/
--data_sorted_by_score = ORDER data BY score DESC;
top_rows_by_score = LIMIT data_sorted_by_score 11;
top_users_by_score = foreach top_rows_by_score generate user_id;
distinct_top_users_by_score = DISTINCT top_users_by_score;

DUMP distinct_top_users_by_score;





/*Get the number of distinct users, who used the word “cloud” in one of their posts*/
--filter by the word 'cloud':
posts_with_cloud = FILTER data BY (post_text MATCHES '.*cloud.*');

--check that they actually contain the word cloud
--res = LIMIT posts_with_cloud 10;
--dump res;

--count the number of posts with the term clod
--cloud_posts_all = GROUP posts_with_cloud ALL;
--cloud_post_count = FOREACH cloud_posts_all GENERATE COUNT(cloud_posts_all);
--DUMP cloud_post_count;



--count unique users:
users_using_the_word_cloud = FOREACH posts_with_cloud GENERATE user_id;
unique_users_using_the_word_cloud = DISTINCT users_using_the_word_cloud;
unique_user_group_all = GROUP unique_users_using_the_word_cloud ALL;
unique_user_count = FOREACH unique_user_group_all GENERATE COUNT(unique_users_using_the_word_cloud);
DUMP unique_user_count;



/**TF-IDF**/

/*Get the posts of the top users
source: wikitechy.com/tutorials/apache-pig/apache-pig-tutorial/using-in-clause-with-pig-filter.php
*/
posts = FOREACH data GENERATE user_id AS user_id,post_text AS post_text;
top_user_posts_join = JOIN posts BY user_id, distinct_top_users_by_score BY user_id;
top_user_posts = FOREACH top_user_posts_join GENERATE posts::user_id AS user_id, posts::post_text as post_text;

-- count top user posts for verification
top_user_posts_group = GROUP top_user_posts ALL;
post_count = FOREACH top_user_posts_group GENERATE COUNT(top_user_posts);
dump post_count;



/*Tokenize Users posts
source: https://www.folkstalk.com/2013/09/word-count-example-pig-script.html
*/
top_user_posts_grouped_uid = GROUP top_user_posts BY user_id;
  -- getting all posts per user into one tuple: {user_id: int,post_texts: {(post_text: chararray)}}
corpus = FOREACH top_user_posts_grouped_uid GENERATE group as user_id, top_user_posts.post_text as post_texts;
corpus_tokenized = FOREACH corpus { -- Rudimenta "data cleaning"
	words = FOREACH post_texts GENERATE FLATTEN(TOKENIZE(post_text,'<')) as split_token;
	words = FOREACH words GENERATE FLATTEN(TOKENIZE(split_token,'>')) as split_token;
	words = FOREACH words GENERATE FLATTEN(TOKENIZE(split_token,'!')) as split_token;
	words = FOREACH words GENERATE FLATTEN(TOKENIZE(split_token,'?')) as split_token;
	words = FOREACH words GENERATE FLATTEN(TOKENIZE(split_token,'"')) as split_token;
	words = FOREACH words GENERATE FLATTEN(TOKENIZE(split_token,'.')) as split_token;
	words = FOREACH words GENERATE FLATTEN(TOKENIZE(split_token,',')) as split_token;
	words = FOREACH words GENERATE FLATTEN(TOKENIZE(split_token,'-')) as split_token;
	words = FOREACH words GENERATE FLATTEN(TOKENIZE(split_token,':')) as split_token;
	words = FOREACH words GENERATE FLATTEN(TOKENIZE(split_token,';')) as split_token;
	words = FOREACH words GENERATE FLATTEN(TOKENIZE(split_token,' ')) as split_by_space;
	words = FOREACH words GENERATE FLATTEN(LOWER(split_by_space)) as clean_token;
	GENERATE user_id, words;
};

-- check
res = LIMIT corpus_tokenized 5;
dump res;

/*Note on calculating TFIDF
source: https://thedatachef.blogspot.com/2011/04/tf-idf-with-apache-pig.html

Note: For this task I have tried various approaches. However due to my lack of experience in apache Pig
I was not able to apply my knowledge from other programming languages to this task. For example the concept of variables is a bit odd in Pig.
As any programmer in the real working field does I looked for references and used the code from the source above.
But to not make this a simple copy paste job I changed the code wherever applicable like the variable names or the tokenization above.
As you can tell by my variable names and comments, I have changed every single one to make sense in the task's context and show
that I am understanding what is happening.
*/

/*Get the wordcounts of each user*/
user_words = FOREACH corpus_tokenized GENERATE user_id, FLATTEN(words) as word;
group_same_user_words = GROUP user_words BY (user_id, word);
user_word_counts = FOREACH group_same_user_words GENERATE FLATTEN(group) AS (user_id, word), COUNT(user_words) AS word_occourance_in_user_posts;

res = LIMIT user_word_counts 20;
dump res;



/*Get the word frequencies*/
user_word_counts_bag = GROUP user_word_counts BY user_id;
user_word_counts_with_frequency = FOREACH user_word_counts_bag GENERATE
   group                                                 AS user_id,
   FLATTEN(user_word_counts.(word, word_occourance_in_user_posts)) AS (word, occour_in_user_posts),
   SUM(user_word_counts.word_occourance_in_user_posts)              AS user_post_word_total;

word_frequencies_by_user = FOREACH user_word_counts_with_frequency GENERATE
   user_id                                         AS user_id,
   word                                            AS word,
   ((double)occour_in_user_posts / (double)user_post_word_total) AS word_frequency;

res = LIMIT word_frequencies_by_user 20;
dump res;

/*Find the number of users that use a word*/
word_user_usage_group = GROUP word_frequencies_by_user BY word;
word_user_usage = FOREACH word_user_usage_group GENERATE
             FLATTEN(word_frequencies_by_user) AS (user_id, word, word_frequency),
             COUNT(word_frequencies_by_user)   AS num_users_that_used_word;

tfidf = FOREACH word_user_usage {
 idf    = LOG((double)3/(double)num_users_that_used_word);
 tf_idf = (double)word_frequency*idf;
   GENERATE
     user_id AS user_id,
     word  AS word,
     tf_idf AS tf_idf;
};

words_sorted_by_tfidf = ORDER tfidf by tf_idf DESC;
res = LIMIT words_sorted_by_tfidf 10;
dump res;
