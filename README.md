# CA675  - Cloud Technologies Assignment 1 Submission

This is my submission for a graded assignment for the course CA675 at DCU. 

The code in 
'assignent.pig' is the assignment. If you want to run it you have to upload the data folder respectively into your Google Dataproc cluster storage and change the link to for the data import:

> Line 1 of `assigment1.pig`
>
> /\*Load files into pig\*/
> batch1 = LOAD =='gs://dataproc-XXXXX/assigment1/input/QueryResults1.csv'== USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (id:charArray,
> post_title:charArray,
> score:int,
> post_text:charArray,
> view_count:int,
> user_id:charArray); 

