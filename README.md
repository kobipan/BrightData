# BrightData
Bright Data - Data Engineer Home Assignment

those are the instarctions 
https://docs.google.com/document/d/17q_aORa3AZ8tZHnWVt2e0gG6Fno9PGKpttURkT5tTp4/edit

i didnt finish the job yet, do to lac of time.
but i have created function that read and copy parquet files from provided api to S3 buckets, them i have create using aws ui Crawler taht creating table in athena selrverless database.
on top pf that i have created function that aggrigate the passengers amount by the month and create csv file located in other bucket in S3 
there are 3 main function on this solution:
1.  download_file(month_id) - this function will download data from specific month to the s3 bucket (bucket_name = 'yellowtrip')
2.  download_files_parallel(month_to_upload: list) - this function get list of month and apply it on download_file function, the fiels will be download in parallel.
3.  get_num_of_pasangers() - this function return aggrigated table of passangers/month. that data will be saved in file named monthly_passengers_count on other s3 bucket named athenabkt.
