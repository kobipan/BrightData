import Functions as fnc



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # lst_id = fnc.set_last_id()+1
    # fnc.append_data(from_id=lst_id,limit=10)

    list_of_month = ['2021-07','2021-08'] # ['2021-01','2021-02','2021-03','2021-04']
    fnc.download_files_parallel(month_to_upload = list_of_month)

    #fnc.read_data_from_s3(parquet_file_path)
    fnc.get_num_of_pasangers()