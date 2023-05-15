
test_folder='test'
make &> ./$test_folder/compile.out
# binary_file page_size buffer_slots max_opened_files buffer_replacement_policy database_folder input_data queries output_log

timeout 10s ./main 64 6 3 CLS ./data ./$test_folder/test1/data_1.txt ./$test_folder/test1/query_1.txt ./$test_folder/test1/log_1.txt &> ./$test_folder/test1/trace-1

rm ./data/*
# selection only test
timeout 10s ./main 50 3 3 CLS ./data ./$test_folder/test2/data_2.txt ./$test_folder/test2/query_2.txt ./$test_folder/test2/log_2.txt &> ./$test_folder/test2/trace-2

rm ./data/*
# join only test （for enough buffers）
timeout 10s ./main 50 14 2 CLS ./data ./$test_folder/test3/data_3.txt ./$test_folder/test3/query_3.txt ./$test_folder/test3/log_3.txt &> ./$test_folder/test3/trace-3

rm ./data/*
# join only test  
timeout 10s ./main 50 5 2 CLS ./data ./$test_folder/test4/data_4.txt ./$test_folder/test4/query_4.txt ./$test_folder/test4/log_4.txt &> ./$test_folder/test4/trace-4

rm ./data/*
# mixed test 
timeout 10s ./main 40 3 3 CLS ./data ./$test_folder/test5/data_5.txt ./$test_folder/test5/query_5.txt ./$test_folder/test5/log_5.txt &> ./$test_folder/test5/trace-5

rm ./data/*
timeout 10s ./main 57 4 2 CLS ./data ./$test_folder/test6/data_6.txt ./$test_folder/test6/query_6.txt ./$test_folder/test6/log_6.txt &> ./$test_folder/test6/trace-6

rm ./data/*
timeout 10s ./main 200 6 3 CLS ./data ./$test_folder/test7/data_7.txt ./$test_folder/test7/query_7.txt ./$test_folder/test7/log_7.txt &> ./$test_folder/test7/trace-7

rm ./data/*
timeout 10s ./main 80 7 5 CLS ./data ./$test_folder/test8/data_8.txt ./$test_folder/test8/query_8.txt ./$test_folder/test8/log_8.txt &> ./$test_folder/test8/trace-8

rm ./data/*
timeout 10s ./main 40 3 4 CLS ./data ./$test_folder/test9/data_9.txt ./$test_folder/test9/query_9.txt ./$test_folder/test9/log_9.txt &> ./$test_folder/test9/trace-9

rm ./data/*
timeout 10s ./main 40 3 2 CLS ./data ./$test_folder/test10/data_10.txt ./$test_folder/test10/query_10.txt ./$test_folder/test10/log_10.txt &> ./$test_folder/test10/trace-10

rm ./data/*



