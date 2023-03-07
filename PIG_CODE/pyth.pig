--register_python_udf.pig 
register 'Documents/YARAB.py' using streaming_python as extract_q_mark;
register 'Documents/YARAB.py' using org.apache.pig.scripting.streaming.python.PythonScriptEngine as extract_q_mark;

data = load '/home/osboxes/Documents/whquestion1.txt' as ( line : chararray) ;

/* Filter Question ONLY using udf python code*/

data_q_mark = filter data by extract_q_mark.mark(line) == 'True';
dump data_q_mark;

Store data_q_mark into 'output_Q_mark_bonus__' ;

/* Change the case of your text in lower and replace any (') and (? )with space */

data_lower_word = foreach data_q_mark generate REPLACE(LOWER(line), '[\']' , '  ') as line_lower;
data_lower_word = FOREACH data_lower_word  GENERATE REPLACE(line_lower ,'[?]',' ') as line_lower;
/* Tokenize your data and store the output in the output_tokenize */

word_list = foreach data_lower_word generate TOKENIZE(line_lower) as word_lst;
dump word_list ;
Store word_list into 'output_tokenize_bonus__' ;

/* Flatten your data and store the output into the output_flattened  */

word_flattened = foreach word_list generate flatten(word_lst) as word;
Store word_flattened into 'output_flattened_bonus__' ;

/* Filter word_flattened to extract the whquestions only and store the output into output_Extract_Wh_Q_ */

Extract_Wh_Q = filter word_flattened by  ((word) == 'how' or (word) == 'whom' or 
(word)=='what' or (word) == 'when' or (word) == 'whom'  or (word) == 'whose' or (word) =='why' or (word) == 'which' or (word) == 'where') or (word) == 'who';
dump Extract_Wh_Q  ;
Store Extract_Wh_Q  into 'output_Extract_Wh_Q_bonus__' ;

/* Group and merge the Extract_Wh_Q */
merge = group Extract_Wh_Q by word ; 

/* Finally,count the frequency of the WH question words and store the output into output_count_wh_*/

count_wh =  foreach merge  generate group ,COUNT(Extract_Wh_Q.word) ;

dump count_wh ; 
Store count_wh  into 'output_count_wh_bonus__'  ;

/* I uploaded all the output files on onQ */

/* -----------------------------------THE END ^_^ ------------------------------------ */






