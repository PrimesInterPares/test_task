
from random import choice
from string import ascii_lowercase
import numpy as np
import datetime
import random
import pymysql
import os

def get_db_connection():
    '''Connect to the database'''
    db_connection = pymysql.connect(host = 'localhost',
                                    user = 'root',
                                    password = '1234',
                                    db = 'mysql',
                                    charset = 'utf8',
                                    cursorclass = pymysql.cursors.DictCursor)
    return db_connection

def db_table_create():
    db_connection = get_db_connection()
    db_cursor = db_connection.cursor()
    '''Create new Table to the datebase'''
    sql = '''CREATE TABLE IF NOT EXISTS testtable(
                            date VARCHAR(30),
                            english_string VARCHAR(30),
                            russian_string VARCHAR(30),
                            int_number INT(20),
                            float_number FLOAT)'''
    db_cursor.execute(sql)

def unpack_line(line):
    '''String splitting into tokens separated by ||
       returns values from input string'''
    split_line = str.split(line, '||')
    dt = split_line[0]
    eng_str = split_line[1]
    rus_str = split_line[2]
    int_num = split_line[3]
    float_num = split_line[4].split('\n')
    return dt, eng_str, rus_str, int_num, float_num[0]

def random_cyrillic_str(random_str_length):
    '''Formation random Cyrillic string with 'random_str_lengh' number of symbols
       returns formed string'''
    ALPHABET = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
    ALPHABET_LENGTH = str.__len__(ALPHABET) - 1
    cyrillic_string = ""
    for i in range(random_str_length):
        temp_rand = random.randint(0, ALPHABET_LENGTH)
        cyrillic_string += ALPHABET[temp_rand]
    return cyrillic_string

def random_date(today_date, delta):
    '''Returns random data for last delta years'''
    return datetime.date.fromordinal(today_date - random.randint(0, delta))

def delta_date(year_difference):
    '''Returns today date and delta = number of days
       the difference between today and x years earlier'''
    today_date = datetime.date.today()
    late_date = datetime.date(today_date.year - year_difference, today_date.month, today_date.day)
    today_date = datetime.datetime.toordinal(today_date)
    late_date = datetime.datetime.toordinal((late_date))
    delta = today_date - late_date
    return today_date, delta

def file_merger(file_quantity):
    '''Merging all files from 'txt_folder'
       while merging may delete some custom sequences
       returns number of deleted rows'''
    merged_file = open('./txt_folder/merged_file.txt', 'w')
    yes_answer = False
    sequence_to_delete = ''
    deleted_str_counter = 0
    answer = input('Do you like to delete something while merging? y\\n\n')
    if answer == 'y':
        sequence_to_delete = input('Input sequence you want to delete\n')
        yes_answer = True
    for file_counter in range(file_quantity):
        temp_file = open('./txt_folder/test_file' + str(file_counter + 1) + '.txt', 'r')
        lines = temp_file.readlines()
        temp_file.close()
        temp_file = open('./txt_folder/test_file' + str(file_counter + 1) + '.txt', 'w')
        for line in lines:
            if yes_answer:
                if sequence_to_delete in line:
                    deleted_str_counter += 1
                else:
                    merged_file.write(line)
                    temp_file.write(line)
            else:
                merged_file.write(line)
                temp_file.write(line)
    if yes_answer:
        print('Amount of deleted lines with \'' + sequence_to_delete + '\' sequence: ' + str(deleted_str_counter) + '.\n')
    merged_file.close()
    return deleted_str_counter

def file_writer(FILE_QUANTITY, LINE_QUANTITY, YEAR_DIFFERENCE, RANDOM_CYRILLIC_STR_LENGTH, RANDOM_LATIN_STR_LENGTH):
    '''Create and fills files with the following row-structure:
                            random date for the last 'YEAR_DIFFERENCE' years
                            string with random latin letters 'RANDOM_LATIN_STR_LENGTH' length
                            string with random cyrrilic letters 'RANDOM_CYRILLIC_STR_LENGTH' length
                            random number in range from 1 to 100 000 000
                            random number with 8 decimal places in range from 1 to 20'''
    today_date, delta = delta_date(YEAR_DIFFERENCE)
    for file_counter in range(FILE_QUANTITY):
        temp_file = open('./txt_folder/test_file' + str(file_counter + 1) + '.txt', 'w')
        for line_counter in range(LINE_QUANTITY):
            temp_file.write(str(random_date(today_date, delta)))
            temp_file.write('||')
            temp_file.write(''.join(choice(ascii_lowercase) for i in range(RANDOM_LATIN_STR_LENGTH)))
            temp_file.write('||')
            temp_file.write(random_cyrillic_str(RANDOM_CYRILLIC_STR_LENGTH))
            temp_file.write('||')
            temp_file.write(str(random.randrange(1, 10e7, 2)))
            temp_file.write('||')
            temp_file.write((str('{:.8f}'.format(random.random() + float(random.randint(1, 19))))))
            temp_file.write('\n')
        temp_file.close()

def select_int_from_db():
    '''Takes out a field 'int_number' from datebase with sql-inquiry, returns it values'''
    db_connection = get_db_connection()
    db_cursor = db_connection.cursor()
    sql = '''SELECT int_number FROM testtable'''
    db_cursor.execute(sql)
    selected_int_from_db = []
    db_dict_list = db_cursor.fetchall()
    for item in db_dict_list:
        selected_int_from_db.append(item['int_number'])
    return selected_int_from_db

def select_float_from_db():
    '''Takes out a field 'float_number' from datebase with sql-inquiry, returns it values'''
    db_connection = get_db_connection()
    db_cursor = db_connection.cursor()
    sql = 'SELECT float_number FROM testtable'
    db_cursor.execute(sql)
    selected_float_from_db = []
    db_dict_list= db_cursor.fetchall()
    for item in db_dict_list:
        selected_float_from_db.append(item['float_number'])
    return selected_float_from_db

def selected_int_sum():
    '''Calculate sum of all 'int_number' from datebase'''
    selected_int = select_int_from_db()
    return sum(selected_int)

def selected_float_median():
    '''Calculate median of all 'float_numbe' from datebase'''
    selected_float = select_float_from_db()
    return np.median(selected_float)

def folder_writer():
    '''Create folder in directory'''
    try:
        os.makedirs('./txt_folder')
    except OSError:
        print('Folder already exist.')

def db_date_entry(rows_left_to_import):
    '''Enters date from 'merged_file' into the datebase table with sql-inquiry
       shows process of entering '''
    rows_imported = 0
    db_connection = get_db_connection()
    try:
        db_cursor = db_connection.cursor()
        if ('SELECT COUNT(*) from testtable') == 0:
            pass
        else:
            sql = 'DROP TABLE testtable'
        db_cursor.execute(sql)
        db_table_create()
        merged_file = open('./txt_folder/merged_file.txt', 'r')
        while True:
            line = merged_file.readline()
            if line == "":
                print('Rows imported:   ' + str(rows_imported) + '. Rows left to import:   ' + str(rows_left_to_import) + '.')
                break
            print('Rows imported:   ' + str(rows_imported) + '. Rows left to import:   ' + str(rows_left_to_import) + '.')
            date_line, english_str, cyrillic_str, int_num, float_num = unpack_line(line)
            sql_item = '''INSERT INTO testtable(date, english_string, russian_string, int_number, float_number)
                            VALUES('%(date)s', '%(english_string)s', '%(russian_string)s', '%(int_number)s', '%(float_number)s')
                            ''' % {'date': date_line, 'english_string': english_str, 'russian_string': cyrillic_str,
                                   'int_number': int_num, 'float_number': float_num}
            db_cursor.execute(sql_item)
            rows_left_to_import -= 1
            rows_imported += 1
    finally:
        db_connection.commit()
        db_connection.close()
    merged_file.close()



if __name__ == '__main__':
    FILE_QUANTITY = 20
    LINE_QUANTITY = 1000
    TOTAL_ROW_QUANTITY = FILE_QUANTITY * LINE_QUANTITY
    YEAR_DIFFERENCE = 5
    RANDOM_LATIN_STR_LENGTH = 10
    RANDOM_CYRILLIC_STR_LENGTH = 10


    folder_writer()
    file_writer(FILE_QUANTITY, LINE_QUANTITY, YEAR_DIFFERENCE, RANDOM_CYRILLIC_STR_LENGTH, RANDOM_LATIN_STR_LENGTH)
    print("Merging files.")
    print('Answer via console')
    rows_left_to_import = (TOTAL_ROW_QUANTITY - file_merger(FILE_QUANTITY))
    db_table_create()
    db_date_entry(rows_left_to_import)
    while True:
        answer = input('Would you like to continue? y/n\n')
        if answer == 'y':
            answer = input("""Input what do you like to do?
            merge
            import to DateBase
            take sum of integer number
            take median of float numbers\n\n""")
            if answer == 'merge':
                rows_left_to_import -= file_merger(FILE_QUANTITY)
            elif answer == 'import':
                db_date_entry(rows_left_to_import)
            elif answer == 'sum':
                print('Sum of int number from Date Base: ' + str(selected_int_sum()))
            elif answer == 'median' :
                print('Median of float number from Date Base: ' + str(selected_float_median()))
        else:
            break