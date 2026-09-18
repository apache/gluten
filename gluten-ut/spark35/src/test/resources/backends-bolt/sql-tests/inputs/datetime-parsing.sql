--- TESTS FOR DATETIME PARSING FUNCTIONS ---

-- parsing with pattern 'y'.
-- the range of valid year is [-290307, 294247],
-- but particularly, some thrift client use java.sql.Timestamp to parse timestamp, which allows
-- only positive year values less or equal than 9999. So the cases bellow only use [1, 9999] to pass
-- ThriftServerQueryTestSuite
select to_timestamp('1', 'y');

-- reduced two digit form is used, the range of valid year is 20-[01, 99]
select to_timestamp('00', 'yy');

-- the range of valid year is [-290307, 294247], the number of digits must be in [3, 6] for 'yyy'
select to_timestamp('001', 'yyy');

-- the range of valid year is [-9999, 9999], the number of digits must be 4 for 'yyyy'.
select to_timestamp('0001', 'yyyy');

-- the range of valid year is [-99999, 99999], the number of digits must be 5 for 'yyyyy'.
select to_timestamp('00001', 'yyyyy');

-- the range of valid year is [-290307, 294247], the number of digits must be 6 for 'yyyyyy'.
select to_timestamp('000001', 'yyyyyy');

-- parsing with pattern 'D'
select to_timestamp('2020-365', 'yyyy-DDD');
select to_timestamp('2020-30-365', 'yyyy-dd-DDD');
select to_timestamp('2020-12-350', 'yyyy-MM-DDD');
select to_timestamp('2020-12-31-366', 'yyyy-MM-dd-DDD');
