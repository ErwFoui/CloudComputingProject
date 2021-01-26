CREATE EXTERNAL TABLE IF NOT EXISTS flights(
    YEAR int,
    MONTH int,
    DAY_OF_MONTH int,
    DAY_OF_WEEK int,
    FL_DATE string,
    OP_UNIQUE_CARRIER string,
    OP_CARRIER_FL_NUM string,
    ORIGIN_AIRPORT_ID string,
    ORIGIN string,
    DEST_AIRPORT_ID string,
    DEST string,
    CRS_DEP_TIME string,
    DEP_TIME string,
    DEP_DELAY int,
    DEP_DELAY_NEW int,
    CRS_ARR_TIME string,
    ARR_TIME string,
    ARR_DELAY int,
    ARR_DELAY_NEW int,
    CANCELLED int
)
PARTITIONED BY (Yeard int, Monthd int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES("skip.header.line.count"="1");

ALTER TABLE flights ADD PARTITION(yeard=1987, monthd=1) LOCATION "hdfs:/Data/1987/1/";
ALTER TABLE flights ADD PARTITION(yeard=1987, monthd=2) LOCATION "hdfs:/Data/1987/2/";
ALTER TABLE flights ADD PARTITION(yeard=1987, monthd=3) LOCATION "hdfs:/Data/1987/3/";
ALTER TABLE flights ADD PARTITION(yeard=1987, monthd=4) LOCATION "hdfs:/Data/1987/4/";
ALTER TABLE flights ADD PARTITION(yeard=1987, monthd=5) LOCATION "hdfs:/Data/1987/5/";
ALTER TABLE flights ADD PARTITION(yeard=1987, monthd=6) LOCATION "hdfs:/Data/1987/6/";
ALTER TABLE flights ADD PARTITION(yeard=1987, monthd=7) LOCATION "hdfs:/Data/1987/7/";
ALTER TABLE flights ADD PARTITION(yeard=1987, monthd=8) LOCATION "hdfs:/Data/1987/8/";
ALTER TABLE flights ADD PARTITION(yeard=1987, monthd=9) LOCATION "hdfs:/Data/1987/9/";
ALTER TABLE flights ADD PARTITION(yeard=1987, monthd=10) LOCATION "hdfs:/Data/1987/10/";
ALTER TABLE flights ADD PARTITION(yeard=1987, monthd=11) LOCATION "hdfs:/Data/1987/11/";
ALTER TABLE flights ADD PARTITION(yeard=1987, monthd=12) LOCATION "hdfs:/Data/1987/12/";
ALTER TABLE flights ADD PARTITION(yeard=1988, monthd=1) LOCATION "hdfs:/Data/1988/1/";
ALTER TABLE flights ADD PARTITION(yeard=1988, monthd=2) LOCATION "hdfs:/Data/1988/2/";
ALTER TABLE flights ADD PARTITION(yeard=1988, monthd=3) LOCATION "hdfs:/Data/1988/3/";
ALTER TABLE flights ADD PARTITION(yeard=1988, monthd=4) LOCATION "hdfs:/Data/1988/4/";
ALTER TABLE flights ADD PARTITION(yeard=1988, monthd=5) LOCATION "hdfs:/Data/1988/5/";
ALTER TABLE flights ADD PARTITION(yeard=1988, monthd=6) LOCATION "hdfs:/Data/1988/6/";
ALTER TABLE flights ADD PARTITION(yeard=1988, monthd=7) LOCATION "hdfs:/Data/1988/7/";
ALTER TABLE flights ADD PARTITION(yeard=1988, monthd=8) LOCATION "hdfs:/Data/1988/8/";
ALTER TABLE flights ADD PARTITION(yeard=1988, monthd=9) LOCATION "hdfs:/Data/1988/9/";
ALTER TABLE flights ADD PARTITION(yeard=1988, monthd=10) LOCATION "hdfs:/Data/1988/10/";
ALTER TABLE flights ADD PARTITION(yeard=1988, monthd=11) LOCATION "hdfs:/Data/1988/11/";
ALTER TABLE flights ADD PARTITION(yeard=1988, monthd=12) LOCATION "hdfs:/Data/1988/12/";
ALTER TABLE flights ADD PARTITION(yeard=1989, monthd=1) LOCATION "hdfs:/Data/1989/1/";
ALTER TABLE flights ADD PARTITION(yeard=1989, monthd=2) LOCATION "hdfs:/Data/1989/2/";
ALTER TABLE flights ADD PARTITION(yeard=1989, monthd=3) LOCATION "hdfs:/Data/1989/3/";
ALTER TABLE flights ADD PARTITION(yeard=1989, monthd=4) LOCATION "hdfs:/Data/1989/4/";
ALTER TABLE flights ADD PARTITION(yeard=1989, monthd=5) LOCATION "hdfs:/Data/1989/5/";
ALTER TABLE flights ADD PARTITION(yeard=1989, monthd=6) LOCATION "hdfs:/Data/1989/6/";
ALTER TABLE flights ADD PARTITION(yeard=1989, monthd=7) LOCATION "hdfs:/Data/1989/7/";
ALTER TABLE flights ADD PARTITION(yeard=1989, monthd=8) LOCATION "hdfs:/Data/1989/8/";
ALTER TABLE flights ADD PARTITION(yeard=1989, monthd=9) LOCATION "hdfs:/Data/1989/9/";
ALTER TABLE flights ADD PARTITION(yeard=1989, monthd=10) LOCATION "hdfs:/Data/1989/10/";
ALTER TABLE flights ADD PARTITION(yeard=1989, monthd=11) LOCATION "hdfs:/Data/1989/11/";
ALTER TABLE flights ADD PARTITION(yeard=1989, monthd=12) LOCATION "hdfs:/Data/1989/12/";
ALTER TABLE flights ADD PARTITION(yeard=1990, monthd=1) LOCATION "hdfs:/Data/1990/1/";
ALTER TABLE flights ADD PARTITION(yeard=1990, monthd=2) LOCATION "hdfs:/Data/1990/2/";
ALTER TABLE flights ADD PARTITION(yeard=1990, monthd=3) LOCATION "hdfs:/Data/1990/3/";
ALTER TABLE flights ADD PARTITION(yeard=1990, monthd=4) LOCATION "hdfs:/Data/1990/4/";
ALTER TABLE flights ADD PARTITION(yeard=1990, monthd=5) LOCATION "hdfs:/Data/1990/5/";
ALTER TABLE flights ADD PARTITION(yeard=1990, monthd=6) LOCATION "hdfs:/Data/1990/6/";
ALTER TABLE flights ADD PARTITION(yeard=1990, monthd=7) LOCATION "hdfs:/Data/1990/7/";
ALTER TABLE flights ADD PARTITION(yeard=1990, monthd=8) LOCATION "hdfs:/Data/1990/8/";
ALTER TABLE flights ADD PARTITION(yeard=1990, monthd=9) LOCATION "hdfs:/Data/1990/9/";
ALTER TABLE flights ADD PARTITION(yeard=1990, monthd=10) LOCATION "hdfs:/Data/1990/10/";
ALTER TABLE flights ADD PARTITION(yeard=1990, monthd=11) LOCATION "hdfs:/Data/1990/11/";
ALTER TABLE flights ADD PARTITION(yeard=1990, monthd=12) LOCATION "hdfs:/Data/1990/12/";
ALTER TABLE flights ADD PARTITION(yeard=1991, monthd=1) LOCATION "hdfs:/Data/1991/1/";
ALTER TABLE flights ADD PARTITION(yeard=1991, monthd=2) LOCATION "hdfs:/Data/1991/2/";
ALTER TABLE flights ADD PARTITION(yeard=1991, monthd=3) LOCATION "hdfs:/Data/1991/3/";
ALTER TABLE flights ADD PARTITION(yeard=1991, monthd=4) LOCATION "hdfs:/Data/1991/4/";
ALTER TABLE flights ADD PARTITION(yeard=1991, monthd=5) LOCATION "hdfs:/Data/1991/5/";
ALTER TABLE flights ADD PARTITION(yeard=1991, monthd=6) LOCATION "hdfs:/Data/1991/6/";
ALTER TABLE flights ADD PARTITION(yeard=1991, monthd=7) LOCATION "hdfs:/Data/1991/7/";
ALTER TABLE flights ADD PARTITION(yeard=1991, monthd=8) LOCATION "hdfs:/Data/1991/8/";
ALTER TABLE flights ADD PARTITION(yeard=1991, monthd=9) LOCATION "hdfs:/Data/1991/9/";
ALTER TABLE flights ADD PARTITION(yeard=1991, monthd=10) LOCATION "hdfs:/Data/1991/10/";
ALTER TABLE flights ADD PARTITION(yeard=1991, monthd=11) LOCATION "hdfs:/Data/1991/11/";
ALTER TABLE flights ADD PARTITION(yeard=1991, monthd=12) LOCATION "hdfs:/Data/1991/12/";
ALTER TABLE flights ADD PARTITION(yeard=1992, monthd=1) LOCATION "hdfs:/Data/1992/1/";
ALTER TABLE flights ADD PARTITION(yeard=1992, monthd=2) LOCATION "hdfs:/Data/1992/2/";
ALTER TABLE flights ADD PARTITION(yeard=1992, monthd=3) LOCATION "hdfs:/Data/1992/3/";
ALTER TABLE flights ADD PARTITION(yeard=1992, monthd=4) LOCATION "hdfs:/Data/1992/4/";
ALTER TABLE flights ADD PARTITION(yeard=1992, monthd=5) LOCATION "hdfs:/Data/1992/5/";
ALTER TABLE flights ADD PARTITION(yeard=1992, monthd=6) LOCATION "hdfs:/Data/1992/6/";
ALTER TABLE flights ADD PARTITION(yeard=1992, monthd=7) LOCATION "hdfs:/Data/1992/7/";
ALTER TABLE flights ADD PARTITION(yeard=1992, monthd=8) LOCATION "hdfs:/Data/1992/8/";
ALTER TABLE flights ADD PARTITION(yeard=1992, monthd=9) LOCATION "hdfs:/Data/1992/9/";
ALTER TABLE flights ADD PARTITION(yeard=1992, monthd=10) LOCATION "hdfs:/Data/1992/10/";
ALTER TABLE flights ADD PARTITION(yeard=1992, monthd=11) LOCATION "hdfs:/Data/1992/11/";
ALTER TABLE flights ADD PARTITION(yeard=1992, monthd=12) LOCATION "hdfs:/Data/1992/12/";
ALTER TABLE flights ADD PARTITION(yeard=1993, monthd=1) LOCATION "hdfs:/Data/1993/1/";
ALTER TABLE flights ADD PARTITION(yeard=1993, monthd=2) LOCATION "hdfs:/Data/1993/2/";
ALTER TABLE flights ADD PARTITION(yeard=1993, monthd=3) LOCATION "hdfs:/Data/1993/3/";
ALTER TABLE flights ADD PARTITION(yeard=1993, monthd=4) LOCATION "hdfs:/Data/1993/4/";
ALTER TABLE flights ADD PARTITION(yeard=1993, monthd=5) LOCATION "hdfs:/Data/1993/5/";
ALTER TABLE flights ADD PARTITION(yeard=1993, monthd=6) LOCATION "hdfs:/Data/1993/6/";
ALTER TABLE flights ADD PARTITION(yeard=1993, monthd=7) LOCATION "hdfs:/Data/1993/7/";
ALTER TABLE flights ADD PARTITION(yeard=1993, monthd=8) LOCATION "hdfs:/Data/1993/8/";
ALTER TABLE flights ADD PARTITION(yeard=1993, monthd=9) LOCATION "hdfs:/Data/1993/9/";
ALTER TABLE flights ADD PARTITION(yeard=1993, monthd=10) LOCATION "hdfs:/Data/1993/10/";
ALTER TABLE flights ADD PARTITION(yeard=1993, monthd=11) LOCATION "hdfs:/Data/1993/11/";
ALTER TABLE flights ADD PARTITION(yeard=1993, monthd=12) LOCATION "hdfs:/Data/1993/12/";
ALTER TABLE flights ADD PARTITION(yeard=1994, monthd=1) LOCATION "hdfs:/Data/1994/1/";
ALTER TABLE flights ADD PARTITION(yeard=1994, monthd=2) LOCATION "hdfs:/Data/1994/2/";
ALTER TABLE flights ADD PARTITION(yeard=1994, monthd=3) LOCATION "hdfs:/Data/1994/3/";
ALTER TABLE flights ADD PARTITION(yeard=1994, monthd=4) LOCATION "hdfs:/Data/1994/4/";
ALTER TABLE flights ADD PARTITION(yeard=1994, monthd=5) LOCATION "hdfs:/Data/1994/5/";
ALTER TABLE flights ADD PARTITION(yeard=1994, monthd=6) LOCATION "hdfs:/Data/1994/6/";
ALTER TABLE flights ADD PARTITION(yeard=1994, monthd=7) LOCATION "hdfs:/Data/1994/7/";
ALTER TABLE flights ADD PARTITION(yeard=1994, monthd=8) LOCATION "hdfs:/Data/1994/8/";
ALTER TABLE flights ADD PARTITION(yeard=1994, monthd=9) LOCATION "hdfs:/Data/1994/9/";
ALTER TABLE flights ADD PARTITION(yeard=1994, monthd=10) LOCATION "hdfs:/Data/1994/10/";
ALTER TABLE flights ADD PARTITION(yeard=1994, monthd=11) LOCATION "hdfs:/Data/1994/11/";
ALTER TABLE flights ADD PARTITION(yeard=1994, monthd=12) LOCATION "hdfs:/Data/1994/12/";
ALTER TABLE flights ADD PARTITION(yeard=1995, monthd=1) LOCATION "hdfs:/Data/1995/1/";
ALTER TABLE flights ADD PARTITION(yeard=1995, monthd=2) LOCATION "hdfs:/Data/1995/2/";
ALTER TABLE flights ADD PARTITION(yeard=1995, monthd=3) LOCATION "hdfs:/Data/1995/3/";
ALTER TABLE flights ADD PARTITION(yeard=1995, monthd=4) LOCATION "hdfs:/Data/1995/4/";
ALTER TABLE flights ADD PARTITION(yeard=1995, monthd=5) LOCATION "hdfs:/Data/1995/5/";
ALTER TABLE flights ADD PARTITION(yeard=1995, monthd=6) LOCATION "hdfs:/Data/1995/6/";
ALTER TABLE flights ADD PARTITION(yeard=1995, monthd=7) LOCATION "hdfs:/Data/1995/7/";
ALTER TABLE flights ADD PARTITION(yeard=1995, monthd=8) LOCATION "hdfs:/Data/1995/8/";
ALTER TABLE flights ADD PARTITION(yeard=1995, monthd=9) LOCATION "hdfs:/Data/1995/9/";
ALTER TABLE flights ADD PARTITION(yeard=1995, monthd=10) LOCATION "hdfs:/Data/1995/10/";
ALTER TABLE flights ADD PARTITION(yeard=1995, monthd=11) LOCATION "hdfs:/Data/1995/11/";
ALTER TABLE flights ADD PARTITION(yeard=1995, monthd=12) LOCATION "hdfs:/Data/1995/12/";
ALTER TABLE flights ADD PARTITION(yeard=1996, monthd=1) LOCATION "hdfs:/Data/1996/1/";
ALTER TABLE flights ADD PARTITION(yeard=1996, monthd=2) LOCATION "hdfs:/Data/1996/2/";
ALTER TABLE flights ADD PARTITION(yeard=1996, monthd=3) LOCATION "hdfs:/Data/1996/3/";
ALTER TABLE flights ADD PARTITION(yeard=1996, monthd=4) LOCATION "hdfs:/Data/1996/4/";
ALTER TABLE flights ADD PARTITION(yeard=1996, monthd=5) LOCATION "hdfs:/Data/1996/5/";
ALTER TABLE flights ADD PARTITION(yeard=1996, monthd=6) LOCATION "hdfs:/Data/1996/6/";
ALTER TABLE flights ADD PARTITION(yeard=1996, monthd=7) LOCATION "hdfs:/Data/1996/7/";
ALTER TABLE flights ADD PARTITION(yeard=1996, monthd=8) LOCATION "hdfs:/Data/1996/8/";
ALTER TABLE flights ADD PARTITION(yeard=1996, monthd=9) LOCATION "hdfs:/Data/1996/9/";
ALTER TABLE flights ADD PARTITION(yeard=1996, monthd=10) LOCATION "hdfs:/Data/1996/10/";
ALTER TABLE flights ADD PARTITION(yeard=1996, monthd=11) LOCATION "hdfs:/Data/1996/11/";
ALTER TABLE flights ADD PARTITION(yeard=1996, monthd=12) LOCATION "hdfs:/Data/1996/12/";
ALTER TABLE flights ADD PARTITION(yeard=1997, monthd=1) LOCATION "hdfs:/Data/1997/1/";
ALTER TABLE flights ADD PARTITION(yeard=1997, monthd=2) LOCATION "hdfs:/Data/1997/2/";
ALTER TABLE flights ADD PARTITION(yeard=1997, monthd=3) LOCATION "hdfs:/Data/1997/3/";
ALTER TABLE flights ADD PARTITION(yeard=1997, monthd=4) LOCATION "hdfs:/Data/1997/4/";
ALTER TABLE flights ADD PARTITION(yeard=1997, monthd=5) LOCATION "hdfs:/Data/1997/5/";
ALTER TABLE flights ADD PARTITION(yeard=1997, monthd=6) LOCATION "hdfs:/Data/1997/6/";
ALTER TABLE flights ADD PARTITION(yeard=1997, monthd=7) LOCATION "hdfs:/Data/1997/7/";
ALTER TABLE flights ADD PARTITION(yeard=1997, monthd=8) LOCATION "hdfs:/Data/1997/8/";
ALTER TABLE flights ADD PARTITION(yeard=1997, monthd=9) LOCATION "hdfs:/Data/1997/9/";
ALTER TABLE flights ADD PARTITION(yeard=1997, monthd=10) LOCATION "hdfs:/Data/1997/10/";
ALTER TABLE flights ADD PARTITION(yeard=1997, monthd=11) LOCATION "hdfs:/Data/1997/11/";
ALTER TABLE flights ADD PARTITION(yeard=1997, monthd=12) LOCATION "hdfs:/Data/1997/12/";
ALTER TABLE flights ADD PARTITION(yeard=1998, monthd=1) LOCATION "hdfs:/Data/1998/1/";
ALTER TABLE flights ADD PARTITION(yeard=1998, monthd=2) LOCATION "hdfs:/Data/1998/2/";
ALTER TABLE flights ADD PARTITION(yeard=1998, monthd=3) LOCATION "hdfs:/Data/1998/3/";
ALTER TABLE flights ADD PARTITION(yeard=1998, monthd=4) LOCATION "hdfs:/Data/1998/4/";
ALTER TABLE flights ADD PARTITION(yeard=1998, monthd=5) LOCATION "hdfs:/Data/1998/5/";
ALTER TABLE flights ADD PARTITION(yeard=1998, monthd=6) LOCATION "hdfs:/Data/1998/6/";
ALTER TABLE flights ADD PARTITION(yeard=1998, monthd=7) LOCATION "hdfs:/Data/1998/7/";
ALTER TABLE flights ADD PARTITION(yeard=1998, monthd=8) LOCATION "hdfs:/Data/1998/8/";
ALTER TABLE flights ADD PARTITION(yeard=1998, monthd=9) LOCATION "hdfs:/Data/1998/9/";
ALTER TABLE flights ADD PARTITION(yeard=1998, monthd=10) LOCATION "hdfs:/Data/1998/10/";
ALTER TABLE flights ADD PARTITION(yeard=1998, monthd=11) LOCATION "hdfs:/Data/1998/11/";
ALTER TABLE flights ADD PARTITION(yeard=1998, monthd=12) LOCATION "hdfs:/Data/1998/12/";
ALTER TABLE flights ADD PARTITION(yeard=1999, monthd=1) LOCATION "hdfs:/Data/1999/1/";
ALTER TABLE flights ADD PARTITION(yeard=1999, monthd=2) LOCATION "hdfs:/Data/1999/2/";
ALTER TABLE flights ADD PARTITION(yeard=1999, monthd=3) LOCATION "hdfs:/Data/1999/3/";
ALTER TABLE flights ADD PARTITION(yeard=1999, monthd=4) LOCATION "hdfs:/Data/1999/4/";
ALTER TABLE flights ADD PARTITION(yeard=1999, monthd=5) LOCATION "hdfs:/Data/1999/5/";
ALTER TABLE flights ADD PARTITION(yeard=1999, monthd=6) LOCATION "hdfs:/Data/1999/6/";
ALTER TABLE flights ADD PARTITION(yeard=1999, monthd=7) LOCATION "hdfs:/Data/1999/7/";
ALTER TABLE flights ADD PARTITION(yeard=1999, monthd=8) LOCATION "hdfs:/Data/1999/8/";
ALTER TABLE flights ADD PARTITION(yeard=1999, monthd=9) LOCATION "hdfs:/Data/1999/9/";
ALTER TABLE flights ADD PARTITION(yeard=1999, monthd=10) LOCATION "hdfs:/Data/1999/10/";
ALTER TABLE flights ADD PARTITION(yeard=1999, monthd=11) LOCATION "hdfs:/Data/1999/11/";
ALTER TABLE flights ADD PARTITION(yeard=1999, monthd=12) LOCATION "hdfs:/Data/1999/12/";
ALTER TABLE flights ADD PARTITION(yeard=2000, monthd=1) LOCATION "hdfs:/Data/2000/1/";
ALTER TABLE flights ADD PARTITION(yeard=2000, monthd=2) LOCATION "hdfs:/Data/2000/2/";
ALTER TABLE flights ADD PARTITION(yeard=2000, monthd=3) LOCATION "hdfs:/Data/2000/3/";
ALTER TABLE flights ADD PARTITION(yeard=2000, monthd=4) LOCATION "hdfs:/Data/2000/4/";
ALTER TABLE flights ADD PARTITION(yeard=2000, monthd=5) LOCATION "hdfs:/Data/2000/5/";
ALTER TABLE flights ADD PARTITION(yeard=2000, monthd=6) LOCATION "hdfs:/Data/2000/6/";
ALTER TABLE flights ADD PARTITION(yeard=2000, monthd=7) LOCATION "hdfs:/Data/2000/7/";
ALTER TABLE flights ADD PARTITION(yeard=2000, monthd=8) LOCATION "hdfs:/Data/2000/8/";
ALTER TABLE flights ADD PARTITION(yeard=2000, monthd=9) LOCATION "hdfs:/Data/2000/9/";
ALTER TABLE flights ADD PARTITION(yeard=2000, monthd=10) LOCATION "hdfs:/Data/2000/10/";
ALTER TABLE flights ADD PARTITION(yeard=2000, monthd=11) LOCATION "hdfs:/Data/2000/11/";
ALTER TABLE flights ADD PARTITION(yeard=2000, monthd=12) LOCATION "hdfs:/Data/2000/12/";
ALTER TABLE flights ADD PARTITION(yeard=2001, monthd=1) LOCATION "hdfs:/Data/2001/1/";
ALTER TABLE flights ADD PARTITION(yeard=2001, monthd=2) LOCATION "hdfs:/Data/2001/2/";
ALTER TABLE flights ADD PARTITION(yeard=2001, monthd=3) LOCATION "hdfs:/Data/2001/3/";
ALTER TABLE flights ADD PARTITION(yeard=2001, monthd=4) LOCATION "hdfs:/Data/2001/4/";
ALTER TABLE flights ADD PARTITION(yeard=2001, monthd=5) LOCATION "hdfs:/Data/2001/5/";
ALTER TABLE flights ADD PARTITION(yeard=2001, monthd=6) LOCATION "hdfs:/Data/2001/6/";
ALTER TABLE flights ADD PARTITION(yeard=2001, monthd=7) LOCATION "hdfs:/Data/2001/7/";
ALTER TABLE flights ADD PARTITION(yeard=2001, monthd=8) LOCATION "hdfs:/Data/2001/8/";
ALTER TABLE flights ADD PARTITION(yeard=2001, monthd=9) LOCATION "hdfs:/Data/2001/9/";
ALTER TABLE flights ADD PARTITION(yeard=2001, monthd=10) LOCATION "hdfs:/Data/2001/10/";
ALTER TABLE flights ADD PARTITION(yeard=2001, monthd=11) LOCATION "hdfs:/Data/2001/11/";
ALTER TABLE flights ADD PARTITION(yeard=2001, monthd=12) LOCATION "hdfs:/Data/2001/12/";
ALTER TABLE flights ADD PARTITION(yeard=2002, monthd=1) LOCATION "hdfs:/Data/2002/1/";
ALTER TABLE flights ADD PARTITION(yeard=2002, monthd=2) LOCATION "hdfs:/Data/2002/2/";
ALTER TABLE flights ADD PARTITION(yeard=2002, monthd=3) LOCATION "hdfs:/Data/2002/3/";
ALTER TABLE flights ADD PARTITION(yeard=2002, monthd=4) LOCATION "hdfs:/Data/2002/4/";
ALTER TABLE flights ADD PARTITION(yeard=2002, monthd=5) LOCATION "hdfs:/Data/2002/5/";
ALTER TABLE flights ADD PARTITION(yeard=2002, monthd=6) LOCATION "hdfs:/Data/2002/6/";
ALTER TABLE flights ADD PARTITION(yeard=2002, monthd=7) LOCATION "hdfs:/Data/2002/7/";
ALTER TABLE flights ADD PARTITION(yeard=2002, monthd=8) LOCATION "hdfs:/Data/2002/8/";
ALTER TABLE flights ADD PARTITION(yeard=2002, monthd=9) LOCATION "hdfs:/Data/2002/9/";
ALTER TABLE flights ADD PARTITION(yeard=2002, monthd=10) LOCATION "hdfs:/Data/2002/10/";
ALTER TABLE flights ADD PARTITION(yeard=2002, monthd=11) LOCATION "hdfs:/Data/2002/11/";
ALTER TABLE flights ADD PARTITION(yeard=2002, monthd=12) LOCATION "hdfs:/Data/2002/12/";
ALTER TABLE flights ADD PARTITION(yeard=2003, monthd=1) LOCATION "hdfs:/Data/2003/1/";
ALTER TABLE flights ADD PARTITION(yeard=2003, monthd=2) LOCATION "hdfs:/Data/2003/2/";
ALTER TABLE flights ADD PARTITION(yeard=2003, monthd=3) LOCATION "hdfs:/Data/2003/3/";
ALTER TABLE flights ADD PARTITION(yeard=2003, monthd=4) LOCATION "hdfs:/Data/2003/4/";
ALTER TABLE flights ADD PARTITION(yeard=2003, monthd=5) LOCATION "hdfs:/Data/2003/5/";
ALTER TABLE flights ADD PARTITION(yeard=2003, monthd=6) LOCATION "hdfs:/Data/2003/6/";
ALTER TABLE flights ADD PARTITION(yeard=2003, monthd=7) LOCATION "hdfs:/Data/2003/7/";
ALTER TABLE flights ADD PARTITION(yeard=2003, monthd=8) LOCATION "hdfs:/Data/2003/8/";
ALTER TABLE flights ADD PARTITION(yeard=2003, monthd=9) LOCATION "hdfs:/Data/2003/9/";
ALTER TABLE flights ADD PARTITION(yeard=2003, monthd=10) LOCATION "hdfs:/Data/2003/10/";
ALTER TABLE flights ADD PARTITION(yeard=2003, monthd=11) LOCATION "hdfs:/Data/2003/11/";
ALTER TABLE flights ADD PARTITION(yeard=2003, monthd=12) LOCATION "hdfs:/Data/2003/12/";
ALTER TABLE flights ADD PARTITION(yeard=2004, monthd=1) LOCATION "hdfs:/Data/2004/1/";
ALTER TABLE flights ADD PARTITION(yeard=2004, monthd=2) LOCATION "hdfs:/Data/2004/2/";
ALTER TABLE flights ADD PARTITION(yeard=2004, monthd=3) LOCATION "hdfs:/Data/2004/3/";
ALTER TABLE flights ADD PARTITION(yeard=2004, monthd=4) LOCATION "hdfs:/Data/2004/4/";
ALTER TABLE flights ADD PARTITION(yeard=2004, monthd=5) LOCATION "hdfs:/Data/2004/5/";
ALTER TABLE flights ADD PARTITION(yeard=2004, monthd=6) LOCATION "hdfs:/Data/2004/6/";
ALTER TABLE flights ADD PARTITION(yeard=2004, monthd=7) LOCATION "hdfs:/Data/2004/7/";
ALTER TABLE flights ADD PARTITION(yeard=2004, monthd=8) LOCATION "hdfs:/Data/2004/8/";
ALTER TABLE flights ADD PARTITION(yeard=2004, monthd=9) LOCATION "hdfs:/Data/2004/9/";
ALTER TABLE flights ADD PARTITION(yeard=2004, monthd=10) LOCATION "hdfs:/Data/2004/10/";
ALTER TABLE flights ADD PARTITION(yeard=2004, monthd=11) LOCATION "hdfs:/Data/2004/11/";
ALTER TABLE flights ADD PARTITION(yeard=2004, monthd=12) LOCATION "hdfs:/Data/2004/12/";
ALTER TABLE flights ADD PARTITION(yeard=2005, monthd=1) LOCATION "hdfs:/Data/2005/1/";
ALTER TABLE flights ADD PARTITION(yeard=2005, monthd=2) LOCATION "hdfs:/Data/2005/2/";
ALTER TABLE flights ADD PARTITION(yeard=2005, monthd=3) LOCATION "hdfs:/Data/2005/3/";
ALTER TABLE flights ADD PARTITION(yeard=2005, monthd=4) LOCATION "hdfs:/Data/2005/4/";
ALTER TABLE flights ADD PARTITION(yeard=2005, monthd=5) LOCATION "hdfs:/Data/2005/5/";
ALTER TABLE flights ADD PARTITION(yeard=2005, monthd=6) LOCATION "hdfs:/Data/2005/6/";
ALTER TABLE flights ADD PARTITION(yeard=2005, monthd=7) LOCATION "hdfs:/Data/2005/7/";
ALTER TABLE flights ADD PARTITION(yeard=2005, monthd=8) LOCATION "hdfs:/Data/2005/8/";
ALTER TABLE flights ADD PARTITION(yeard=2005, monthd=9) LOCATION "hdfs:/Data/2005/9/";
ALTER TABLE flights ADD PARTITION(yeard=2005, monthd=10) LOCATION "hdfs:/Data/2005/10/";
ALTER TABLE flights ADD PARTITION(yeard=2005, monthd=11) LOCATION "hdfs:/Data/2005/11/";
ALTER TABLE flights ADD PARTITION(yeard=2005, monthd=12) LOCATION "hdfs:/Data/2005/12/";
ALTER TABLE flights ADD PARTITION(yeard=2006, monthd=1) LOCATION "hdfs:/Data/2006/1/";
ALTER TABLE flights ADD PARTITION(yeard=2006, monthd=2) LOCATION "hdfs:/Data/2006/2/";
ALTER TABLE flights ADD PARTITION(yeard=2006, monthd=3) LOCATION "hdfs:/Data/2006/3/";
ALTER TABLE flights ADD PARTITION(yeard=2006, monthd=4) LOCATION "hdfs:/Data/2006/4/";
ALTER TABLE flights ADD PARTITION(yeard=2006, monthd=5) LOCATION "hdfs:/Data/2006/5/";
ALTER TABLE flights ADD PARTITION(yeard=2006, monthd=6) LOCATION "hdfs:/Data/2006/6/";
ALTER TABLE flights ADD PARTITION(yeard=2006, monthd=7) LOCATION "hdfs:/Data/2006/7/";
ALTER TABLE flights ADD PARTITION(yeard=2006, monthd=8) LOCATION "hdfs:/Data/2006/8/";
ALTER TABLE flights ADD PARTITION(yeard=2006, monthd=9) LOCATION "hdfs:/Data/2006/9/";
ALTER TABLE flights ADD PARTITION(yeard=2006, monthd=10) LOCATION "hdfs:/Data/2006/10/";
ALTER TABLE flights ADD PARTITION(yeard=2006, monthd=11) LOCATION "hdfs:/Data/2006/11/";
ALTER TABLE flights ADD PARTITION(yeard=2006, monthd=12) LOCATION "hdfs:/Data/2006/12/";
ALTER TABLE flights ADD PARTITION(yeard=2007, monthd=1) LOCATION "hdfs:/Data/2007/1/";
ALTER TABLE flights ADD PARTITION(yeard=2007, monthd=2) LOCATION "hdfs:/Data/2007/2/";
ALTER TABLE flights ADD PARTITION(yeard=2007, monthd=3) LOCATION "hdfs:/Data/2007/3/";
ALTER TABLE flights ADD PARTITION(yeard=2007, monthd=4) LOCATION "hdfs:/Data/2007/4/";
ALTER TABLE flights ADD PARTITION(yeard=2007, monthd=5) LOCATION "hdfs:/Data/2007/5/";
ALTER TABLE flights ADD PARTITION(yeard=2007, monthd=6) LOCATION "hdfs:/Data/2007/6/";
ALTER TABLE flights ADD PARTITION(yeard=2007, monthd=7) LOCATION "hdfs:/Data/2007/7/";
ALTER TABLE flights ADD PARTITION(yeard=2007, monthd=8) LOCATION "hdfs:/Data/2007/8/";
ALTER TABLE flights ADD PARTITION(yeard=2007, monthd=9) LOCATION "hdfs:/Data/2007/9/";
ALTER TABLE flights ADD PARTITION(yeard=2007, monthd=10) LOCATION "hdfs:/Data/2007/10/";
ALTER TABLE flights ADD PARTITION(yeard=2007, monthd=11) LOCATION "hdfs:/Data/2007/11/";
ALTER TABLE flights ADD PARTITION(yeard=2007, monthd=12) LOCATION "hdfs:/Data/2007/12/";
ALTER TABLE flights ADD PARTITION(yeard=2008, monthd=1) LOCATION "hdfs:/Data/2008/1/";
ALTER TABLE flights ADD PARTITION(yeard=2008, monthd=2) LOCATION "hdfs:/Data/2008/2/";
ALTER TABLE flights ADD PARTITION(yeard=2008, monthd=3) LOCATION "hdfs:/Data/2008/3/";
ALTER TABLE flights ADD PARTITION(yeard=2008, monthd=4) LOCATION "hdfs:/Data/2008/4/";
ALTER TABLE flights ADD PARTITION(yeard=2008, monthd=5) LOCATION "hdfs:/Data/2008/5/";
ALTER TABLE flights ADD PARTITION(yeard=2008, monthd=6) LOCATION "hdfs:/Data/2008/6/";
ALTER TABLE flights ADD PARTITION(yeard=2008, monthd=7) LOCATION "hdfs:/Data/2008/7/";
ALTER TABLE flights ADD PARTITION(yeard=2008, monthd=8) LOCATION "hdfs:/Data/2008/8/";
ALTER TABLE flights ADD PARTITION(yeard=2008, monthd=9) LOCATION "hdfs:/Data/2008/9/";
ALTER TABLE flights ADD PARTITION(yeard=2008, monthd=10) LOCATION "hdfs:/Data/2008/10/";
ALTER TABLE flights ADD PARTITION(yeard=2008, monthd=11) LOCATION "hdfs:/Data/2008/11/";
ALTER TABLE flights ADD PARTITION(yeard=2008, monthd=12) LOCATION "hdfs:/Data/2008/12/";