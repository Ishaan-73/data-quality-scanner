import duckdb
con = duckdb.connect("tpcds_test.duckdb")
con.execute("INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=0.01);")
con.close()

import shutil, duckdb
shutil.copy("tpcds_test.duckdb", "tpcds_dirty.duckdb")
con = duckdb.connect("tpcds_dirty.duckdb")
con.execute("UPDATE store_sales SET ss_sold_date_sk = NULL WHERE rowid % 10 = 0;")
con.execute("INSERT INTO customer SELECT * FROM customer LIMIT 50;")
con.execute("UPDATE store_sales SET ss_store_sk = 999999 WHERE rowid % 20 = 0;")
con.execute("UPDATE item SET i_current_price = -99.99 WHERE rowid % 15 = 0;")
con.close()

