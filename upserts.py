"""
UPSERTS
Converted from the UPSERTS notebook.

Demonstrates a Delta upsert (MERGE) workflow:
1. Create the source table pyspark.upserts.products
2. Insert sample rows
3. Deduplicate by id keeping the latest updatedate
4. MERGE (upsert) the deduplicated source into a Delta table
   stored at /Volumes/pyspark/upserts/target/
"""

from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ---------------------------------------------------------------------------
# Cell 2: create the source Delta table
# NOTE: this fails if the table already exists; swap in CREATE TABLE IF NOT
# EXISTS (or DROP TABLE first) to make it re-runnable.
# ---------------------------------------------------------------------------
spark.sql("""
create table pyspark.upserts.products (
    id int,
    name string,
    price double,
    category string,
    updatedate timestamp
)
using delta
""")

# ---------------------------------------------------------------------------
# Cell 3: insert sample rows (original rows commented out, kept as-is)
# ---------------------------------------------------------------------------
spark.sql("""
insert into pyspark.upserts.products values
-- (1, 'iphone', 100, 'electronics', current_timestamp()),
-- (2, 'macbook', 200, 'electronics', current_timestamp()),
-- (3, 'shoes', 50, 'clothing', current_timestamp()),
-- (4, 'pants', 75, 'clothing', current_timestamp()),
-- (5, 'dress', 100, 'clothing', current_timestamp()),
(7, 'mobile', 2500, 'electronics', current_timestamp())
""")

# ---------------------------------------------------------------------------
# Cell 4: view the source table
# ---------------------------------------------------------------------------
spark.sql("select * from pyspark.upserts.products").show(truncate=False)

# ---------------------------------------------------------------------------
# Cell 5: deduplicate by id, keeping the latest updatedate per id
# ---------------------------------------------------------------------------
df = spark.sql("select * from pyspark.upserts.products")

df = (
    df.withColumn(
        "dedup",
        row_number().over(Window.partitionBy("id").orderBy(col("updatedate").desc())),
    )
    .filter(col("dedup") == 1)
    .drop("dedup")
)

df.show(truncate=False)

# ---------------------------------------------------------------------------
# Cell 6: MERGE (upsert) the deduplicated source into the target Delta table
# ---------------------------------------------------------------------------
deltaTable = DeltaTable.forPath(spark, "/Volumes/pyspark/upserts/target/")

if len(dbutils.fs.ls("/Volumes/pyspark/upserts/target")) > 0:
    (
        deltaTable.alias("tgt")
        .merge(df.alias("src"), "tgt.id = src.id")
        .whenMatchedUpdateAll(condition="1=1")
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df.write.format("delta").mode("overwrite").save("/Volumes/pyspark/upserts/target")

# ---------------------------------------------------------------------------
# Cell 7: view the target Delta table
# ---------------------------------------------------------------------------
spark.read.format("delta").load("/Volumes/pyspark/upserts/target").show(truncate=False)
