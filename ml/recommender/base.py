import os

from pyspark.sql import Window
from pyspark.sql import functions as F

# implicit-feedback confidence per event: a purchase signals real preference,
# a view barely any. these are the weights the ALS baseline multiplies interactions by.
EVENT_WEIGHTS = {"view": 1, "addtocart": 3, "transaction": 5}

TOP_N = 20          # related items stored per item
CANDIDATE_CAP = 3000  # cap the item universe before the O(n^2) similarity join

def pg_config():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "retailrocket")
    url = f"jdbc:postgresql://{host}:{port}/{db}"
    props = {
        "user": os.getenv("POSTGRES_USER", "retail"),
        "password": os.getenv("POSTGRES_PASSWORD", "retail"),
        "driver": "org.postgresql.Driver",
        "_host": host, "_port": port, "_db": db,
    }
    return url, props

def build_implicit_ratings(events):
    weight = F.create_map([F.lit(x) for kv in EVENT_WEIGHTS.items() for x in kv])
    rated = events.withColumn("w", weight[F.col("event")]).filter(F.col("w").isNotNull())
    return (rated.groupBy("visitorid", "itemid")
            .agg(F.sum("w").cast("double").alias("rating"))
            # ALS needs int ids; visitorid/itemid are well within int range for this data
            .withColumn("visitorid", F.col("visitorid").cast("int"))
            .withColumn("itemid", F.col("itemid").cast("int")))

def top_n_neighbors(vectors, top_n=TOP_N):
    # vectors: (id: long, vec: array<double>). cosine similarity between every pair,
    # keep the top_n per item. this is O(n^2) so callers cap the item set first;
    # a real system would use an ANN index (FAISS/annoy) instead.
    v = vectors.withColumn(
        "norm", F.expr("sqrt(aggregate(transform(vec, x -> x*x), 0D, (a, x) -> a + x))"))

    a = v.select(F.col("id").alias("item_a"), F.col("vec").alias("va"), F.col("norm").alias("na"))
    b = v.select(F.col("id").alias("item_b"), F.col("vec").alias("vb"), F.col("norm").alias("nb"))

    dot = F.expr("aggregate(zip_with(va, vb, (x, y) -> x*y), 0D, (acc, x) -> acc + x)")
    pairs = (a.join(b, F.col("item_a") < F.col("item_b"))
             .withColumn("score", dot / (F.col("na") * F.col("nb")))
             .select("item_a", "item_b", "score"))

    # emit both directions so each item has its own neighbour list
    both = pairs.select(F.col("item_a").alias("item_id"),
                        F.col("item_b").alias("rec_item_id"), "score") \
        .unionByName(pairs.select(F.col("item_b").alias("item_id"),
                                  F.col("item_a").alias("rec_item_id"), "score"))

    w = Window.partitionBy("item_id").orderBy(F.col("score").desc())
    return (both.withColumn("rank", F.row_number().over(w))
            .filter(F.col("rank") <= top_n))

def save_recommendations(df, method, url, props):
    import psycopg2
    # keep the method column so als and item2vec can live in one table and be compared.
    # refresh only this method's rows (delete-then-append) instead of overwriting the table.
    conn = psycopg2.connect(host=props["_host"], port=props["_port"], dbname=props["_db"],
                            user=props["user"], password=props["password"])
    with conn, conn.cursor() as cur:
        cur.execute("""
            create table if not exists item_recommendations (
                item_id bigint, rec_item_id bigint, score double precision,
                rank int, method text)
        """)
        cur.execute("delete from item_recommendations where method = %s", (method,))
    conn.close()

    jdbc_props = {k: v for k, v in props.items() if not k.startswith("_")}
    df.withColumn("method", F.lit(method)) \
        .select("item_id", "rec_item_id", "score", "rank", "method") \
        .write.mode("append").jdbc(url, "item_recommendations", properties=jdbc_props)
