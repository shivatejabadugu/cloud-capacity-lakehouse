"""silver.node_events -> gold.dim_node (SCD Type 2).

Incremental by watermark: find nodes with new events, rebuild each affected
node's full version history from silver, then swap those nodes' rows in one
atomic MERGE (update changed versions, insert new ones, delete versions that no
longer exist). Unaffected nodes are never touched.
"""

from pyspark.sql import functions as F

from lakehouse.config import get_paths
from lakehouse.delta_utils import ensure_table, get_watermark, in_list_sql, read_pinned, set_watermark
from lakehouse.spark import get_spark
from lakehouse.transforms.gold import build_scd2

JOB = "gold_dim_node"


def main() -> None:
    paths = get_paths()
    spark = get_spark(JOB)
    events, version = read_pinned(spark, paths.silver_node_events)
    wm = get_watermark(spark, paths, JOB)
    new = events if wm is None else events.where(F.col("_silver_loaded_at") > F.lit(wm))
    new_hwm = new.agg(F.max("_silver_loaded_at")).first()[0]
    if new_hwm is None:
        print(f"{JOB}: nothing new (silver version {version})")
        return

    affected = [r.node_id for r in new.select("node_id").distinct().collect()]
    history = events.where(F.col("node_id").isin(affected))
    rebuilt = build_scd2(history).cache()

    dim = ensure_table(spark, rebuilt, paths.gold_dim_node)
    (
        dim.alias("t")
        .merge(rebuilt.alias("s"), "t.node_sk = s.node_sk")
        .whenMatchedUpdateAll(
            condition="t.attr_hash <> s.attr_hash OR t.effective_to <> s.effective_to OR t.is_current <> s.is_current"
        )
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete(condition=in_list_sql("t.node_id", affected))
        .execute()
    )
    set_watermark(spark, paths, JOB, new_hwm)
    print(f"{JOB}: rebuilt {len(affected)} nodes, {rebuilt.count()} versions; watermark -> {new_hwm}")


if __name__ == "__main__":
    main()
