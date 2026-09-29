"""Daily table maintenance.

Streaming and MERGE-heavy tables accumulate many small files; OPTIMIZE compacts
them, and Z-ORDER clusters rows by the columns most queries filter on so Delta's
file-level min/max stats can skip files. VACUUM removes files no longer
referenced by the table (7-day retention keeps time travel working for a week).
"""

from delta.tables import DeltaTable

from lakehouse.config import get_paths
from lakehouse.spark import get_spark

RETENTION_HOURS = 168


def main() -> None:
    paths = get_paths()
    spark = get_spark("maintenance")
    plan = [
        (paths.bronze_raw, None),
        (paths.silver_metrics, ["node_id"]),
        (paths.silver_node_events, ["node_id"]),
        (paths.gold_fact_node_hourly, ["cluster_id", "node_id"]),
        (paths.gold_cluster_daily, None),
        (paths.silver_quarantine, None),
    ]
    for path, zorder in plan:
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        table = DeltaTable.forPath(spark, path)
        opt = table.optimize()
        metrics = opt.executeZOrderBy(*zorder) if zorder else opt.executeCompaction()
        m = metrics.select("metrics.numFilesAdded", "metrics.numFilesRemoved").first()
        print(f"optimize {path}: +{m[0]} files / -{m[1]} files (zorder={zorder})")
        table.vacuum(RETENTION_HOURS)


if __name__ == "__main__":
    main()
