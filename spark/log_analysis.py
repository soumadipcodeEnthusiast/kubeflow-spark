import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
import time

def main():
    start_time = time.time()
    spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()
    
    # Generate 2M fake web logs
    num_rows = 2000000
    df = spark.range(num_rows)
    
    # Simulate endpoints, status codes, and latencies
    df = df.withColumn("endpoint", F.expr("element_at(array('/home', '/api/users', '/api/orders', '/login', '/checkout'), cast(rand() * 5 + 1 as int))"))
    df = df.withColumn("status_code", F.expr("case when rand() < 0.95 then 200 when rand() < 0.98 then 404 else 500 end"))
    df = df.withColumn("latency_ms", F.expr("abs(randn()) * 50 + 100"))
    
    # Compute per-endpoint request count, 5xx rate, and p95 latency
    stats = df.groupBy("endpoint").agg(
        F.count("*").alias("request_count"),
        F.sum(F.when(F.col("status_code") >= 500, 1).otherwise(0)).alias("5xx_count"),
        F.expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency_ms")
    ).withColumn("5xx_rate", F.col("5xx_count") / F.col("request_count"))
    
    # Overall 5xx rate for the pipeline condition
    overall_5xx = df.where(F.col("status_code") >= 500).count()
    overall_rate = overall_5xx / num_rows
    
    # Collect results
    results = stats.collect()
    endpoint_stats = []
    for row in results:
        endpoint_stats.append({
            "endpoint": row["endpoint"],
            "request_count": row["request_count"],
            "5xx_rate": row["5xx_rate"],
            "p95_latency_ms": row["p95_latency_ms"]
        })
    
    end_time = time.time()
    
    output = {
        "overall_error_rate": overall_rate,
        "endpoint_stats": endpoint_stats,
        "spark_elapsed_s": end_time - start_time
    }
    
    print(f"RESULT_JSON:{json.dumps(output)}")
    
    spark.stop()

if __name__ == "__main__":
    main()
