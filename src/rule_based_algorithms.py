import datetime
import pandas as pd


def calculate_pods(
    pods_count: int, metrics_df: pd.DataFrame, upper_threshold: float, bottom_threshold: float, scaling_period: int
) -> int:
    """
    Receiving pandas DataFrame
    pd.DataFrame(
        columns=[
            "pods_count",
            "cpu",
            "memory",
            "cpu_utilization",
            "memory_utilization",
            "timestamp",
        ]
    )
    """
    last_timestamp = datetime.datetime.strptime(metrics_df["timestamp"].max(), "%Y-%m-%d %H:%M:%S.%f") - datetime.timedelta(
        seconds=scaling_period
    )

    df = metrics_df[metrics_df["timestamp"] > str(last_timestamp)]
    mean_cpu_utilization = df["cpu_utilization"].mean()
    print(mean_cpu_utilization)
    if mean_cpu_utilization > upper_threshold:
        return pods_count + 1
    elif mean_cpu_utilization < bottom_threshold and pods_count > 1:
        return pods_count - 1
    else:
        return pods_count
# '2023-06-09 04:00:13.912116'