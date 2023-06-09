import time

import pandas as pd
import pika
import typer
import kubernetes_helper
import rule_based_algorithms
import datetime


app = typer.Typer()
DEPLOYMENT_NAME: str = "intelliscale-busy-server"


@app.command()
def ping():
    print("Pong")


# @app.command()
def main(
    polling_timeout: float,
    mode: str,
    upper_cpu_threshold: float = 0.8,
    bottom_cpu_threshold: float = 0.1,
    scaling_period: int = 60,
    backup_timeout: int = 30,
    metric_log_filename: str = "metrics_log.csv",
):
    metrics_df = pd.DataFrame(
        columns=[
            "pods_count",
            "cpu",
            "memory",
            "cpu_utilization",
            "memory_utilization",
            "timestamp",
        ]
    )  # type: ignore
    apps_api = kubernetes_helper.create_kubernetes_apps_api_client()
    if apps_api is None:
        print("API was not initialized. Exiting...")
        exit(1)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()
    channel.queue_declare(queue="metrics")
    current_pods_count = 0
    desired_pods_count = 0
    last_backup_timestamp = datetime.datetime.now()

    while True:
        current_timestamp = datetime.datetime.now()
        _, _, raw_metrics = channel.basic_get(queue="metrics", auto_ack=True)
        if raw_metrics:
            metrics = raw_metrics.decode().split(",")
            print(f"Receiving metric {metrics=}")
            (
                pods_count,
                cpu,
                memory,
                cpu_utilization,
                memory_utilization,
                timestamp,
            ) = metrics
            current_pods_count = int(pods_count)

            metrics_df.loc[len(metrics_df.index)] = [  # type: ignore
                int(pods_count),
                float(cpu),
                float(memory),
                float(cpu_utilization),
                float(memory_utilization),
                timestamp,
            ]
            if mode == "rule-based":
                desired_pods_count = rule_based_algorithms.calculate_pods(
                    pods_count=int(pods_count),
                    metrics_df=metrics_df,
                    upper_threshold=upper_cpu_threshold,
                    bottom_threshold=bottom_cpu_threshold,
                    scaling_period=scaling_period,
                )
                if desired_pods_count != current_pods_count:
                    kubernetes_helper.update_deployment(
                        api=apps_api,
                        deployment_name=DEPLOYMENT_NAME,
                        replicas_count=desired_pods_count,
                    )
        if (current_timestamp - last_backup_timestamp).seconds > backup_timeout:
            last_backup_timestamp = current_timestamp
            metrics_df.to_csv(
                metric_log_filename, sep=",", index=False, mode="w"
            )
            # metrics_df.drop(metrics_df.index, inplace=True)

        time.sleep(polling_timeout)


if __name__ == "__main__":
    main(0.1, "rule-based", 0.8, 60)
