import time
import pandas as pd
import datetime

import typer
import kubernetes_helper

app = typer.Typer()
DEPLOYMENT_NAME = "intelliscale-busy-server"


@app.command()
def ping():
    print("Pong")


@app.command()
def run_metrics_collection(stable_timeout: int):
    metrics_df = pd.DataFrame(
        columns=[
            "pods_count",
            "cpu",
            "memory",
            "cpu_utilization",
            "memory_utilization",
            "timestamp",
        ]
    )
    metrics_api = kubernetes_helper.create_kubernetes_metrics_api_client()
    apps_api = kubernetes_helper.create_kubernetes_apps_api_client()
    desired_pods_count = 0
    if metrics_api is None or apps_api is None:
        print("API was not initialized. Exiting...")
        exit(1)
    while True:
        metrics = kubernetes_helper.get_pods_metrics(
            deployment_name=DEPLOYMENT_NAME, api=metrics_api
        )
        pods_count = len(metrics)
        cpu = (
            kubernetes_helper.convert_cpu_metric(metrics[0]["usage"]["cpu"])
            / pods_count
        )
        memory = (
            kubernetes_helper.convert_memory_metric(metrics[0]["usage"]["memory"])
            / pods_count
        )
        cpu_utilization = (cpu / kubernetes_helper.LIMITS["cpu"]) / pods_count
        memory_utilization = (memory / kubernetes_helper.LIMITS["memory"]) / pods_count
        metrics_df.loc[len(metrics_df.index)] = [  # type: ignore
            pods_count,
            cpu,
            memory,
            cpu_utilization,
            memory_utilization,
            str(datetime.datetime.now()),
        ]
        print(metrics_df.head(50))
        if cpu_utilization > 0.5:
            desired_pods_count = pods_count + 1
            print("Increasing pods replicas!!!")
            kubernetes_helper.update_deployment(apps_api, DEPLOYMENT_NAME, pods_count + 1) # think of doing this async
            break
        elif cpu_utilization < 0.1 and pods_count > 1:
            print("Decreasing pods replicas!!!")
        time.sleep(stable_timeout)


if __name__ == "__main__":
    app()
