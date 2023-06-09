import datetime
from functools import reduce
import time

import pandas as pd
import pika
import typer

import kubernetes_helper

app = typer.Typer()
DEPLOYMENT_NAME = "intelliscale-busy-server"


@app.command()
def ping():
    print("Pong")


# @app.command()
def run_metrics_collection(step_timeout: int):
    metrics_api = kubernetes_helper.create_kubernetes_metrics_api_client()
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()
    channel.queue_declare(queue="metrics")

    if metrics_api is None:
        print("API was not initialized. Exiting...")
        exit(1)
    while True:
        raw_metrics = kubernetes_helper.get_pods_metrics(
            deployment_name=DEPLOYMENT_NAME, api=metrics_api
        )
        pods_count = len(raw_metrics)
        pods_metrics = [
            {"cpu": pod["usage"]["cpu"], "memory": pod["usage"]["memory"]}
            for pod in raw_metrics
        ]
        cpu = reduce(
            lambda x, _: x + _,
            map(
                lambda pod: kubernetes_helper.convert_cpu_metric(pod["cpu"]),
                pods_metrics,
            ),
        )
        memory = reduce(
            lambda x, _: x + _,
            map(
                lambda pod: kubernetes_helper.convert_memory_metric(pod["memory"]),
                pods_metrics,
            ),
        )
        cpu_utilization = cpu / (pods_count * kubernetes_helper.LIMITS["cpu"])
        memory_utilization = memory / (pods_count * kubernetes_helper.LIMITS["memory"])
        metrics = [
            str(pods_count),
            str(cpu),
            str(memory),
            str(cpu_utilization),
            str(memory_utilization),
            str(datetime.datetime.now()),
        ]
        message = ",".join(metrics)
        channel.basic_publish(exchange="", routing_key="metrics", body=message)
        print(" [x] Sent %r" % message)

        time.sleep(step_timeout)


if __name__ == "__main__":
    # app()
    run_metrics_collection(5)
