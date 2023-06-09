import typing

from kubernetes import client, config

MEMORY_TRANSFORM_TABLE = {
    "Ei": 1_152_921_504_606_846_976,
    "Pi": 1_125_899_906_842_624,
    "Ti": 1_099_511_627_776,
    "Gi": 1_073_741_824,
    "Mi": 1_048_576,
    "Ki": 1_024,
}

CPU_TRANSFORM_TABLE: typing.Dict[str, int] = {"m": 1000, "n": 1000000000}

LIMITS = {
    "cpu": 1.0,
    "memory": 1_073_741_824.0,
}


def create_kubernetes_metrics_api_client() -> typing.Optional[client.CustomObjectsApi]:
    print("Initiating client connection...")
    api = None
    try:
        config.load_kube_config()
        api = client.CustomObjectsApi()
    except Exception:
        print("Connection failed :-(")
    else:
        print("Connection Succeeded!")

    return api


def create_kubernetes_apps_api_client() -> typing.Optional[client.AppsV1Api]:
    print("Initiating client connection...")
    api = None
    try:
        config.load_kube_config()
        api = client.AppsV1Api()
    except Exception:
        print("Connection failed :-(")
    else:
        print("Connection Succeeded!")

    return api


def get_pods_metrics(
    deployment_name: str, api: client.CustomObjectsApi
) -> typing.List[typing.Dict[str, typing.Any]]:
    print(f"Extracting metrics for deployment {deployment_name}")
    k8s_pods = api.list_cluster_custom_object("metrics.k8s.io", "v1beta1", "pods")
    pods_metrics = []

    for pod in k8s_pods["items"]:
        if deployment_name in pod["metadata"]["name"]:
            pods_metrics.append(pod["containers"][0])

    return pods_metrics


def convert_cpu_metric(pretty_cpu: str) -> float:
    cpu = 0
    for suffix, factor in CPU_TRANSFORM_TABLE.items():
        if pretty_cpu.endswith(suffix):
            cpu = int(pretty_cpu.replace(suffix, "")) / factor

    return cpu


def convert_memory_metric(pretty_memory: str):
    memory_bytes = 0
    for suffix, factor in MEMORY_TRANSFORM_TABLE.items():
        if pretty_memory.endswith(suffix):
            memory_bytes = int(pretty_memory.replace(suffix, "")) * factor

    return memory_bytes


def update_deployment(api: client.AppsV1Api, deployment_name: str, replicas_count: int):
    # patch the deployment
    print(f"Patching {deployment_name=} with {replicas_count=}")
    resp = api.patch_namespaced_deployment_scale(
        name=deployment_name, namespace="default", body={"spec": {"replicas": replicas_count}}
    )

    print("\n[INFO] deployment's container image updated.\n")
