#!/usr/bin/env python3

import os
import re
import json
import asyncio
import secrets
import argparse
import collections
import http.client
from pathlib import Path

ETCD_IMAGE = "pachyderm/etcd:v3.3.5"

NEWLINE_SEPARATE_OBJECTS_PATTERN = re.compile(r"\}\n+\{", re.MULTILINE)

# Path to file used for ensuring minikube doesn't need to be deleted.
# With newer versions of minikube, cluster state (host paths, pods, etc.) is
# persisted across host system restarts, but credentials aren't, causing
# permissions failures on k8s admin calls. We use this file to reference
# whether minikube has been started since the last host system restart, which
# will allow us to figure out whether to reset the cluster state so we don't
# get the permissions errors. It's stored in `/tmp` because that directory
# is wiped on every system restart, and doesn't require root to write to.
MINIKUBE_RUN_FILE = Path("/tmp/pachyderm-minikube-reset")

RunResult = collections.namedtuple("RunResult", ["rc", "stdout", "stderr"])

client_version = None
async def get_client_version():
    global client_version
    if client_version is None:
        client_version = (await capture("pachctl", "version", "--client-only")).strip()
    return client_version

class BaseDriver:
    def image(self, name):
        return name

    async def reset(self):
         # ignore errors here because most likely no cluster is just deployed
         # yet
        await run("helm", "delete", "pachyderm", raise_on_error=False)
        # Helm won't remove statefulset volumes by design
        await run("kubectl", "delete", "pvc", "-l", "suite=pachyderm")

    async def push_image(self, images):
        pass

    async def deploy(self):
        pull_images = [run("docker", "pull", ETCD_IMAGE)]

        await asyncio.gather(*pull_images)

        push_images = [ETCD_IMAGE, "pachyderm/pachd:local", "pachyderm/worker:local"]

        await asyncio.gather(*[self.push_image(i) for i in push_images])
        await run("helm", "install", "pachyderm", "etc/helm/pachyderm", "-f", "etc/helm/examples/local-dev-values.yaml")

        await retry(ping, attempts=60)

class MinikubeDriver(BaseDriver):
    async def reset(self):
        is_minikube_running = True

        async def minikube_status():
            await run("minikube", "status", capture_output=True)

        if MINIKUBE_RUN_FILE.exists():
            try:
                await minikube_status()
            except:
                is_minikube_running = False
        else:
            await run("minikube", "delete")
            is_minikube_running = False

        if not is_minikube_running:
            await run("minikube", "start")
            await retry(minikube_status)
            MINIKUBE_RUN_FILE.touch()

        await super().reset()

    async def push_image(self, image):
        await run("./etc/kube/push-to-minikube.sh", image)

    async def deploy(self):
        await super().deploy()

        # enable direct connect
        ip = (await capture("minikube", "ip")).strip()
        await run("pachctl", "config", "update", "context", f"--pachd-address={ip}:30650")

        # the config update above will cause subsequent deploys to create a
        # new context, so to prevent the config from growing in size on every
        # deploy, we'll go ahead and delete any "orphaned" contexts now
        for line in (await capture("pachctl", "config", "list", "context")).strip().split("\n")[1:]:
            context = line.strip()
            if context.startswith("local"):
                await run("pachctl", "config", "delete", "context", context)

class GCPDriver(BaseDriver):
    def __init__(self, project_id, cluster_name=None):
        if cluster_name is None:
            cluster_name = f"pach-{secrets.token_hex(5)}"
        self.cluster_name = cluster_name
        self.object_storage_name = f"{cluster_name}-storage"
        self.project_id = project_id

    def image(self, name):
        return f"gcr.io/{self.project_id}/{name}"

    async def reset(self):
        cluster_exists = (await run("gcloud", "container", "clusters", "describe", self.cluster_name,
            raise_on_error=False, capture_output=True)).rc == 0

        if cluster_exists:
            await super().reset()
        else:
            await run("gcloud", "config", "set", "container/cluster", self.cluster_name)
            await run("gcloud", "container", "clusters", "create", self.cluster_name, "--scopes=storage-rw",
                "--machine-type=n1-standard-8", "--num-nodes=2")

            account = (await capture("gcloud", "config", "get-value", "account")).strip()
            await run("kubectl", "create", "clusterrolebinding", "cluster-admin-binding",
                "--clusterrole=cluster-admin", f"--user={account}")

            await run("gsutil", "mb", f"gs://{self.object_storage_name}")

            docker_config_path = Path.home() / ".docker" / "config.json"
            await run("kubectl", "create", "secret", "generic", "regcred",
                f"--from-file=.dockerconfigjson={docker_config_path}",
                "--type=kubernetes.io/dockerconfigjson")

    async def push_image(self, image):
        image_url = self.image(image)
        if ":local" in image_url:
            image_url = image_url.replace(":local", ":" + (await get_client_version()))
        await run("docker", "tag", image, image_url)
        await run("docker", "push", image_url)

    def deploy_args(self):
        return ["google", self.object_storage_name, "32", "--dynamic-etcd-nodes=1", "--image-pull-secret=regcred",
            f"--registry=gcr.io/{self.project_id}"]

async def run(cmd, *args, raise_on_error=True, stdin=None, capture_output=False, timeout=None, cwd=None):
    print_status("running: `{} {}`".format(cmd, " ".join(args)))

    proc = await asyncio.create_subprocess_exec(
        cmd, *args,
        stdin=asyncio.subprocess.PIPE if stdin is not None else None,
        stdout=asyncio.subprocess.PIPE if capture_output else None,
        stderr=asyncio.subprocess.PIPE if capture_output else None,
        cwd=cwd,
    )
    
    future = proc.communicate(input=stdin.encode("utf8") if stdin is not None else None)
    result = await (future if timeout is None else asyncio.wait_for(future, timeout=timeout))

    if capture_output:
        stdout, stderr = result
        stdout = stdout.decode("utf8")
        stderr = stderr.decode("utf8")
    else:
        stdout, stderr = None, None

    if raise_on_error and proc.returncode:
        raise Exception(f"unexpected return code from `{cmd}`: {proc.returncode}")

    return RunResult(rc=proc.returncode, stdout=stdout, stderr=stderr)

async def capture(cmd, *args, **kwargs):
    _, stdout, _ = await run(cmd, *args, capture_output=True, **kwargs)
    return stdout

def find_in_json(j, f):
    if f(j):
        return j

    iter = None
    if isinstance(j, dict):
        iter = j.values()
    elif isinstance(j, list):
        iter = j

    if iter is not None:
        for sub_j in iter:
            v = find_in_json(sub_j, f)
            if v is not None:
                return v

def print_status(status):
    print(f"===> {status}")

async def retry(f, attempts=10, sleep=1.0):
    """
    Repeatedly retries operation up to `attempts` times, with a given `sleep`
    between runs.
    """
    for i in range(attempts):
        try:
            return await f()
        except:
            if i == attempts - 1:
                raise
            await asyncio.sleep(sleep)

async def ping():
    await run("pachctl", "version", capture_output=True, timeout=5)

async def main():
    parser = argparse.ArgumentParser(description="Resets a pachyderm cluster.")
    parser.add_argument("--target", default="", help="Where to deploy")
    args = parser.parse_args()

    if "GOPATH" not in os.environ:
        raise Exception("Must set GOPATH")
    if "PACH_CA_CERTS" in os.environ:
        raise Exception("Must unset PACH_CA_CERTS\nRun:\nunset PACH_CA_CERTS")

    driver = None

    if args.target == "":
        # derive which driver to use from the k8s context name
        kube_context = await capture("kubectl", "config", "current-context", raise_on_error=False)
        kube_context = kube_context.strip() if kube_context else ""
        if kube_context == "minikube":
            print_status("using the minikube driver")
            driver = MinikubeDriver()
        elif kube_context == "docker-desktop":
            print_status("using the base driver")
            driver = BaseDriver()
        if driver is None:
            # minikube won't set the k8s context if the VM isn't running. This
            # checks for the presence of the minikube executable as an
            # alternate means.
            try:
                await run("minikube", "version", capture_output=True)
            except:
                pass
            else:
                driver = MinikubeDriver()
        if driver is None:
            raise Exception(f"could not derive driver from context name: {kube_context}")
    elif args.target == "minikube":
        print_status("using the minikube driver")
        driver = MinikubeDriver()
    elif args.target == "base":
        print_status("using the base driver")
        driver = BaseDriver()
    elif args.target.startswith("gcp"):
        print_status("using the gcp driver")
        project_id = (await capture("gcloud", "config", "get-value", "project")).strip()
        target_parts = args.target.split(":", maxsplit=1)
        cluster_name = target_parts[1] if len(target_parts) == 2 else None
        driver = GCPDriver(project_id, cluster_name)
    else:
        raise Exception(f"unknown target: {args.target}")

    await asyncio.gather(
        run("make", "docker-build"),
        run("make", "install"),
        driver.reset(),
    )

    await driver.deploy()

if __name__ == "__main__":
    asyncio.run(main(), debug=True)
