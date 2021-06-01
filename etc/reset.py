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
PG_BOUNCER_IMAGE = "edoburu/pgbouncer:1.15.0"
IDE_USER_IMAGE = "pachyderm/ide-user:local"
IDE_HUB_IMAGE = "pachyderm/ide-hub:local"
PIPELINE_BUILD_DIR = "etc/pipeline-build"

DELETABLE_RESOURCES = [
    "roles.rbac.authorization.k8s.io",
    "rolebindings.rbac.authorization.k8s.io"
]

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
        # Check for the presence of the pachyderm IDE to see whether it should
        # be undeployed too. Using kubectl rather than helm here because
        # this'll work even if the helm CLI is not installed.
        undeploy_args = []
        jupyterhub_apps = json.loads(await capture("kubectl", "get", "pod", "-lapp=jupyterhub", "-o", "json"))
        if len(jupyterhub_apps["items"]) > 0:
            undeploy_args.append("--ide")

         # ignore errors here because most likely no cluster is just deployed
         # yet
        await run("pachctl", "undeploy", "--metadata", *undeploy_args, stdin="y\n", raise_on_error=False)
        # clear out resources not removed from the undeploy process
        await run("kubectl", "delete", ",".join(DELETABLE_RESOURCES), "-l", "suite=pachyderm")

    async def push_image(self, images):
        pass

    def deploy_args(self):
        # We use hostpaths for storage. On docker for mac and minikube,
        # hostpaths aren't cleared until the VM is restarted. Because of this
        # behavior, re-deploying on the same hostpath without a restart will
        # cause us to bring up a new pachyderm cluster with access to the old
        # cluster volume, causing a bad state. This works around the issue by
        # just using a different hostpath on every deployment.
        host_path = Path("/var") / f"pachyderm-{secrets.token_hex(5)}"
        return ["local", "-d", "--no-guaranteed", f"--host-path={host_path}"]

    async def deploy(self, dash, ide, builder_images):
        deploy_args = ["pachctl", "deploy", *self.deploy_args(), "--dry-run", "--create-context", "--log-level=debug"]
        if not dash:
            deploy_args.append("--no-dashboard")

        deployments_str = await capture(*deploy_args)
        deployments_json = json.loads("[{}]".format(NEWLINE_SEPARATE_OBJECTS_PATTERN.sub("},{", deployments_str)))

        dash_spec = find_in_json(deployments_json, lambda j: \
            isinstance(j, dict) and j.get("name") == "dash" and j.get("image") is not None)
        grpc_proxy_spec = find_in_json(deployments_json, lambda j: \
            isinstance(j, dict) and j.get("name") == "grpc-proxy")
        
        pull_images = [run("docker", "pull", ETCD_IMAGE)]
        if dash_spec is not None:
            pull_images.append(run("docker", "pull", dash_spec["image"]))
        if grpc_proxy_spec is not None:
            pull_images.append(run("docker", "pull", grpc_proxy_spec["image"]))
        await asyncio.gather(*pull_images)

        push_images = [ETCD_IMAGE, "pachyderm/pachd:local", "pachyderm/worker:local", PG_BOUNCER_IMAGE, *builder_images]
        if dash_spec is not None:
            push_images.append(dash_spec["image"])
        if grpc_proxy_spec is not None:
            push_images.append(grpc_proxy_spec["image"])

        await asyncio.gather(*[self.push_image(i) for i in push_images])
        await run("kubectl", "create", "-f", "-", stdin=deployments_str)

        await retry(ping, attempts=60)

        if ide:
            await asyncio.gather(*[self.push_image(i) for i in [IDE_USER_IMAGE, IDE_HUB_IMAGE]])

            await run("pachctl", "enterprise", "activate", stdin=os.environ["PACH_ENTERPRISE_KEY"])
            await run("pachctl", "auth", "activate", stdin="admin\n")
            await run("pachctl", "deploy", "ide", 
                "--user-image", self.image(IDE_USER_IMAGE),
                "--hub-image", self.image(IDE_HUB_IMAGE),
            )

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

    async def deploy(self, dash, ide, builder_images):
        await super().deploy(dash, ide, builder_images)

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

class HubApiError(Exception):
    def __init__(self, errors):
        def get_message(error):
            try:
                return f"{error['title']}: {error['detail']}"
            except KeyError:
                return json.dumps(error)

        if len(errors) > 1:
            message = ["multiple errors:"]
            for error in errors:
                message.append(f"- {get_message(error)}")
            message = "\n".join(message)
        else:
            message = get_message(errors[0])

        super().__init__(message)
        self.errors = errors

class HubDriver:
    def __init__(self, api_key, org_id, cluster_name):
        self.api_key = api_key
        self.org_id = org_id
        self.cluster_name = cluster_name
        self.old_cluster_names = []

    def request(self, method, endpoint, body=None):
        headers = {
            "Authorization": f"Api-Key {self.api_key}",
        }
        if body is not None:
            body = json.dumps({
                "data": {
                    "attributes": body,
                }
            })

        conn = http.client.HTTPSConnection(f"hub.pachyderm.com")
        conn.request(method, f"/api/v1{endpoint}", headers=headers, body=body)
        response = conn.getresponse()
        j = json.load(response)

        if "errors" in j:
            raise HubApiError(j["errors"])
        
        return j["data"]

    async def push_image(self, src):
        dst = src.replace(":local", ":" + (await get_client_version()))
        await run("docker", "tag", src, dst)
        await run("docker", "push", dst)

    async def reset(self):
        if self.cluster_name is None:
            return

        for pach in self.request("GET", f"/organizations/{self.org_id}/pachs?limit=100"):
            if pach["attributes"]["name"].startswith(f"{self.cluster_name}-"):
                self.request("DELETE", f"/organizations/{self.org_id}/pachs/{pach['id']}")
                self.old_cluster_names.append(pach["attributes"]["name"])

    async def deploy(self, dash, ide, builder_images):
        if ide:
            raise Exception("cannot deploy IDE in hub")
        if len(builder_images):
            raise Exception("cannot deploy builder images")

        await asyncio.gather(
            self.push_image("pachyderm/pachd:local"),
            self.push_image("pachyderm/worker:local"),
        )

        response = self.request("POST", f"/organizations/{self.org_id}/pachs", body={
            "name": self.cluster_name or "sandbox",
            "pachVersion": await get_client_version(),
        })

        cluster_name = response["attributes"]["name"]
        gke_name = response["attributes"]["gkeName"]
        pach_id = response["id"]

        await run("pachctl", "config", "set", "context", cluster_name, stdin=json.dumps({
            "source": 2,
            "pachd_address": f"grpcs://{gke_name}.clusters.pachyderm.io:31400",
        }))

        await run("pachctl", "config", "set", "active-context", cluster_name)

        # hack-ey way to clean up the old contexts, now that the active
        # context has been swapped to the new cluster
        await asyncio.gather(*[
            run("pachctl", "config", "delete", "context", n, raise_on_error=False) for n in self.old_cluster_names
        ])

        await retry(ping, attempts=100)

        async def get_otp():
            response = self.request("GET", f"/organizations/{self.org_id}/pachs/{pach_id}/otps")
            return response["attributes"]["otp"]
        otp = await retry(get_otp, sleep=5)

        await run("pachctl", "auth", "login", "--one-time-password", stdin=f"{otp}\n")

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
    parser.add_argument("--dash", action="store_true", help="Deploy dash")
    parser.add_argument("--ide", action="store_true", help="Deploy IDE")
    parser.add_argument("--builders", action="store_true", help="Deploy images used in pipeline builds")
    args = parser.parse_args()

    if "PACH_CA_CERTS" in os.environ:
        raise Exception("Must unset PACH_CA_CERTS\nRun:\nunset PACH_CA_CERTS")
    if args.ide and "PACH_ENTERPRISE_KEY" not in os.environ:
        raise Exception("Must set PACH_ENTERPRISE_KEY")

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
    elif args.target.startswith("hub"):
        print_status("using the hub driver")
        target_parts = args.target.split(":", maxsplit=1)
        cluster_name = target_parts[1] if len(target_parts) == 2 else None
        driver = HubDriver(os.environ["PACH_HUB_API_KEY"], os.environ["PACH_HUB_ORG_ID"], cluster_name)
    else:
        raise Exception(f"unknown target: {args.target}")

    await asyncio.gather(
        run("make", "docker-build"),
        run("make", "install"),
        driver.reset(),
    )

    builder_images = []
    if args.builders:
        procs = []
        version = await get_client_version()
        for language in (d for d in os.listdir(PIPELINE_BUILD_DIR) if os.path.isdir(os.path.join(PIPELINE_BUILD_DIR, d))):
            builder_image = f"pachyderm/{language}-build:{version}"
            procs.append(run("docker", "build", "-t", builder_image, ".", cwd=os.path.join(PIPELINE_BUILD_DIR, language)))
            builder_images.append(builder_image)
        await asyncio.gather(*procs)
    
    await driver.deploy(args.dash, args.ide, builder_images)

if __name__ == "__main__":
    asyncio.run(main(), debug=True)
