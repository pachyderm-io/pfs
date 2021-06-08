import os
import setuptools
import subprocess
import sys


if "PACHCTL" in os.environ:
    pachctl = os.environ["PACHCTL"]
else:
    from distutils.spawn import find_executable
    pachctl = find_executable("pachctl")

if pachctl is None:
    print(
        "pachctl not found, please install pachctl to get the correct version",
        file=sys.stderr,
    )
    sys.exit(-1)


def version(pachctl):
    """Get version using pachctl CLI"""
    return (
        subprocess.run([pachctl, "version", "--client-only"], capture_output=True)
        .stdout.decode("utf-8")
        .strip()
    )


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


setuptools.setup(
    name="python-pachyderm-proto",
    version=version(pachctl),
    author="Albert Cui",
    author_email="albert.cui@pachyderm.io",
    description="Auto-generated protobuf and gRPC code for python-pachyderm to consume.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pachyderm/python-pachyderm",
    package_dir={
        "": "src",
    },
    packages=setuptools.find_namespace_packages(where="src"),
    python_requires=">=3.5",
    install_requires=[
        "protobuf>=3.17.0,<4",
        "grpcio>=1.38.0,<2",
    ],
)
