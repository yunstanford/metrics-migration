import os
import subprocess
from uranium import task_requires


ROOT = os.path.dirname(os.path.realpath("__file__"))
WHISPER_FOR_PY3 = "https://github.com/graphite-project/whisper/tarball/feature/py3"


def main(build):
    build.packages.install(".", develop=True)
    build.packages.install("pip")
    build.executables.run([
        "{0}/bin/pip".format(ROOT), "install",
        WHISPER_FOR_PY3,
    ])


@task_requires("main")
def test(build):
    build.packages.install("pytest")
    build.packages.install("pytest-cov")
    build.packages.install("pytest-asyncio")
    build.packages.install("radon")
    build.packages.install("flake8")
    build.executables.run([
        "pytest", "./tests",
        "--cov", "migration",
        "--cov-report", "term-missing",
    ] + build.options.args)
    build.executables.run([
        "flake8", "migration", "tests"
    ])


def distribute(build):
    """ distribute the uranium package """
    build.packages.install("wheel")
    build.executables.run([
        "python", "setup.py",
        "sdist", "bdist_wheel", "--universal", "upload"
    ])


@task_requires("main")
def build_docs(build):
    build.packages.install("sphinx")
    build.packages.install("sphinx_rtd_theme")
    return subprocess.call(
        ["make", "html"], cwd=os.path.join(build.root, "docs")
    )


@task_requires("main")
def secrete(build):
    build.packages.install("motor")
    build.packages.install("pyyaml")
