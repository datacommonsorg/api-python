workspace(name="datacommons")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# The following rules are needed to perform pip-install of dependencies.
# Reference: https://github.com/bazelbuild/rules_python
git_repository(
    name = "io_bazel_rules_python",
    remote = "https://github.com/drigz/rules_python.git",
    commit = "8b5d0683a7d878b28fffe464779c8a53659fc645",
)
load("@io_bazel_rules_python//python:pip.bzl", "pip_repositories")
pip_repositories()

# Install the requirements.
load("@io_bazel_rules_python//python:pip.bzl", "pip_import")
pip_import(
    name = "requirements",
    requirements = "//:requirements.bazel.txt",
)
load("@requirements//:requirements.bzl", "pip_install")
pip_install()
