from setuptools import find_packages, setup

setup(
    name="script_orchestrator",
    packages=find_packages(exclude=["script_orchestrator_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
