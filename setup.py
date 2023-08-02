from setuptools import setup, find_packages

setup(
    name="sqliteORM",
    version="0.2.1",
    description="Un package pour avoir une implémentation orientée objet de sqlite3",
    author="Combattant54",
    author_email="combat54ant@gmail.com",
    packages=find_packages(
        exclude=["*.examples"]
    ),
    install_requires=[],
    include_package_data=True,
)