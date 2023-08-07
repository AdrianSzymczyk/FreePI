from setuptools import setup, find_packages
from pathlib import Path

VERSION = "0.0.1"
DESCRIPTION = "Yahoo Finance API"
LONG_DESCRIPTION = "This is API created for Yahoo Finance based on the web scraping"

BASE_DIR = Path(__file__).parent.absolute()
with open(Path(BASE_DIR, "requirements.txt")) as file:
    required_packages = [ln.strip() for ln in file.readlines()]

setup(
    name='FreePI',
    version=VERSION,
    author="Adrian Szymczyk",
    author_email="adiszymczyk@gmail.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    install_requires=[required_packages],
)
