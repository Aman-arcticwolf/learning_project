from setuptools import setup, find_packages

setup(
    name="data-ai-pipeline",
    version="1.0.0",
    description="Scalable data pipeline with entity resolution, Apache Iceberg, and ML",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyspark>=3.5.0",
        "pyiceberg[glue,s3fs]>=0.6.0",
        "boto3>=1.34.0",
        "fuzzywuzzy>=0.18.0",
        "python-Levenshtein>=0.25.0",
        "mlflow>=2.9.2",
        "pydantic>=2.5.3",
        "python-dotenv>=1.0.0",
        "pyyaml>=6.0.1",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "black>=23.12.1",
            "flake8>=6.1.0",
            "pylint>=3.0.3",
            "mypy>=1.7.1",
        ]
    },
    python_requires=">=3.9",
)
