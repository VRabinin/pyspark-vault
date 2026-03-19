# PySpark Playground

A simple Python project to test PySpark algorithms for Data Vault transformations on Spark.

## Development

**System Requirements**

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Java 17+

To start development, create a virtual environment and install dependencies:

```bash
$ cd pyspark-vault
$ uv venv
$ make init-dev
```

## Makefile Commands

| Command         | Description                              |
|-----------------|------------------------------------------|
| `make init`     | Install core dependencies                |
| `make init-dev` | Install core + dev dependencies          |
| `make test`     | Run tests                                |
| `make build`    | Build the wheel package into `dist/`     |

## Running Tests

```bash
$ make test
```

## Test Data
**Relational Schema**

![image](https://user-images.githubusercontent.com/62486916/153193425-a8337124-6ac6-441c-be21-f402025bb7af.png)

**Data Vault Schema**

![image](https://user-images.githubusercontent.com/62486916/153193456-639ae7a1-8d07-439e-aceb-679da637ed2f.png)
