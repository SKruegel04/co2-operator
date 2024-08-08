# CO2-Operator

A Kubernetes operator that watches MOER-values and utilizes load shifting to reduce CO2 emissions.

## Installation

```bash
pip install -r requirements.txt
```

## Requirements

- Python 3.7+
- Docker

## Usage

```bash
# Start services (K3S, Postgres)
docker compose up --scale k3s-node=10

# Run the operator
python -m co2_operator
```

## Configuration

All configuration is done in the `co2_operator/__main__.py` file at the top or via a `.env.local` file (you can copy the `.env` file to create it)

By default it connects to the K3S-Server and Postgres-Database provided by the `compose.yml`.
