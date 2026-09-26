# Fraud Detection Project

An experimental fraud-detection platform for **banking transactions** and **insurance claims**. The repository focuses on real-time ingestion, model integration, and streaming predictions.

The machine-learning work is performed separately from the streaming application. Trained models are exported as artifacts, copied into the PySpark environment, and then loaded by the streaming job.

## Architecture

```text
┌──────────────────────────────┐
│ External ML development      │
│ notebooks / training process │
│ scikit-learn / XGBoost       │
└──────────────┬───────────────┘
               │ exported .pkl artifacts
               │ copied to pyspark/models
               ▼
┌─────────────────────────────────────────────────────────────┐
│ PySpark streaming application                               │
│                                                             │
│  Kafka input → Avro deserialization → Spark preprocessing  │
│                                      → model UDF prediction │
│                                      → Avro serialization   │
└──────────────┬──────────────────────────┬───────────────────┘
               │                          │
               ▼                          ▼
     Kafka: fraud_prediction       Cassandra: fraud_detection
```

### Runtime flow

1. External training notebooks prepare the credit and insurance datasets.
2. A model and its required preprocessing artifacts are serialized with `joblib`.
3. The artifacts are made available under `pyspark/models` and `pyspark/preprocessors`.
4. The PySpark notebook loads the appropriate model with `joblib.load`.
5. Kafka producers publish Avro-encoded events to:
   - `raw_credit_data`
   - `raw_insurance_data`
6. Spark Structured Streaming consumes the events and deserializes them using schemas from Schema Registry.
7. Spark preprocessing transforms the input into the feature vector expected by the model.
8. A PySpark UDF wraps the loaded model and calls `model.predict(...)` for each transformed record.
9. Predictions are serialized as Avro, published to `fraud_prediction`, and stored with the processed data in Cassandra.

The model is therefore not a service connected directly to Kafka. PySpark is the runtime that embeds the serialized model and exposes its prediction logic through a UDF.

## Technology choices

| Technology | Role | Rationale |
|---|---|---|
| Python 3.9 | ML runtime | Mature ecosystem for tabular data and experimentation |
| pandas / NumPy | Data preparation | Cleaning, transformation, and analysis |
| scikit-learn / XGBoost | Model training | Supervised classification for structured fraud data |
| joblib | Artifact serialization | Persists trained models and preprocessing objects |
| Jupyter | Development workflow | Iterative experimentation and visual analysis |
| Apache Kafka | Event transport | Decouples data producers from stream processing |
| Avro + Schema Registry | Message contracts | Defines and manages the format of Kafka messages |
| Apache Spark | Stream processing | Parses events, applies transformations, and runs inference |
| Cassandra | Result storage | Distributed storage keyed by `client_id` and `transaction_id` |
| Docker Compose | Local orchestration | Starts the complete development environment |

## Repository structure

```text
.
├── docker-compose.yml
├── ml_model/
│   └── py_files/                 # External model-training notebooks
├── pyspark/
│   ├── py_files/                 # Producers, Spark streaming, and prediction consumer
│   ├── models/                   # Model artifacts used by the streaming job
│   ├── preprocessors/            # Preprocessing artifacts used by the streaming job
│   ├── schemas/                  # Avro schemas used by PySpark
│   ├── csv/                      # Prepared datasets
│   └── json/                     # Categorical-value metadata
├── kafka/bash_scripts/           # Kafka topic initialization
├── schema_registry/
│   ├── schemas/                  # Avro schemas registered at startup
│   └── bash_scripts/             # Schema Registry initialization
└── cassandra/
    ├── cql_scripts/              # Keyspace and table definitions
    └── bash_scripts/             # Cassandra initialization
```

Checkpoint files and local runtime data are not part of the functional architecture.

## Getting started

### Prerequisites

- Docker Desktop with Docker Compose
- At least 8 GB of memory allocated to Docker is recommended
- Available ports: `2181`, `8081`, `8888`, `8889`, `9042`, and `9092`

### Start the stack

```bash
docker compose up --build
```

Jupyter interfaces:

- ML workspace: <http://localhost:8888>
- PySpark workspace: <http://localhost:8889>

The initialization containers create:

- Kafka topics `raw_credit_data`, `raw_insurance_data`, and `fraud_prediction`;
- Avro subjects in Schema Registry;
- the Cassandra keyspace `fraud_detection` with the `credit_data` and `insurance_data` tables.

Stop the stack with:

```bash
docker compose down
```

### Run the notebooks

1. Train or update a model in `ml_model/py_files/creditFraudModelDef.ipynb` or `insuranceFraudModelDef.ipynb`.
2. Export the model and preprocessing artifacts to the locations expected by the streaming workspace.
3. Ensure the corresponding artifacts are available in `pyspark/models` and `pyspark/preprocessors`.
4. Run `pyspark/py_files/credit_data_producer.ipynb` or `insurance_data_producer.ipynb`.
5. Run `pyspark/py_files/Untitled1.ipynb` to start the Spark streaming pipelines.
6. Run `pyspark/py_files/fraud_prediction_consdumer.ipynb` to observe the `fraud_prediction` topic.

## Data and model artifacts

The repository contains two input domains:

- **Credit**: transaction type, amount, and origin-account balances.
- **Insurance**: customer, claim, accident, channel, and vehicle characteristics.

The streaming notebook currently loads the credit and insurance model artifacts from `pyspark/models`, applies the matching preprocessing pipeline, and creates a prediction UDF for each model. The model and preprocessing artifacts must remain compatible with the feature columns and transformations used by PySpark.

## Current scope and limitations

- The ML training process and the streaming process are separate notebook-based workflows.
- There is no model-serving API in the current repository; inference is embedded in PySpark through a UDF.
- Docker Compose uses several `latest` images and a single Kafka broker, which is suitable for local development but not high availability.
- The notebooks contain the main application logic and should be extracted into tested Python modules before production use.
- The `fraud_prediction` Avro schema currently contains only the prediction value. Transaction identifiers should be added if consumers need to correlate a prediction with its source event.

## Recommended next steps

1. Package the training and streaming logic into versioned Python modules.
2. Define a model-artifact version and feature-schema compatibility check.
3. Add unit tests for preprocessing, UDF inference, and Avro serialization.
4. Add Kafka/Cassandra integration tests and operational monitoring.
5. Pin Docker image versions and introduce production-specific configuration and secret management .
