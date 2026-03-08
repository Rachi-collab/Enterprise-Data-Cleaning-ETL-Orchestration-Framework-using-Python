### Folder Structure

AIRFLOW/
│
├── dags/
│   ├── retail_etl_dag.py        # Main Airflow DAG (pipeline orchestration)
│   ├── src/
│   │   ├── extract.py           # Extract logic
│   │   ├── transform.py         # Transform logic
│   │   └── load.py              # Load logic
│   └── __pycache__/             # Auto-generated Python cache
│
├── data/
│   ├── raw/
│   │   └── online_retail_II.csv # Original dataset
│   └── processed/               # Cleaned output data
│
├── logs/
│   ├── dag_id=retail_etl_pipeline
│   ├── dag_processor_manager
│   └── scheduler
│
├── .env                         # Environment variables
├── docker-compose.yaml          # Airflow Docker setup
└── README.md                    # Project documentation



### To start airflow
docker-compose up -d
->it will start in http://localhost:8080
### to stop
docker-compose down