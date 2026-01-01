from sqlalchemy import create_engine
import pandas as pd

def load_tosql(**kwargs):
    # Retrieve the transformed data from the previous task
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_task')

    # Convert to DataFrame if necessary
    if isinstance(transformed_data, dict):
        transformed_data = pd.DataFrame(transformed_data)

    # Connect to the PostgreSQL database
    engine = create_engine('postgresql://data_analyst:0@localhost:5432/bank_reviews_dw')

    # Extract dimension tables
    dim_topics = transformed_data[["topics"]]
    
    # Make sure the date exists and extract day/month/year if needed
    transformed_data["date"] = pd.to_datetime(transformed_data["date"], errors="coerce")
    transformed_data["day"] = transformed_data["date"].dt.day
    transformed_data["month"] = transformed_data["date"].dt.month
    transformed_data["year"] = transformed_data["date"].dt.year
    dim_date = transformed_data[["date", "day", "month", "year"]].drop_duplicates()

    reviews = transformed_data[["place_address", "text", "sentiment", "score"]].rename(columns={"text": "review"})
    dim_agency = transformed_data[["place_address", "city"]].drop_duplicates()

    # Save to PostgreSQL
    reviews.to_sql('reviews', engine, if_exists="append", index=False)
    dim_date.to_sql('dim_date', engine, if_exists="append", index=False)
    dim_topics.to_sql('dim_topics', engine, if_exists="append", index=False)
    dim_agency.to_sql('dim_agency', engine, if_exists="append", index=False)


