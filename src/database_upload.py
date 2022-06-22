import os
import sys
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()


def concatenate_data(
    train_data_path: str, test_data_path: str, gender_submission_path: str
) -> pd.DataFrame:
    """
    Read and concatenate separate csv files in order to create one single complete titanic dataset.
    Titanic dataset will be exported as separate csv file.

    Args:
        train_data_path (str): Path to train data
        test_data_path (str): Path to test data w/o labels
        gender_submission_path (str): Path to PassengerId and Survived label

    Returns:
        pd.DataFrame: Return concatenated dataframe with complete dataset
    """
    ### Read raw data ###
    train_data = pd.read_csv(train_data_path, sep=",", header=0)
    test_data = pd.read_csv(test_data_path, sep=",", header=0)
    gender_submission = pd.read_csv(gender_submission_path, sep=",", header=0)

    ### Join test data and gender submisson data on PassengerId ###
    test_data_with_label = pd.merge(test_data, gender_submission, on="PassengerId")
    test_data_with_label = test_data_with_label.loc[:, train_data.columns]

    ### Concatenating train and test data with labels into one dataframe ###
    df = pd.concat([train_data, test_data_with_label])
    print(f"Shape of test data with labels: {df.shape}")

    ### Save complete data as separate csv file ###
    df.to_csv(
        "/Users/lucasmoeller/Desktop/Lucas/Projects/MLOps/data/titanic_data.csv",
        sep=",",
        header=True,
        index=False,
    )

    return df


db_params = {
    "host": os.environ.get("HOST"),
    "database": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
}


def connect(params: dict) -> psycopg2.connect:
    """Connect to PostgreSQL database.

    Args:
        params (dict): Dictionary with connection parameters.

    Returns:
        psycopg2.connect: Returns psycopg2.connect object.
    """

    conn = None

    try:
        print("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(**params)
    except (Exception, psycopg2.psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        sys.exit(1)
    return conn


def execute_query_with_values(
    conn: psycopg2.connect, df: pd.DataFrame, table: str
) -> None:
    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ",".join(df.columns.to_list())

    ### SQL query to execute ###
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()

    try:
        execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("The dataframe is inserted.")
    cursor.close()


def main() -> None:
    df = concatenate_data(
        train_data_path="/Users/lucasmoeller/Desktop/Lucas/Projects/MLOps/data/train.csv",
        test_data_path="/Users/lucasmoeller/Desktop/Lucas/Projects/MLOps/data/test.csv",
        gender_submission_path="/Users/lucasmoeller/Desktop/Lucas/Projects/MLOps/data/gender_submission.csv",
    )

    conn = connect(db_params)

    execute_query_with_values(conn, df, "titanic")

    # Close the connection
    conn.close()


if __name__ == "__main__":
    main()
