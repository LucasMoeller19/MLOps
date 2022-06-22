import pandas as pd
import psycopg2

train_data = pd.read_csv("train.csv", sep=",", header=0)
"""train_data = pd.read_csv(filepath_or_buffer="../data/train.csv", sep=",", header=0)
test_data = pd.read_csv(filepath_or_buffer="../data/test.csv", sep=",", header=0)
gender_submission = pd.read_csv(
    filepath_or_buffer="../data/gender_submission.csv", sep=",", header=0
)"""

print(f"Shape of train data: {train_data.shape}")
"""print(f"Shape of test data: {test_data.shape}")
print(f"Shape of gender submission data: {gender_submission.shape}")"""
