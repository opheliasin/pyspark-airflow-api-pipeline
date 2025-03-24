import requests
import datetime
import csv
import os
import pandas as pd

def clean_forms_data():
    print("🔹 Cleaning forms data...")

    # Read the CSV file
    df = pd.read_csv("../data/typeform_responses.csv")

    

if __name__ == "__main__":
    clean_forms_data()