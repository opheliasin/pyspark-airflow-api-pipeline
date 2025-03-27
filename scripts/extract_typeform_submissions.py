import requests
import datetime
import csv
import os
import json
from datetime import timezone


def get_typeform_answers(form_id, headers, execution_date):
    BASE_URL = f"https://api.typeform.com/forms/{form_id}/responses"

    end_date = execution_date
    end_time = "00:00:00"
    start_date = end_date - datetime.timedelta(days=1)
    start_time = "00:00:00"
    page_size = 1000

    params = {
        "page_size": page_size,
        "since": str(start_date) + "T" + start_time,
        "until": str(end_date) + "T" + end_time
    }

    response = requests.get(BASE_URL, params=params, headers=headers)

    formatted_responses = []

    if response.status_code == 200:
        data = response.json()

        # response_dict = dict()

        # Extract form IDs
        # [] is just the default if the key is not found
        form_list = data.get("items", [])
        for form in form_list:
            landing_id = form.get("landing_id", "")
            landed_at = form.get("landed_at", "")
            submitted_at = form.get("submitted_at", "")
            metadata = form.get("metadata", [])
            answers = form.get("answers", [])
            formatted_responses.append([landing_id, landed_at, submitted_at, datetime.datetime.now(
                timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), metadata, json.dumps(answers)])

        file_exists = os.path.isfile('./data/bronze/typeform_responses.csv')
        with open('./data/bronze/typeform_responses.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(['landing_id', 'landed_at', 'submitted_at', 'ingested_at',
                                'metadata', 'answers'])  # header
            writer.writerows(formatted_responses)

    else:
        print("Error:", response.status_code, response.text)


def main(execution_date):
    import pendulum  # optional but preferred
    from dotenv import load_dotenv
    import os

    # Convert execution_date from string to datetime if it's coming from Airflow templating
    if isinstance(execution_date, str):
        execution_date = pendulum.parse(execution_date)

    load_dotenv()

    token = os.getenv("TYPEFORM_TOKEN")
    form_id = os.getenv("FORM_ID")

    headers = {
        "Authorization": f"Bearer {token}"
    }

    get_typeform_answers(form_id, headers, execution_date)
