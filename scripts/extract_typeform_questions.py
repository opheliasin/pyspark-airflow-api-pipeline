import requests
# import datetime
import csv
import json


def get_typeform_questions(form_id, headers):
    url = f"https://api.typeform.com/forms/{form_id}"

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        form_data = response.json()

        questions = []

        # Extract questions
        for question in form_data.get("fields", []):
            question_id = question.get("id", [])
            question_title = question.get("title", [])
            question_ref = question.get("ref", [])
            question_properties = question.get("properties", [])
            question_required = question.get(
                "validations", {}).get("required", False)
            question_type = question.get("type", [])

            questions.append([question_id, question_title, question_ref, json.dumps(question_properties),
                             question_required, question_type])

        with open('./data/bronze/typeform_questions.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['question_id', 'question_title', 'question_ref', 'question_properties',
                             'question_required', 'question_type'])  # header
            writer.writerows(questions)
    else:
        print("Error:", response.status_code, response.text)


def main():
    from dotenv import load_dotenv
    import os

    load_dotenv()

    token = os.getenv("TYPEFORM_TOKEN")
    form_id = os.getenv("FORM_ID")

    headers = {
        "Authorization": f"Bearer {token}"
    }

    get_typeform_questions(form_id, headers)
