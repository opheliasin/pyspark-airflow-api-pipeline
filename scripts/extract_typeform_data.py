import requests
import datetime
import csv
import os
import json
from datetime import timezone


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
            question_validations = question.get("validations", [])
            question_required = question.get(
                "validations", {}).get("required", False)
            question_type = question.get("type", [])

            print(f"question_id: {question_id}, question_title: {question_title}, question_ref: {question_ref}, question_properties: {question_properties}, question_required: {question_required}, question_type: {question_type}")
            questions.append([question_id, question_title, question_ref, question_properties,
                             question_validations, question_required, question_type])

        with open('./data/typeform_questions.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['question_id', 'question_title', 'question_ref', 'question_properties',
                            'question_validations', 'question_required', 'question_type'])  # header
            writer.writerows(questions)
    else:
        print("Error:", response.status_code, response.text)


def get_typeform_answers(form_id, headers):
    BASE_URL = f"https://api.typeform.com/forms/{form_id}/responses"

    end_date = datetime.date.today()
    end_time = "00:00:00"
    start_date = end_date - datetime.timedelta(days=1)
    start_time = "00:00:00"
    page_size = 1000
    # timeframe = f"since={start_date}T{start_time}&until={end_date}T{end_time}"

    params = {
        "page_size": page_size,
        "since": str(start_date) + "T" + start_time,
        "until": str(end_date) + "T" + end_time
    }

    # endpoint = f"{self.BASE_URL}/forms/{form_id}/responses?{timeframe}&page_size={page_size}"

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
            formatted_responses.append([landing_id, landed_at, submitted_at, metadata, datetime.datetime.now(
                timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), json.dumps(answers)])

            # print(f"answers: {answers}")

            # for answer in answers:
            #     question_id = answer.get("field", {}).get("id")  # Extract question ID
            #     response_value = (
            #         answer.get("text") or
            #         answer.get("phone_number") or
            #         answer.get("email") or
            #         answer.get("choice", {}).get("label")
            #     )

            #     if question_id and response_value:
            #         response_dict[question_id] = response_value

            # formatted_responses.append(response_dict)
            # print(f"formatted_responses: {formatted_responses}")
        file_exists = os.path.isfile('./data/typeform_responses.csv')
        with open('./data/typeform_responses.csv', mode='a', newline='') as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(['landing_id', 'landed_at', 'submitted_at',
                                'metadata', 'ingest_date', 'answers'])  # header
            writer.writerows(formatted_responses)
        # print("response: ", formatted_responses)
        # return formatted_responses
    else:
        print("Error:", response.status_code, response.text)


if __name__ == "__main__":
    with open("form_id.txt", "r") as file:
        form_id = file.read().strip()

    with open("access_token.txt", "r") as file:
        access_token = file.read().strip()

    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    get_typeform_questions(form_id, headers)
    # get_typeform_answers(form_id, headers)
