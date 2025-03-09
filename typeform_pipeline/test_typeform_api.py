import requests
import datetime
import psycopg2

class TypeformAPI:
    BASE_URL = "https://api.typeform.com"

    def get_form_answers(self):
        form_id = "gqAQaDWU"
        end_date = datetime.date.today()
        end_time = "00%3A00%3A00"
        start_date = end_date - datetime.timedelta(days=1)
        start_time = "00%3A00%3A00"
        page_size = 1
        timeframe = f"since={start_date}T{start_time}&until={end_date}T{end_time}"
        endpoint = f"{self.BASE_URL}/forms/{form_id}/responses?{timeframe}&page_size={page_size}"
        
        with open("access_token.txt", "r") as file:
            access_token = file.read().strip()
        
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        response = requests.get(endpoint, headers=headers)
        
        formatted_responses = []  

        if response.status_code == 200:
            data = response.json()
            
            response_dict = dict()

            # Extract form IDs
            form_list = data.get("items", [])
            for form in form_list:
                answers = form.get("answers", [])
                print(f"answers: {answers}")

                for answer in answers:
                    question_id = answer.get("field", {}).get("id")  # Extract question ID
                    response_value = (
                        answer.get("text") or 
                        answer.get("phone_number") or
                        answer.get("email") or 
                        answer.get("choice", {}).get("label")
                    )
                    
                    if question_id and response_value:
                        response_dict[question_id] = response_value

                formatted_responses.append(response_dict)

            return formatted_responses

        else:
            print("Error:", response.status_code, response.text)

    def insert_responses_to_postgres(self):
        """
        Inserts transformed responses into PostgreSQL in row format.
        """
        extracted_data = self.get_form_answers()  # Fetch transformed responses

        if not extracted_data:
            print("⚠️ No responses found to insert.")
            return

        try:
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()

            # Get all unique question IDs from the extracted responses
            all_question_ids = set()
            for response in extracted_data:
                all_question_ids.update(response.keys())

            all_question_ids.discard("response_id")  # Remove response_id from column names
            all_question_ids.discard("submitted_at")  # Remove submitted_at from column names

            # Convert question IDs into SQL column names
            columns = ["response_id", "submitted_at"] + list(all_question_ids)
            placeholders = ", ".join(["%s"] * len(columns))
            insert_query = f"""
            INSERT INTO typeform_responses ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT (response_id) DO UPDATE SET
            {", ".join([f"{col} = EXCLUDED.{col}" for col in all_question_ids])};
            """

            # Convert each response into a tuple matching column order
            records = []
            for response in extracted_data:
                row_values = [response.get(col, None) for col in columns]
                records.append(tuple(row_values))

            cur.executemany(insert_query, records)
            conn.commit()
            cur.close()
            conn.close()
            print(f"✅ Successfully inserted {len(records)} responses into PostgreSQL.")

        except Exception as e:
            print(f"❌ Database Error: {e}")

        
if __name__ == "__main__":
    typeform_api = TypeformAPI()
    responses = typeform_api.get_form_answers()