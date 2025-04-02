from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import (
    regexp_replace, udf, col, from_json, explode, coalesce,
    to_timestamp, expr
)


def clean_json_string(s):
    if not s or s.strip() == "":
        return None
    try:
        s = s.strip('"')          # Remove outer double quotes
        return s
    except Exception as e:
        return None


def create_answer_schema():
    return ArrayType(StructType([
        StructField("field", StructType([
            StructField("id", StringType()),
            StructField("type", StringType()),
            StructField("ref", StringType())
        ])),
        StructField("type", StringType()),
        StructField("text", StringType()),
        StructField("email", StringType()),
        StructField("number", DoubleType()),
        StructField("boolean", BooleanType()),
        StructField("date", StringType()),
        StructField("choice", StructType([
            StructField("label", StringType()),
            StructField("other", StringType())
        ])),
        StructField("phone_number", StringType()),
        StructField("file_url", StringType())
    ]))


def create_metadata_schema():
    return StructType([
        StructField("user_agent", StringType()),
        StructField("platform", StringType()),
        StructField("referer", StringType()),
        StructField("network_id", StringType()),
        StructField("browser", StringType())
    ])


def create_question_properties_schema():
    return StructType([
        StructField("description", StringType()),
        StructField("choices", ArrayType(StructType([
            StructField("id", StringType()),
            StructField("ref", StringType()),
            StructField("label", StringType())
        ])))
    ])


def process_answers(spark, input_path):
    # Read the CSV file
    typeform_responses_df = spark.read.option("multiline", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("header", True) \
        .option("mode", "PERMISSIVE") \
        .csv(input_path, header=True)

    # Clean and parse answers
    typeform_responses_df = typeform_responses_df.withColumn(
        "answers_clean",
        regexp_replace("answers", '""', '"')
    )

    clean_json_udf = udf(clean_json_string, StringType())
    typeform_responses_df = typeform_responses_df.withColumn(
        "answers_fixed",
        clean_json_udf("answers_clean")
    )

    # Parse JSON answers
    typeform_responses_df = typeform_responses_df.withColumn(
        "answers_parsed",
        from_json(col("answers_fixed"), create_answer_schema())
    )

    # Explode answers
    typeform_responses_df_exploded = typeform_responses_df.withColumn(
        "answers",
        explode("answers_parsed")
    )

    # Create answers fact table
    answers_f = typeform_responses_df_exploded.select(
        "landing_id",
        col("answers.field.id").alias("question_id"),
        col("answers.field.ref").alias("question_ref"),
        col("answers.type").alias("answer_type"),
        coalesce(
            col("answers.text"),
            col("answers.email"),
            col("answers.number").cast("string"),
            col("answers.boolean").cast("string"),
            col("answers.date"),
            col("answers.phone_number"),
            col("answers.file_url"),
            col("answers.choice.label"),
            col("answers.choice.other")
        ).alias("answer_value")
    )

    return answers_f


def process_submissions(spark, input_path):
    # Read the CSV file
    typeform_responses_df = spark.read.option("multiline", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("header", True) \
        .option("mode", "PERMISSIVE") \
        .csv(input_path, header=True)

    # Parse metadata
    typeform_responses_df = typeform_responses_df.withColumn(
        "metadata_parsed",
        from_json(col("metadata"), create_metadata_schema())
    )

    # Create submissions fact table
    submissions_f = typeform_responses_df.select(
        "landing_id",
        "landed_at",
        "submitted_at",
        "ingested_at",
        col("metadata_parsed.referer")
    )

    # Convert timestamps
    submissions_f = submissions_f.withColumn(
        "landed_at_ts",
        to_timestamp(col("landed_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    ).withColumn(
        "submitted_at_ts",
        to_timestamp(col("submitted_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    ).withColumn(
        "ingested_at_ts",
        to_timestamp(col("ingested_at"), "yyyy-MM-dd HH:mm:ss")
    )

    # Drop original timestamp columns
    submissions_f = submissions_f.drop(
        "landed_at", "submitted_at", "ingested_at")

    # Parse UTM parameters
    submissions_f = submissions_f.withColumn(
        "typeform_source",
        expr("parse_url(referer, 'QUERY', 'typeform-source')")
    ).withColumn(
        "utm_source",
        expr("parse_url(referer, 'QUERY', 'utm_source')")
    ).withColumn(
        "utm_medium",
        expr("parse_url(referer, 'QUERY', 'utm_medium')")
    ).withColumn(
        "utm_campaign",
        expr("parse_url(referer, 'QUERY', 'utm_campaign')")
    )

    # Drop referer column
    submissions_f = submissions_f.drop("referer")

    return submissions_f


def process_questions(spark, input_path):
    # Read questions CSV
    typeform_questions_df = spark.read.option("multiline", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("header", True) \
        .option("mode", "PERMISSIVE") \
        .csv(input_path, header=True)

    # Parse question properties
    typeform_questions_df = typeform_questions_df.withColumn(
        "question_properties_parsed",
        from_json(col("question_properties"),
                  create_question_properties_schema())
    )

    # Create questions dimension table
    questions_d = typeform_questions_df.select(
        "question_id",
        "question_ref",
        "question_title",
        "question_required",
        "question_type",
        col("question_properties_parsed.description").alias(
            "question_description")
    )

    questions_d = create_short_question_title(questions_d)

    return questions_d


def create_short_question_title(typeform_questions_df):
    def map_column_name(title):
        title_lower = title.lower()

        # Define keyword mappings
        if "name" in title_lower:
            return "name"
        elif "email" in title_lower:
            return "email"
        elif "phone" in title_lower:
            return "phone"
        elif "citizen" in title_lower or "resident" in title_lower:
            return "is_us_citizen"
        elif "region" in title_lower:
            return "region"
        elif "state" in title_lower:
            return "state"
        elif "province" in title_lower or "territory" in title_lower:
            return "province"
        elif "country" in title_lower:
            if "other" in title_lower:
                return "country_other"
            return "country"
        elif "city" in title_lower:
            return "city"
        elif "location" in title_lower:
            return "location"
        elif "career" in title_lower or "education" in title_lower:
            return "career_status"
        elif "employment" in title_lower:
            return "employment_status"
        elif "income" in title_lower:
            return "annual_income"
        elif "funding" in title_lower:
            return "available_funding"
        elif "refer" in title_lower:
            if "name" in title_lower:
                return "referrer_name"
            return "is_referred"
        elif "congrat" in title_lower:
            return "congratulations"
        else:
            return title

    map_column_name_udf = udf(map_column_name, StringType())

    # Include shortened title column
    questions_d = typeform_questions_df.select(
        "question_id",
        "question_ref",
        "question_title",
        "question_required",
        "question_type",
        "question_description",
        map_column_name_udf("question_title").alias("question_title_short")
    )

    return questions_d


def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName(
        "Build Typeform Dimensions and Fact Tables").getOrCreate()

    try:
        # Process tables
        form_answers_f = process_answers(
            spark, "./data/bronze/typeform_responses.csv")
        form_submissions_f = process_submissions(
            spark, "./data/bronze/typeform_responses.csv")
        form_questions_d = process_questions(
            spark, "./data/bronze/typeform_questions.csv")

        # Write tables to parquet
        form_answers_f.write.mode("overwrite").parquet(
            "./data/silver/form_answers_f")
        form_submissions_f.write.mode("overwrite").parquet(
            "./data/silver/form_submissions_f")
        form_questions_d.write.mode("overwrite").parquet(
            "./data/silver/form_questions_d")

    finally:
        # Stop Spark session
        spark.stop()


if __name__ == "__main__":
    main()
