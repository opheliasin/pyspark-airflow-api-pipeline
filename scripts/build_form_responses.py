from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, initcap, when, substring, row_number
from pyspark.sql import Window


def create_spark_session():
    return SparkSession.builder.appName("Build Responses Table Wide").getOrCreate()


def read_silver_tables(spark):
    form_answers_f = spark.read.parquet("./data/silver/form_answers_f")
    form_submissions_f = spark.read.parquet(
        "./data/silver/form_submissions_f")
    form_questions_d = spark.read.parquet("./data/silver/form_questions_d")
    return form_answers_f, form_submissions_f, form_questions_d


def create_wide_format(form_answers_f, form_submissions_f):

    return form_answers_f \
        .groupBy("landing_id") \
        .pivot("question_ref") \
        .agg(F.first("answer_value")) \
        .join(form_submissions_f, on="landing_id", how="left")

def rename_columns(gold_df, form_questions_d): 
    question_mapping = form_questions_d.select(
        "question_ref",
        "question_title_short"
        ).distinct().rdd.map(lambda x: (x.question_ref, x.question_title_short)).collectAsMap()

    new_columns = []
    for col_name in gold_df.columns:
        if col_name in question_mapping:
            new_name = question_mapping[col_name]
            if new_name: 
                new_columns.append(new_name)
            else:
                new_columns.append(col_name)
        else:
            new_columns.append(col_name)

    gold_df = gold_df.toDF(*new_columns)

    return gold_df

def clean_data(gold_df):
    # Coalesce country columns
    gold_df = gold_df.withColumn(
        "country_final",
        coalesce(
            col("country"),
            col("country_other"),
            col("region")
        )
    )

    # Capitalize first letter of city names
    gold_df = gold_df.withColumn(
        "city",
        initcap(col("city"))
    )

    # Remove + from phone numbers
    gold_df = gold_df.withColumn(
        "phone",
        substring(col("phone"), 2, 100)
)

    # Drop original country columns
    gold_df = gold_df.drop("country", "country_other", "region")

    return gold_df


def handle_duplicates(gold_df):
    # Identify duplicates based on email and phone
    window_spec = Window.partitionBy("email", "phone").orderBy(
        col("submitted_at_ts").desc())

    # Add row number to each submission within the same email+phone group
    gold_df = gold_df.withColumn(
        "row_num",
        row_number().over(window_spec)
    )

    # Keep only the latest submission
    gold_df = gold_df.filter(col("row_num") == 1)

    # Drop the row_num column
    gold_df = gold_df.drop("row_num")

    return gold_df

def main():
    try:
        # Create Spark session
        spark = create_spark_session()

        # Read silver tables
        form_answers_f, form_submissions_f, form_questions_d = read_silver_tables(
            spark)

        # Create wide format
        gold_df = create_wide_format(form_answers_f, form_submissions_f)

        # Rename columns
        gold_df = rename_columns(gold_df, form_questions_d)

        # Clean data
        gold_df = clean_data(gold_df)

        # Handle duplicates
        gold_df = handle_duplicates(gold_df)

        gold_df.write.mode("overwrite").parquet("../data/gold/form_responses_wide")

    except Exception as e:
        print(f"Error in gold layer processing: {str(e)}")
        raise

    finally:
        # Stop Spark session
        spark.stop()

