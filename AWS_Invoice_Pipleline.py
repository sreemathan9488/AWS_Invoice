import boto3
import pdfplumber
import pandas as pd
import re
import os
import tempfile
import json
import psycopg2

#AWS_Connection
s3 = boto3.client('s3', region_name='eu-west-2')
sqs = boto3.client('sqs', region_name='eu-west-2')

INPUT_BUCKET = "invoice-input-project"
OUTPUT_BUCKET = "extratced-output-files"
SQS_QUEUE_URL = "https://sqs.eu-west-2.amazonaws.com/696630819021/InvoiceQueue"


#RDS Connection >Update with your own ID or create environmnet configuration
instead of providing real AWS _Info
DB_HOST = "*****"
DB_NAME = "****"
DB_USER = "****"
DB_PASSWORD = "*****"
DB_PORT = 5432

temp_dir = tempfile.gettempdir()

#Database_Connection
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

#Creating table_in rds
def create_table(conn, table_name):

    cur = conn.cursor()

    create_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        file_name TEXT,
        code TEXT,
        description TEXT,
        pack INT,
        size TEXT,
        qty INT,
        price FLOAT,
        value FLOAT,
        vat TEXT,
        rrp FLOAT,
        por FLOAT
    );
    """

    cur.execute(create_query)
    conn.commit()
    cur.close()

# Extracting data by using REGEX
def extract_invoice(pdf_path):

    headers = ["CODE","DESCRIPTION","PACK","SIZE","QTY","PRICE","VALUE","VAT","RRP","POR"]

    pattern = re.compile(
        r"^(\d{5,6})\s+(.+?)\s+(\d+)\s+([\w×]+)\s+(\d+)\s+([\d.]+.*?)\s+([\d.]+)\s+([A-Z])\s+([\d.]+)\s+([\d.]+%?)$"
    )

    rows = []

    with pdfplumber.open(pdf_path) as pdf:

        for page in pdf.pages:

            text = page.extract_text()

            if not text:
                continue

            for line in text.split("\n"):

                m = pattern.match(line.strip())

                if m:
                    rows.append(m.groups())

    df = pd.DataFrame(rows, columns=headers)

   
    df["PRICE"] = df["PRICE"].str.replace(r"[^\d.]", "", regex=True).astype(float)
    df["VALUE"] = df["VALUE"].str.replace(r"[^\d.]", "", regex=True).astype(float)
    df["RRP"] = df["RRP"].str.replace(r"[^\d.]", "", regex=True).astype(float)

   
    df["POR"] = df["POR"].str.replace("%", "", regex=False)
    df["POR"] = pd.to_numeric(df["POR"], errors="coerce")

    df["QTY"] = df["QTY"].astype(int)
    df["PACK"] = df["PACK"].astype(int)

    return df

#Insert_data in RDS


def insert_into_rds(conn, df, filename, table_name):

    cur = conn.cursor()

    insert_query = f"""
    INSERT INTO {table_name}
    (file_name, code, description, pack, size,
     qty, price, value, vat, rrp, por)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    for _, row in df.iterrows():

        cur.execute(insert_query,
            (filename,
             row.CODE,
             row.DESCRIPTION,
             row.PACK,
             row.SIZE,
             row.QTY,
             row.PRICE,
             row.VALUE,
             row.VAT,
             row.RRP,
             row.POR)
        )

    conn.commit()
    cur.close()

#Deelting SQS_Messgae
def process_sqs_message(message):

    try:

        body = json.loads(message["Body"])

        if "Records" not in body:
            print("Skipping message: no Records found")
            return

        s3_info = body["Records"][0]["s3"]

        bucket = s3_info["bucket"]["name"]
        key = s3_info["object"]["key"]

        filename = os.path.basename(key)
        output_key = filename.replace(".pdf", ".csv")

        print(f"Processing {filename}...")

     
        invoice_number = filename.replace("Booker-Invoice-", "").replace(".pdf", "")
        table_name = f"invoice_{invoice_number}"

        local_pdf = os.path.join(temp_dir, filename)
        s3.download_file(bucket, key, local_pdf)

       
        df = extract_invoice(local_pdf)

        local_csv = os.path.join(temp_dir, output_key)
        df.to_csv(local_csv, index=False)

        s3.upload_file(local_csv, OUTPUT_BUCKET, output_key)
        print(f"✅ CSV uploaded to {OUTPUT_BUCKET}/{output_key}")

        conn = get_connection()

        create_table(conn, table_name)
        insert_into_rds(conn, df, filename, table_name)

        conn.close()

        print(f"✅ Data inserted into table {table_name}")

   
        os.remove(local_pdf)
        os.remove(local_csv)

       
        sqs.delete_message(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=message["ReceiptHandle"]
        )

        print("✅ Message deleted from SQS\n")

    except Exception as e:
        print(f"❌ Error processing message: {e}")

#Deleting SQS
def listen_sqs():

    print("Listening for new files on SQS...")

    while True:

        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )

        if "Messages" in response:

            for message in response["Messages"]:
                process_sqs_message(message)


if __name__ == "__main__":
    listen_sqs()
