import boto3
import json
import time
from datetime import datetime
import pytz
import os
ist = pytz.timezone('Asia/Kolkata')

glue = boto3.client('glue')
s3 = boto3.client('s3')
data_glue = []
count = [0]
#get config from enviornment variable
bucket = os.environ['bucket']
key = os.environ['key']
def lambda_handler(event, context):
    # Time
    current_time = time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime())
    print(current_time)
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist)
    date_ist = current_time.strftime('%Y-%m-%d')
    # Get Config from S3 path
    print("Getting Config")
    config = s3.get_object(Bucket=bucket, Key=key)
    config = json.loads(config['Body'].read().decode('utf-8'))
    print(config)
    glue_main(config)
    json_data = json.dumps(data_glue,default=str)
    #print(data_glue)
    html_table = generate_html_table(json_data)
    #print(html_table)
    style = """<style>
    .failed-row{
        background-color:#E0115F;
        color:black;
        font-weight:bold;
    }
    .head-row{
        background-color:#258FFF;
        color:white;
        font-weight:bold;
    }
    .running-row{
        background-color:#00a86b;
        color:white;
        font-weight:bold;
    }
    .failed_table_style{
        background-color:#ffffff;
        color:black;
        font-weight:bold;
    }
    </style>"""
    styled_html_table = apply_row_styling(html_table)
    print(styled_html_table)
    mail_head = f'<h4>Hi Team,<br>Please find the Glue status report for {date_ist}.<br></h4>'
    all_head = '<h2>Job Details:</h2>'
    body = style+mail_head+all_head+styled_html_table
    print(body)

def glue_main(config):
    # Get Glue Jobs Status
    print("Getting Glue Jobs Status")
    for job_name in config['glue']:
        print(job_name)
        try:
            job_name,job_status,job_started_on,job_completed_on,execution_time = glue_jobs_status(job_name)
            print(job_name,job_status,job_started_on,job_completed_on,execution_time)
            count[0] = count[0]+1
            remark = "Job not updated today" if job_started_on.date() < datetime.now().date() else ''
            remark = "Manual Check Required!" if (int(execution_time)//60 <=1 and job_status!='RUNNING') else ''
            data_glue.append({
                "S.No" : count[0],
                "JobName": job_name,
                "JobStatus": job_status,
                "JobStartedOn": convert_timezone(job_started_on,ist)[:16],
                "JobCompletedOn": convert_timezone(job_completed_on,ist)[:16] if job_completed_on!=" " else " " ,
                "ExecutionTime": int(execution_time)//60,
                "Remark":  remark
            })
        except Exception as e:
            print(e)
            print(data_glue)
    print(data_glue)

def glue_jobs_status(job_name):
    job_response = glue.get_job_runs(
        JobName=job_name,
        MaxResults=1
    )
    job_status = job_response['JobRuns'][0]['JobRunState']
    job_started_on = job_response['JobRuns'][0]['StartedOn']
    try:
        job_completed_on = job_response['JobRuns'][0]['CompletedOn']
    except:
        job_completed_on = " "
    execution_time = job_response['JobRuns'][0]['ExecutionTime']
    return job_name,job_status,job_started_on,job_completed_on,execution_time

def generate_html_table(json_data):
    parsed_data = json.loads(json_data)
    headers = parsed_data[0].keys()
    table_rows = ['<tr><th style="border: 1px solid black;">{}</th></tr>'.format('</th><th style="border: 1px solid black;">'.join(headers))]  # Create table header row

    for row in parsed_data:
        values = [str(value) for value in row.values()]
        row_cells = []
        for value in values:
            row_cells.append('<td style="border: 1px solid black;text-align: center">{}</td>'.format(value))
        table_rows.append('<tr>{}</tr>'.format(''.join(row_cells)))  # Create table row

    table_html = '<table style="border-collapse: collapse;">{}</table>'.format(''.join(table_rows))  # Combine rows into table
    return table_html

def convert_timezone(timestamp, timezone):
    dt_object = datetime.strptime(str(timestamp)[0:19], '%Y-%m-%d %H:%M:%S')
    dt_object = dt_object.replace(tzinfo=pytz.utc)
    converted_time = dt_object.astimezone(timezone)
    return converted_time.strftime('%Y-%m-%d %H:%M:%S')

def apply_row_styling(html_table):
    # Find all rows that contain failed jobs
    start_tag = '<tr>'
    end_tag = '</tr>'
    failed_row_start = html_table.find(start_tag)
    while failed_row_start != -1:
        failed_row_end = html_table.find(end_tag, failed_row_start)
        row = html_table[failed_row_start:failed_row_end + len(end_tag)]
    
        if('SUCCEEDED' not in row):
            row = row.replace('<tr>', '<tr class="failed-row">')
        if('JobName' in row):
            row = row.replace('failed', 'head')
        if('RUNNING' in row):
            row = row.replace('failed', 'running')
        html_table = html_table[:failed_row_start] + row + html_table[failed_row_end + len(end_tag):]
        failed_row_start = html_table.find(start_tag, failed_row_end)
    return html_table
