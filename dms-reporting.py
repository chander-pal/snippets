import boto3
import json
import time
from datetime import datetime,timedelta
import pytz
import os
dms = boto3.client('dms')
s3 = boto3.client('s3')
cloudwatch = boto3.client('cloudwatch')

#get config from enviornment variable
bucket = os.environ['bucket']
key = os.environ['key']
threshhold_days = os.environ['threshhold_days']

data_dms = []
count = [0]
threshhold = (datetime.now() - timedelta(days=threshhold_days)).replace(tzinfo=pytz.utc)


def lambda_handler(event, context):
    # Time
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist)
    date_ist = current_time.strftime('%Y-%m-%d')
    print(date_ist)
    # Get Config from S3 path
    print("Getting Config")
    config = s3.get_object(Bucket=bucket, Key=key)
    config = json.loads(config['Body'].read().decode('utf-8'))
    print(config)
    dms_main(config)
    print(data_dms)
    json_data = json.dumps(data_dms, default=str)
    html_table = generate_html_table(json_data)
    print(html_table)
    styled_html_table = apply_row_styling(html_table)
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
    mail_head = f'<h4>Hi Team,<br>Please find the DMS status report for {date_ist}.'
    all_head = '<br><h2>DMS Task Details:</h2>'
    body = style+mail_head+all_head+styled_html_table
    print(body)
    


def dms_main(config):
    # Get DMS Status
    print("Getting DMS Status")
    for task_arn in config['dms_tasks']:
        print(task_arn)
        try:
            dms_status(task_arn)
        except Exception as e:
            print(e)

def dms_status(task_arn):
    
    task_response = dms.describe_replication_tasks(
        Filters=[
            {
                'Name': 'replication-task-arn',
                'Values': [
                    task_arn,
                ]
            },
        ]
    )
    for task in task_response['ReplicationTasks']:
        task_name = task['ReplicationTaskIdentifier']
        task_status = task['Status']
        task_type = task['MigrationType']
        try:
            task_stop_reason = task['StopReason']
        except:
            task_stop_reason = 'NA'
        latency = " "    
        if(task_type=='cdc'):
            instance = 'dms-replication-instance-prod-new'
            task_identifier = task_arn.split(':')[-1]
            response = get_metric(instance,task_identifier)
            try:
                latency = int(response['Datapoints'][0]['Average'])
            except:
                latency = " "
        stats_response = dms.describe_table_statistics(
            ReplicationTaskArn=task_arn
        )
        print(stats_response)
        for stat in stats_response['TableStatistics']:
            table_name = stat['TableName']
            last_update_time_utc = stat['LastUpdateTime']
            last_update_time = convert_timezone(last_update_time_utc, pytz.timezone('Asia/Kolkata'))
            last_update_time = datetime.strptime(str(last_update_time)[0:19], '%Y-%m-%d %H:%M:%S')
            if(task_type=='full-load'):
                Remark = "Table Not Updated Today"  if (last_update_time_utc.date() < datetime.now().date()) else " "
            elif((task_type=='cdc')):
                minutes =int((datetime.now().replace(tzinfo=None)-last_update_time_utc.replace(tzinfo=None)).total_seconds())//60
                Remark = f'Time difference is {minutes} minutes.' if (minutes<30 and latency<5000 and latency!=0) else 'Action Required'
            Task_Stop_Reason = 'Finished' if task_stop_reason=='Stop Reason FULL_LOAD_ONLY_FINISHED' else task_stop_reason
            if(last_update_time_utc>threshhold):
                count[0] = count[0]+1
                data_dms.append({
                    "S.No" : count[0],
                    "Task_Name": task_name,
                    "Task_Status": Task_Stop_Reason if ((task_type=='full-load') and (task_status=='stopped')) else task_status,
                    "Task_Type": task_type,
                    "Table_Name": table_name,
                    "Last_Update_Time": last_update_time,
                    "Latency":latency,
                    "Remark": Remark 
                })

def convert_timezone(timestamp, timezone):
    dt_object = datetime.strptime(str(timestamp)[0:19], '%Y-%m-%d %H:%M:%S')
    dt_object = dt_object.replace(tzinfo=pytz.utc)
    converted_time = dt_object.astimezone(timezone)
    return converted_time.strftime('%Y-%m-%d %H:%M:%S')


def generate_html_table(json_data):
    try:
        parsed_data = json.loads(json_data)
        headers = parsed_data[0].keys()
        table_rows = ['<tr class="head-row"><th style="border: 1px solid black;">{}</th></tr>'.format('</th><th style="border: 1px solid black;">'.join(headers))]  # Create table header row
        for row in parsed_data:
            values = [str(value) for value in row.values()]
            row_cells = []
            for value in values:
                row_cells.append('<td style="border: 1px solid black;text-align: center">{}</td>'.format(value))
            table_rows.append('<tr>{}</tr>'.format(''.join(row_cells)))  # Create table row
    
        table_html = '<table style="border-collapse: collapse;">{}</table>'.format(''.join(table_rows))  # Combine rows into table
    except:
        table_html=''
    return table_html
        

def apply_row_styling(html_table):
    # Find all rows that contain failed jobs
    start_tag = '<tr>'
    end_tag = '</tr>'
    failed_row_start = html_table.find(start_tag)
    while failed_row_start != -1:
        failed_row_end = html_table.find(end_tag, failed_row_start)
        row = html_table[failed_row_start:failed_row_end + len(end_tag)]
        if(('Table Not Updated Today' in row) or ('Action Required' in row)):
            row = row.replace('<tr>', '<tr class="failed-row">')
        if('Task_Name' in row):
            row = row.replace('failed', 'head')
        #if('RUNNING' in row):
            #row = row.replace('failed', 'running')
        html_table = html_table[:failed_row_start] + row + html_table[failed_row_end + len(end_tag):]
        failed_row_start = html_table.find(start_tag, failed_row_end)

    return html_table


def get_metric(instance, task):
    response = cloudwatch.get_metric_statistics(
        Namespace="AWS/DMS",
        MetricName="CDCLatencySource",
        Dimensions=[
            {"Name": "ReplicationInstanceIdentifier", "Value": instance},
            {"Name": "ReplicationTaskIdentifier", "Value":task},
        ],
        StartTime=datetime.utcnow() - timedelta(minutes=10),
        EndTime=datetime.utcnow(),
        Period=300,
        Statistics=["Average"]
    )
    return response