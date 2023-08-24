
import boto3

def calculate_directory_size(s3_client, bucket_name, prefix=''):
    total_size = 0
    
    params = {
        'Bucket': bucket_name,
        'Prefix': prefix,
        'Delimiter': '/'
    }
    
    response = s3_client.list_objects_v2(**params)
    
    for obj in response.get('Contents', []):
        total_size += obj['Size']
        
    for common_prefix in response.get('CommonPrefixes', []):
        subdirectory = common_prefix['Prefix']
        subdirectory_size = calculate_directory_size(s3_client, bucket_name, subdirectory)
        total_size += subdirectory_size
    
    return total_size

def lambda_handler(event, context):
    bucket_name = 'chander-testing'
    s3_client = boto3.client('s3')
    
    # Fetch all objects at the root path
    root_objects_response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='')
    
    # Create an HTML table with styling
    html_table = "<table style='border-collapse: collapse; width: 100%; border: 2px solid black;'>"
    html_table += "<tr style='text-align: center;'><th style='border: 2px solid black;'>Path</th><th style='border: 2px solid black;'>Size</th></tr>"
    
    for obj in root_objects_response.get('Contents', []):
        object_key = obj['Key']
        object_size = obj['Size']
        formatted_size = convert_bytes_to_readable(object_size)
        # Add row to the HTML table if key doesn't contain / to make sure only files at root path are listed.
        if(object_key.find('/')==-1):
            print(object_key,object_size)
            html_table += f"<tr><td style='border: 2px solid black;'>{object_key}</td><td style='border: 2px solid black;'>{formatted_size}</td></tr>"
    
    # Calculate and display size for subdirectories
    response = s3_client.list_objects_v2(Bucket=bucket_name, Delimiter='/', Prefix='')
    
    for common_prefix in response.get('CommonPrefixes', []):
        subdirectory = common_prefix['Prefix']
        subdirectory_size = calculate_directory_size(s3_client, bucket_name, subdirectory)
        formatted_size = convert_bytes_to_readable(subdirectory_size)
        # Add row to the HTML table
        html_table += f"<tr><td style='border: 2px solid black;'>{subdirectory}</td><td style='border: 2px solid black;'>{formatted_size}</td></tr>"
    
    # Close the HTML table
    html_table += "</table>"
    
    # Combine and format the results
    result_html = f"<html><body>{html_table}</body></html>"
    
    return result_html
    
def convert_bytes_to_readable(size_in_bytes):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_in_bytes < 1024.0:
            return f"{size_in_bytes:.3f} {unit}"
        size_in_bytes /= 1024.0
    return f"{size_in_bytes:.3f} TB"
