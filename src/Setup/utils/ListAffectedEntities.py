import boto3
import time
import json
import os
from datetime import datetime

# Get user inputs
DataCollectionAccountID = input("Enter DataCollection Account ID: ")
DataCollectionRegion = input("Enter DataCollection region: ")
ResourcePrefix = input("Enter ResourcePrefix, Hit enter to use default (heidi-): ") or "heidi-"
HeidiDataCollectionDB = input("Enter HeidiDataCollectionDB name, Hit enter to use default (datacollectiondb): ") or "datacollectiondb"
default_athena_bucket = f"aws-athena-query-results-{DataCollectionRegion}-{DataCollectionAccountID}"
AthenaResultBucket = input(f"Enter AthenaResultBucket, Hit enter to use default ({default_athena_bucket}): ") or default_athena_bucket
ResourceExplorerViewArn = input("Enter Resource Explorer view ARN: ")

# Checkpoint file path
CHECKPOINT_FILE = f"checkpoint_listentities_{DataCollectionAccountID}.json"


def save_checkpoint(processed_arns, next_token=None):
    """Save checkpoint to file"""
    checkpoint = {
        'processed_arns': list(processed_arns),
        'next_token': next_token,
        'timestamp': datetime.now().isoformat()
    }
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f)
        print(f"Checkpoint saved: {len(processed_arns)} ARNs processed")
    except Exception as e:
        print(f"Error saving checkpoint: {e}")

def load_checkpoint():
    """Load checkpoint from file"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                checkpoint = json.load(f)
            print(f"Checkpoint loaded: {len(checkpoint['processed_arns'])} ARNs already processed")
            return checkpoint
        except Exception as e:
            print(f"Error loading checkpoint: {e}")
            return None
    return None

def clear_checkpoint():
    """Remove checkpoint file after successful completion"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            os.remove(CHECKPOINT_FILE)
            print("Checkpoint file removed after successful completion")
        except Exception as e:
            print(f"Error removing checkpoint file: {e}")

def query_athena(query, database, output_location, region):
    athena_client = boto3.client('athena', region_name=region)
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}
    )
    
    query_execution_id = response['QueryExecutionId']
    print(f"Query execution started with ID: {query_execution_id}")
    
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        print(f"Query status: {status}. Waiting...")
        time.sleep(2)
    
    if status == 'SUCCEEDED':
        print("Query succeeded!")
        return athena_client.get_query_results(QueryExecutionId=query_execution_id)
    else:
        error_message = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        print(f"Query failed: {error_message}")
        return None


def query_resource_explorer_batch(arns, view_arn, processed_arns_set):
    region = view_arn.split(":")[3]
    resource_explorer = boto3.client('resource-explorer-2', region)
    
    arn_to_tags = {}
    arns_set = set(arns) - processed_arns_set  # Skip already processed ARNs
    
    print(f"\nQuerying Resource Explorer for {len(arns_set)} ARNs (skipping {len(processed_arns_set)} already processed)...")
    
    paginator = resource_explorer.get_paginator('list_resources')
    response_iterator = paginator.paginate(ViewArn=view_arn, PaginationConfig={'PageSize': 1000})
    
    resources_checked = 0
    pages_checked = 0
    
    for page in response_iterator:
        pages_checked += 1
        
        for resource in page.get('Resources', []):
            resources_checked += 1
            resource_arn = resource.get('Arn')
            
            if resource_arn in arns_set:
                tags = [{'entityKey': item['Key'], 'entityValue': item['Value']} 
                        for prop in resource.get('Properties', []) 
                        for item in prop.get('Data', [])]
                arn_to_tags[resource_arn] = tags
                print(f"Found: {resource_arn} ({len(tags)} tags)")
                arns_set.remove(resource_arn)
                processed_arns_set.add(resource_arn)
                
                # Stop if we found all ARNs
                if not arns_set:
                    break
        
        # Save checkpoint after each page
        save_checkpoint(processed_arns_set)
        
        # Stop going to next page if we found all ARNs
        if not arns_set:
            break
    
    print(f"Complete: {len(arn_to_tags)}/{len(arns)} found, {resources_checked} resources checked, {pages_checked} pages\n")
    return arn_to_tags


def send_tags_to_eventbridge(arn, tags):
    eventbridge_client = boto3.client('events', DataCollectionRegion)
    event_bus_arn = f"arn:aws:events:{DataCollectionRegion}:{DataCollectionAccountID}:event-bus/{ResourcePrefix}DataCollectionBus-{DataCollectionAccountID}"
    
    tag_data = {'entityArn': arn, 'tags': tags}
    return eventbridge_client.put_events(
        Entries=[{
            'Source': 'heidi.taginfo',
            'DetailType': 'Heidi tags from resource explorer',
            'Detail': json.dumps(tag_data),
            'EventBusName': event_bus_arn
        }]
    )


def list_affected_entities(database_name, output_location, region):
    query = f"""
    SELECT DISTINCT entities.entityValue AS affectedEntities
    FROM "AwsDataCatalog"."{database_name}"."awshealthevent"
    CROSS JOIN UNNEST(detail.affectedEntities) AS t(entities)
    WHERE entities.entityValue IS NOT NULL
    ORDER BY affectedEntities
    """
    
    print(f"Querying Athena database: {database_name}\n")
    results = query_athena(query, database_name, output_location, region)
    
    if not results:
        print("Failed to retrieve results from Athena.")
        return []
    
    rows = results['ResultSet']['Rows']
    if len(rows) <= 1:
        print("No affected entities found.")
        return []
    
    affected_arns = [row['Data'][0].get('VarCharValue') for row in rows[1:] if row['Data']]
    print(f"\nFound {len(affected_arns)} unique affected entities\n")
    return affected_arns


def main():
    database_name = f"{ResourcePrefix}{HeidiDataCollectionDB}"
    output_location = f"s3://{AthenaResultBucket}/"
    
    print(f"\nConfiguration:")
    print(f"  Account ID: {DataCollectionAccountID}")
    print(f"  Region: {DataCollectionRegion}")
    print(f"  Database: {database_name}")
    print(f"  Resource Explorer View ARN: {ResourceExplorerViewArn}\n")
    
    # Load checkpoint if exists
    checkpoint = load_checkpoint()
    processed_arns_set = set(checkpoint['processed_arns']) if checkpoint else set()
    
    # Get affected entities from Athena
    affected_arns = list_affected_entities(database_name, output_location, DataCollectionRegion)
    if not affected_arns:
        return
    
    # Query Resource Explorer for tags
    arn_to_tags = query_resource_explorer_batch(affected_arns, ResourceExplorerViewArn, processed_arns_set)
    
    # Send tags to EventBridge
    if arn_to_tags:
        print("Sending tags to EventBridge...")
        events_sent = 0
        for arn, tags in arn_to_tags.items():
            if tags:
                send_tags_to_eventbridge(arn, tags)
                events_sent += 1
                print(f"Sent: {arn} ({len(tags)} tags)")
        print(f"\nTotal events sent: {events_sent}/{len(arn_to_tags)}")
    else:
        print("No tags found for affected entities.")
    
    # Clear checkpoint after successful completion
    clear_checkpoint()


if __name__ == "__main__":
    main()
