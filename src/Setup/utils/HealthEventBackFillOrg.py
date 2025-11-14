import boto3
import json
import logging
import os
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DataCollectionAccountID = input("Enter DataCollection Account ID: ")
DataCollectionRegion = input("Enter DataCollection region: ")
ResourcePrefix = input("Enter ResourcePrefix, Hit enter to use default (heidi-): ") or "heidi-"

# Checkpoint file path
CHECKPOINT_FILE = f"checkpoint_{DataCollectionAccountID}.json"

health_client = boto3.client('health', 'us-east-1')
eventbridge_client = boto3.client('events', DataCollectionRegion)
EventBusArnVal = f"arn:aws:events:{DataCollectionRegion}:{DataCollectionAccountID}:event-bus/{ResourcePrefix}DataCollectionBus-{DataCollectionAccountID}"

def save_checkpoint(next_token=None, processed_events=0):
    """Save checkpoint to file"""
    checkpoint = {
        'next_token': next_token,
        'processed_events': processed_events,
        'timestamp': datetime.now().isoformat()
    }
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f)
        logger.info(f"Checkpoint saved: {processed_events} events processed, next_token: {next_token[:20] if next_token else 'None'}...")
    except Exception as e:
        logger.error(f"Error saving checkpoint: {e}")

def load_checkpoint():
    """Load checkpoint from file"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                checkpoint = json.load(f)
            logger.info(f"Checkpoint loaded: {checkpoint['processed_events']} events already processed")
            return checkpoint
        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}")
            return None
    return None

def clear_checkpoint():
    """Remove checkpoint file after successful completion"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            os.remove(CHECKPOINT_FILE)
            logger.info("Checkpoint file removed after successful completion")
        except Exception as e:
            logger.error(f"Error removing checkpoint file: {e}")

def get_organization_events_page(next_token=None):
    """Retrieve one page of organization health events"""
    try:
        kwargs = {'maxResults': 100}
        if next_token and len(next_token) >= 4:
            kwargs['nextToken'] = next_token
        events_response = health_client.describe_events_for_organization(filter={}, **kwargs)
        events = events_response.get('events', [])
        new_next_token = events_response.get('nextToken')
        logger.info(f"Retrieved {len(events)} events in this page")
        return events, new_next_token
    except Exception as e:
        logger.error(f"Error getting organization events: {e}")
        return [], None

def describe_health_events_details_for_organization(item, account_id):
    """Get event details for a specific account in the organization"""
    try:
        response = health_client.describe_event_details_for_organization(
            organizationEventDetailFilters=[{
                'eventArn': item['arn'],
                'awsAccountId': account_id
            }]
        )
        return response
    except Exception as e:
        logger.error(f"Error getting event details: {e}")
        return {}

def describe_affected_accounts(item):
    """Get all affected accounts for an organization event"""
    try:
        affected_accounts = []
        next_token = None
        
        while True:
            params = {
                'eventArn': item['arn'],
                'maxResults': 100
            }
            if next_token:
                params['nextToken'] = next_token
            
            response = health_client.describe_affected_accounts_for_organization(**params)
            affected_accounts.extend(response.get('affectedAccounts', []))
            
            next_token = response.get('nextToken')
            if not next_token:
                break
        
        return affected_accounts
    except Exception as e:
        logger.error(f"Error getting affected accounts: {e}")
        return []

def describe_affected_entities(item, account_id):
    """Get all affected entities for a specific account"""
    try:
        entities = []
        next_token = None
        
        while True:
            params = {
                'maxResults': 100,
                'organizationEntityAccountFilters': [{
                    'eventArn': item['arn'],
                    'awsAccountId': account_id,
                    'statusCodes': ['IMPAIRED', 'UNIMPAIRED', 'UNKNOWN', 'PENDING']
                }]
            }
            if next_token:
                params['nextToken'] = next_token
            
            response = health_client.describe_affected_entities_for_organization(**params)
            entities.extend(response.get('entities', []))
            
            next_token = response.get('nextToken')
            if not next_token:
                break
        
        return entities
    except Exception as e:
        logger.error(f"Error getting affected entities: {e}")
        return []

def get_event_data(event_details, event_description, event_metadata, affected_entities, account_id=None):
    """Format event data for EventBridge"""
    event_data = {
        'eventArn': event_details['arn'],
        'eventRegion': event_details.get('region', ''),
        'eventTypeCode': event_details.get('eventTypeCode', ''),
        'startTime': event_details.get('startTime').strftime('%a, %d %b %Y %H:%M:%S GMT'),
        'eventDescription': [{'latestDescription': event_description.get('latestDescription', '')}],
        'eventMetadata': event_metadata
    }
    
    # Only add affectedAccount field if account_id is provided
    if account_id:
        event_data['affectedAccount'] = account_id
    
    # Only add affectedEntities field if there are affected entities
    if affected_entities:
        event_data['affectedEntities'] = affected_entities
    
    # Add optional time fields
    if 'endTime' in event_details:
        event_data['endTime'] = event_details['endTime'].strftime('%a, %d %b %Y %H:%M:%S GMT')
    
    if 'lastUpdatedTime' in event_details:
        event_data['lastUpdatedTime'] = event_details['lastUpdatedTime'].strftime('%a, %d %b %Y %H:%M:%S GMT')
    
    # Add any additional fields from event_details
    event_data.update((key, value) for key, value in event_details.items() if key not in event_data)
    
    logger.info(f"Processed event {event_details['arn']} for account {account_id}")
    return event_data

def send_event_to_eventbridge(event_data, EventBusArn):
    """Send the event to EventBridge"""
    try:
        eventbridge_client.put_events(
            Entries=[{
                'Source': 'heidi.health',
                'DetailType': 'awshealthtest',
                'Detail': json.dumps(event_data, default=str),
                'EventBusName': EventBusArn
            }]
        )
        logger.info(f"Sent event to EventBridge: {event_data['eventArn']}")
    except Exception as e:
        logger.error(f"Error sending event to EventBridge: {e}")

def backfill():
    """Main backfill function for organization health events"""
    EventBusArn = EventBusArnVal
    
    total_events_processed = 0
    
    # Load checkpoint if exists
    checkpoint = load_checkpoint()
    next_token = checkpoint.get('next_token') if checkpoint else None
    total_events_processed = checkpoint.get('processed_events', 0) if checkpoint else 0
    
    if checkpoint:
        logger.info(f"Resuming from checkpoint with next_token")
    
    # Process events page by page
    while True:
        events, new_next_token = get_organization_events_page(next_token)
        
        if not events:
            logger.info("No more events to process")
            break
        
        # Process all events in this page
        for awsevent in events:
            try:
                # Get all affected accounts for this event
                affected_accounts = describe_affected_accounts(awsevent)
                
                if not affected_accounts:
                    logger.warning(f"No affected accounts found for event {awsevent['arn']}, processing without account")
                    # Process event without account information
                    try:
                        # Get event details without account filter
                        event_details_response = health_client.describe_event_details_for_organization(
                            organizationEventDetailFilters=[{'eventArn': awsevent['arn']}]
                        )
                        
                        successful_set = event_details_response.get('successfulSet', [])
                        if successful_set:
                            event_details = successful_set[0].get('event', {})
                            if event_details:
                                event_description = successful_set[0].get('eventDescription', {})
                                event_metadata = successful_set[0].get('eventMetadata', {})
                                
                                # Prepare and send event data without account
                                event_data = get_event_data(
                                    event_details, 
                                    event_description, 
                                    event_metadata, 
                                    [],
                                    None
                                )
                                send_event_to_eventbridge(event_data, EventBusArn)
                                total_events_processed += 1
                    except Exception as e:
                        logger.error(f"Error processing event without account {awsevent['arn']}: {e}")
                    continue
                
                # Process each affected account
                for account_id in affected_accounts:
                    try:
                        # Get event details for this account
                        event_details_response = describe_health_events_details_for_organization(awsevent, account_id)
                        
                        successful_set = event_details_response.get('successfulSet', [])
                        if not successful_set:
                            logger.warning(f"No successful set for event {awsevent['arn']} and account {account_id}")
                            continue
                        
                        event_details = successful_set[0].get('event', {})
                        if not event_details:
                            continue
                        
                        event_description = successful_set[0].get('eventDescription', {})
                        event_metadata = successful_set[0].get('eventMetadata', {})
                        
                        # Get affected entities for this account
                        entities = describe_affected_entities(awsevent, account_id)
                        affected_entities = []
                        for entity in entities:
                            entity_value = entity.get('entityValue', 'UNKNOWN')
                            status_code = entity.get('statusCode', 'UNKNOWN')
                            affected_entities.append({'entityValue': entity_value, 'status': status_code})
                        
                        # Prepare and send event data
                        event_data = get_event_data(
                            event_details, 
                            event_description, 
                            event_metadata, 
                            affected_entities,
                            account_id
                        )
                        send_event_to_eventbridge(event_data, EventBusArn)
                        total_events_processed += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing account {account_id} for event {awsevent['arn']}: {e}")
                        continue
            
            except Exception as e:
                logger.error(f"Error processing event {awsevent.get('arn', 'UNKNOWN')}: {e}")
                continue
        
        # Save checkpoint after processing this page
        save_checkpoint(new_next_token, total_events_processed)
        
        # Move to next page
        if not new_next_token:
            logger.info("No more pages to process")
            break
        
        next_token = new_next_token
    
    logger.info(f"Backfill completed. Total events processed: {total_events_processed}")
    # Clear checkpoint after successful completion
    clear_checkpoint()

if __name__ == "__main__":
    backfill()
