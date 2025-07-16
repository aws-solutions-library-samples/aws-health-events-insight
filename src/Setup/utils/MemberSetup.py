import boto3
import subprocess
from botocore.exceptions import ClientError
import os

# Get Tag
def tag():
    """Get multiple tag key-value pairs from user input"""
    tags = {}
    
    # Get the first tag with defaults
    tag_key = input("Enter the first tag key (Hit enter to use default: 'App'): ") or "App"
    tag_value = input(f"Enter the value for '{tag_key}' (Hit enter to use default: 'Heidi'): ") or "Heidi"
    tags[tag_key] = tag_value
    
    # Allow user to add more tags
    while True:
        add_more = input("Do you want to add another tag? (yes/no): ").lower().strip()
        if add_more in ['yes', 'y']:
            tag_key = input("Enter tag key: ").strip()
            if tag_key:
                tag_value = input(f"Enter value for '{tag_key}': ").strip()
                tags[tag_key] = tag_value
            else:
                print("Tag key cannot be empty. Skipping...")
        elif add_more in ['no', 'n']:
            break
        else:
            print("Invalid input. Please enter 'yes' or 'no'.")
    
    return tags

# Initialize tags_dict - will be populated in setup()
tags_dict = {}

# Get Current Account ID
def get_account_id():
    # Get the AWS account ID for Unique names
    sts_client = boto3.client("sts")
    account_id = sts_client.get_caller_identity().get("Account")
    return account_id

# Get yes or no for modules
def ask_yes_no(prompt):
    while True:
        user_input = input(f"{prompt} (yes/no): ").lower()
        if user_input == 'yes':
            return True
        elif user_input == 'no':
            return False
        else:
            print("Invalid input. Please enter 'yes' or 'no'.")

# Print pretty Box
def print_boxed_text(text):
    lines = text.strip().split('\n')
    max_length = max(len(line) for line in lines)
    
    print('═' * (max_length + 2))
    for line in lines:
        print(f' {line.ljust(max_length)} ')
    print('═' * (max_length + 2))

# deploy stack
def deploy_stack(command):
    try:
        subprocess.call(command, shell=True)
    except Exception as e:
        print("An error occurred:", e)

# User Input Data
def get_user_input():
    DeploymentRegionHealth = input("Enter comma-separated Region names for AWS health data collection: ")
    print_boxed_text("Data Collection Account Parameters")
    DataCollectionAccountID = input(f"Enter Data Collection Account ID, Default {get_account_id()}: ") or get_account_id()
    DataCollectionRegion = input("Enter Data Collection Region ID: ")
    ResourcePrefix = input("Enter ResourcePrefix, Hit enter to use default (heidi-): ") or "heidi-"
    return (
        DataCollectionAccountID, DataCollectionRegion, DeploymentRegionHealth, ResourcePrefix
    )

# setup
def setup():
    # Get tag information from user
    global tags_dict
    tags_dict = tag()
    
    parameters_dict = {}
    DataCollectionAccountID, DataCollectionRegion, DeploymentRegionHealth, ResourcePrefix = get_user_input()

    parameters_dict['DataCollectionAccountID'] = DataCollectionAccountID
    parameters_dict['DataCollectionRegion'] = DataCollectionRegion
    parameters_dict['ResourcePrefix'] = ResourcePrefix

    parameters = f"DataCollectionAccountID={parameters_dict['DataCollectionAccountID']} \
                DataCollectionRegion={parameters_dict['DataCollectionRegion']} \
                ResourcePrefix={parameters_dict['ResourcePrefix']}"

    # Format tags for deployment
    tags = " ".join([f"{key}={value}" for key, value in tags_dict.items()])

    for region in DeploymentRegionHealth.split(','):
        stack_name = f"{parameters_dict['ResourcePrefix']}HealthModule-member-{get_account_id()}-{region}"
        command = f"sam deploy --stack-name {stack_name} --region {region} --parameter-overrides {parameters} \
            --template-file ../HealthModule/HealthModuleCollectionSetup.yaml --tags {tags} --capabilities CAPABILITY_NAMED_IAM --disable-rollback"
        # Deploy Stack
        deploy_stack(command)

if __name__ == "__main__":
    setup()
