import boto3
import subprocess
import datetime
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

# Call the tag function to get the tags dictionary
tags_dict = tag()


#Get Current Region
def get_default_region():
    # Get the default AWS region from the current session
    session = boto3.Session()
    region = input(f"Enter datacollection region (Hit enter to use default: {session.region_name}): ") or session.region_name
    MemberRegionHealth = input(f"Enter member regions where you wish to receive events with comma seperated: ")
    return region, MemberRegionHealth

#Get Current Account ID
def get_account_id():
    # Get the AWS account ID for Unique names
    sts_client = boto3.client("sts")
    account_id = sts_client.get_caller_identity().get("Account")
    return account_id

#Get current AWS Organization ID
def get_organization_details():
    # Get the ID of the AWS organization for event bus
    AdditionalOrgs = ''
    org_client = boto3.client('organizations')
    OrgID = org_client.describe_organization()['Organization']['Id']
    AdditionalOrgsRequired = ask_yes_no(f"You will get events from OrganizationId {OrgID}. Do you want to add additional Payers/Organization Ids")
    if AdditionalOrgsRequired:
        AdditionalOrgs = input(f"Enter organizations IDs with comma seperated: ")
        POrgID = f"{OrgID},{AdditionalOrgs}"
    else:
        POrgID = OrgID
    print(f"OrgID included {POrgID}")
    return POrgID

#Create or update user with bucket KMS
def create_or_get_s3_bucket(account_id, region):
    #create bucket or upload file if bucket is supplied by user
    bucket_name = input(f"Enter S3 bucket name for Primary Region (Hit enter to use default: awseventhealth-{account_id}-{region}): ") or f"awseventhealth-{account_id}-{region}"
    try:
        s3_client = boto3.client('s3', region_name=region)
        s3_client.head_bucket(Bucket=bucket_name)
        response = s3_client.get_bucket_encryption(Bucket=bucket_name)
        encryption = response['ServerSideEncryptionConfiguration']['Rules'][0]['ApplyServerSideEncryptionByDefault']['SSEAlgorithm']
        if encryption != "AES256":
            try:
                bucketkmsarn = response['ServerSideEncryptionConfiguration']['Rules'][0]['ApplyServerSideEncryptionByDefault']['KMSMasterKeyID']
                print(f"Skip Creating: S3 bucket {bucket_name} exists and encrypted with kms {bucketkmsarn}")
            except KeyError:
                bucketkmsarn = input(f"Enter kms Key Arn for {bucket_name}: ")
        else:
            bucketkmsarn = "na"
            print(f"Skip Creating: S3 bucket {bucket_name} already exists")
    except ClientError as e:
        if region == 'us-east-1':
            s3_client.create_bucket(Bucket=bucket_name)
            bucketkmsarn = "na"
        else:
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
            bucketkmsarn = "na"
        s3_client.get_waiter("bucket_exists").wait(Bucket=bucket_name)
        print(f"S3 bucket {bucket_name} has been created")

        # Add tags to the newly created bucket
        tag_set = [{'Key': key, 'Value': value} for key, value in tags_dict.items()]
        tagging = {'TagSet': tag_set}
        
        s3_client.put_bucket_tagging(Bucket=bucket_name, Tagging=tagging)
        print(f"Tags added to bucket {bucket_name}: {tags_dict}")
        
    return bucket_name, bucketkmsarn

#Upload CFN and Metadatafiles
def sync_cfnfiles(bucket_name):
    #Sync cloudformation and metadata files
    try:
        aws_sync_command = f"aws s3 sync ../../src/ s3://{bucket_name}/DataCollection-metadata"
        subprocess.call(aws_sync_command.split())
    except ClientError as e:
        print("Error while syncing S3. Check if deployer role has required S3 and KMS permissions.")
        exit()

#Get QuickSight Author User
def get_quicksight_user(account_id, qsregion):
    #get quicksight user. ES user can have multiplenamespaces
    try:
        quicksight_client = boto3.client('quicksight', region_name=qsregion)
        response = quicksight_client.list_namespaces(AwsAccountId=account_id)
        namespaces = [namespace['Name'] for namespace in response['Namespaces']]
        qsusernames = []
    except:
        print("Error while listing namespaces. Check if QuickSight is an enterprise plan.")
        exit()
    try:
        for namespace in namespaces:
            response = quicksight_client.list_users(AwsAccountId=account_id, Namespace=namespace, MaxResults=100)
            qsusernames.extend([user['Arn'] for user in response['UserList']])
    except ClientError as q:
        print("Wrong QuickSight Identity region.")
        print(q)
        exit()
    print("\nAvailable QuickSight Users")
    try:
        for i, qsusername in enumerate(qsusernames, 1):
            print("{}. {}".format(i, qsusername))
        print()
        while True:
            quicksight_number = input("Enter the number corresponding to the QuickSight username from the list: ")
            quicksight_user = qsusernames[int(quicksight_number) - 1] if quicksight_number.isdigit() and 1 <= int(quicksight_number) <= len(qsusernames) else None
            if quicksight_user:
                return quicksight_user
            else: 
                print("Invalid Option") 
    except ClientError as q:
        print("Something went wrong, Check Quicksight settings")

#Get yes or no for modules
def ask_yes_no(prompt):
    while True:
        user_input = input(f"{prompt} (yes/no): ").lower()
        if user_input == 'yes':
            return True
        elif user_input == 'no':
            return False
        else:
            print("Invalid input. Please enter 'yes' or 'no'.")
#Print pretty Box
def print_boxed_text(text):
    lines = text.strip().split('\n')
    max_length = max(len(line) for line in lines)
    
    print('═' * (max_length + 2))
    for line in lines:
        print(f' {line.ljust(max_length)} ')
    print('═' * (max_length + 2))

def SendMockEvent():
    try:
        aws_account_id = get_account_id()
        current_time = datetime.datetime.now()
        support_client = boto3.client('support','us-east-1')
        event_start_time = (current_time + datetime.timedelta(hours=12)).strftime('%Y-%m-%d %H:%M UTC')
        event_end_time = (current_time + datetime.timedelta(hours=24)).strftime('%Y-%m-%d %H:%M UTC')
        communication_body = f"Hello \nCan you please send a mock PHD event to this account? If eventStart time is passed, please pick any random start time, its just a test.\nAccountId: {aws_account_id}\nEvent Region: us-east-1\nEvent Start Time: {event_start_time}\nEvent End Time: {event_end_time}\nEvent Category: EC2 Service Event"
        
        support_client.create_case(
            subject=f"Heidi mock event request for {aws_account_id}",
            serviceCode="aws-health",
            severityCode="low",
            categoryCode="general-guidance",
            communicationBody=communication_body)
    except Exception as e:
        print(e)

#Save Input to file for future use
def save_output_to_file(output):
    with open('utils/ParametersDataCollection.txt', 'w') as file:
        file.write(output + '\n')

#deploy stack
def deploy_stack(command):
    try:
        subprocess.call(command, shell=True)
    except Exception as e:
        print("An error occurred:", e)

#User Input Data
def get_user_input():
    SlackChannelId = "na"
    SlackWorkspaceId = "na"
    TeamId = "na"
    TeamsTenantId = "na"
    TeamsChannelId = "na"
    qsregion ="na"
    QuickSightAnalysisAuthor = "na"
    AthenaResultBucket = "na"
    AthenaBucketKmsArn ="na"

    region, MemberRegionHealth = get_default_region()
    account_id = get_account_id()
    AWSOrganizationID = get_organization_details()
    DataCollectionBucket, DataCollectionBucketKmsArn = create_or_get_s3_bucket(account_id, region)

    ResourcePrefix = input("Enter ResourcePrefix (Must be in lowercase), Hit enter to use default (heidi-): ") or "heidi-"
    ResourcePrefix = ResourcePrefix.lower()
    print_boxed_text("Module Selection")
    EnableHealthModule = ask_yes_no("Do you want to enable the AWS Health Module(HEIDI)?")
    if EnableHealthModule:
        qsregion = input(f"Enter QuickSight Identity Region, Hit enter to use default {region}: ") or region
        QuickSightAnalysisAuthor = get_quicksight_user(account_id, qsregion)
        AthenaResultBucket = input("Enter AthenaResultBucket, Hit enter to use default (aws-athena-query-results-*): ") or "aws-athena-query-results-*"
        AthenaBucketKmsArn = input("Enter AthenaBucketKmsArn, Hit enter to use default (na): ") or "na"
    print()
    EnableNotificationModule = ask_yes_no("Do you want to enable the Notification Module?")
    if EnableNotificationModule:
        print_boxed_text("Notification Module can setup notification for MS Teams and/Or slack")
        EnableSlack = ask_yes_no("Do you want to enable the Notification for Slack Channel?")
        if EnableSlack:
            SlackChannelId = input("    Provide Slack Channel Id (Note: Slack Channel must be private and you must invite aws@ to the channel): ") or "na"
            SlackWorkspaceId = input("    Provide Workspace ID: ") or "na"
        EnableTeams = ask_yes_no("Do you want to enable the Notification for Teams Channel?")
        if EnableTeams:
            TeamId = input("    Provide TeamId: ") or "na"
            TeamsTenantId = input("    Provide TeamsTenantId: ") or "na"
            TeamsChannelId = input("    Provide TeamsChannelId: ") or "na"
    print()
    TestSetupViaSupportCase = ask_yes_no("Do you want to test end to end setup with Mock Health Event(via support case)?")
    if TestSetupViaSupportCase:
        SendMockEvent()

    return (
        "yes" if EnableHealthModule else "no",
        "yes" if EnableNotificationModule else "no",
        region, account_id, AWSOrganizationID,
        DataCollectionBucket, DataCollectionBucketKmsArn, QuickSightAnalysisAuthor,
        AthenaResultBucket, AthenaBucketKmsArn,ResourcePrefix, 
        SlackChannelId, SlackWorkspaceId, TeamId, TeamsTenantId, TeamsChannelId, qsregion, #qs region not required in parameter
        MemberRegionHealth
    )

def save_variables_to_file(variables): #last variable is variables[20], increment from here and also update this comment.
    output = "\n".join([
        f"#Deploy AWS Health Events Intelligence Dashboards and Insights (HEIDI)\nEnableHealthModule: {variables[0]}\n",
        f"#Deploy Notification module\nEnableNotificationModule: {variables[1]}\n",
        f"#Data Collection Region\nDataCollectionRegion: {variables[2]}\n",
        f"#Data Collection Account\nDataCollectionAccountID: {variables[3]}\n",
        f"#Member Regions \nMemberRegionHealth: {variables[17]}\n",
        f"#AWS Organization ID which can send events to Data Collection Account\nAWSOrganizationID: {variables[4]}\n",
        f"#Bucket which would collection data from various members\nDataCollectionBucket: {variables[5]}\n",
        f"#Update here if Collection bucket is encrypted with KMS otherwise na\nDataCollectionBucketKmsArn: {variables[6]}\n",
        f"#QuickSight Analysis Author\nQuickSightAnalysisAuthor: {variables[7]}\n",
        f"#Update here if Athena result bucket is not default\nAthenaResultBucket: {variables[8]}\n",
        f"#Update here Athena bucket is encrypted with KMS otherwise na\nAthenaBucketKmsArn: {variables[9]}\n",
        f"#Resource prefix, DO NOT CHANGE\nResourcePrefix: {variables[10]}\n",
        f"#If EnableNotificationModule, Provide SlackChannelId for slack\nSlackChannelId: {variables[11]}\n",
        f"#If EnableNotificationModule, Provide SlackWorkspaceId for slack\nSlackWorkspaceId: {variables[12]}\n",
        f"#If EnableNotificationModule, Provide TeamId for MS Teams\nTeamId: {variables[13]}\n",
        f"#If EnableNotificationModule, Provide TeamsTenantId for MS Teams\nTeamsTenantId: {variables[14]}\n",
        f"#If EnableNotificationModule, Provide TeamsChannelId for MS Teams\nTeamsChannelId: {variables[15]}\n"
    ])
    save_output_to_file(output)


def read_parameters(file_path):
    # Define a dictionary to store the parameters
    parameters = {}

    # Read the file and extract parameters
    with open(file_path, 'r') as file:
        for line in file:
            # Skip comments and empty lines
            if line.startswith('#') or not line.strip():
                continue
            
            # Split each line into key and value
            key, value = map(str.strip, line.split(':', 1))
            
            # Store in the dictionary
            parameters[key] = value

    # Access the variables
    enable_health_module = parameters.get('EnableHealthModule', '')
    enable_notification_module = parameters.get('EnableNotificationModule', '')
    data_collection_region = parameters.get('DataCollectionRegion', '')
    data_collection_account_id = parameters.get('DataCollectionAccountID', '')
    MemberRegionHealth = parameters.get('MemberRegionHealth','')
    aws_organization_id = parameters.get('AWSOrganizationID', '')
    quicksight_analysis_author = parameters.get('QuickSightAnalysisAuthor', '')
    data_collection_bucket = parameters.get('DataCollectionBucket', '')
    data_collection_bucket_kms_arn = parameters.get('DataCollectionBucketKmsArn', 'na')
    athena_result_bucket = parameters.get('AthenaResultBucket', 'aws-athena-query-results-*')
    athena_bucket_kms_arn = parameters.get('AthenaBucketKmsArn', 'na')
    resource_prefix = parameters.get('ResourcePrefix', '')
    slack_channel_id = parameters.get('SlackChannelId', '')
    Slack_Workspace_Id = parameters.get('SlackWorkspaceId', '')
    team_id = parameters.get('TeamId', '')
    Teams_Tenant_Id = parameters.get('TeamsTenantId', '')
    Teams_Channel_Id = parameters.get('TeamsChannelId', '')

    # Return the variables as a dictionary
    return {
        'EnableHealthModule': enable_health_module,
        'EnableNotificationModule': enable_notification_module,
        'DataCollectionRegion': data_collection_region,
        'DataCollectionAccountID': data_collection_account_id,
        'MemberRegionHealth': MemberRegionHealth,
        'AWSOrganizationID': aws_organization_id,
        'QuickSightAnalysisAuthor': quicksight_analysis_author,
        'DataCollectionBucket': data_collection_bucket,
        'DataCollectionBucketKmsArn': data_collection_bucket_kms_arn,
        'AthenaResultBucket': athena_result_bucket,
        'AthenaBucketKmsArn': athena_bucket_kms_arn,
        'ResourcePrefix': resource_prefix,
        'SlackChannelId': slack_channel_id,
        'SlackWorkspaceId': Slack_Workspace_Id,
        'TeamId': team_id,
        'TeamsTenantId': Teams_Tenant_Id,
        'TeamsChannelId': Teams_Channel_Id
    }

def setup():
    file_path = 'utils/ParametersDataCollection.txt'

    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            print(f"./ParametersDataCollection.txt found with previously saved parameters")
            print_boxed_text(f"{file.read()}")
        reinput = ask_yes_no("Do you want to re-input parameters?")
        if reinput:
            variables = get_user_input()
            save_variables_to_file(variables)
        else:
            print("Skipping re-input. Using existing variables.")
    else:
        variables = get_user_input()
        save_variables_to_file(variables)
        print_boxed_text(f"\nDeployment will use these parameters. Update ./utils/ParametersDataCollection.txt file for additional changes")
        with open(file_path, 'r') as file:
            print(f"{file.read()}")

    #Read dictionary for parameeters
    parameters_dict = read_parameters('utils/ParametersDataCollection.txt')
    #sync cfn template files
    sync_cfnfiles(parameters_dict['DataCollectionBucket'])

    # Create or update the CloudFormation stack
    stack_name = f"{parameters_dict['ResourcePrefix']}{parameters_dict['DataCollectionAccountID']}-{parameters_dict['DataCollectionRegion']}"

    parameters = f"AWSOrganizationID={parameters_dict['AWSOrganizationID']} " \
                f"DataCollectionBucket={parameters_dict['DataCollectionBucket']} " \
                f"DataCollectionBucketKmsArn={parameters_dict['DataCollectionBucketKmsArn']} " \
                f"AthenaBucketKmsArn={parameters_dict['AthenaBucketKmsArn']} " \
                f"QuickSightAnalysisAuthor={parameters_dict['QuickSightAnalysisAuthor']} " \
                f"ResourcePrefix={parameters_dict['ResourcePrefix']} " \
                f"SlackChannelId={parameters_dict['SlackChannelId']} " \
                f"SlackWorkspaceId={parameters_dict['SlackWorkspaceId']} " \
                f"TeamId={parameters_dict['TeamId']} " \
                f"TeamsTenantId={parameters_dict['TeamsTenantId']} " \
                f"TeamsChannelId={parameters_dict['TeamsChannelId']} " \
                f"EnableHealthModule={parameters_dict['EnableHealthModule']} " \
                f"EnableNotificationModule={parameters_dict['EnableNotificationModule']} " 
    
    #Update tags here
    tags = " ".join([f"{key}={value}" for key, value in tags_dict.items()])
                
    command= f"sam deploy --stack-name {stack_name} --region {parameters_dict['DataCollectionRegion']} --parameter-overrides {parameters}\
        --template-file ../DataCollectionModule/HeidiRoot.yaml --tags {tags} --capabilities CAPABILITY_NAMED_IAM --disable-rollback"
    
    #Deploy Stack
    deploy_stack(command)

    memberparameters = f"DataCollectionAccountID={parameters_dict['DataCollectionAccountID']} " \
                f"DataCollectionRegion={parameters_dict['DataCollectionRegion']} " \
                f"ResourcePrefix={parameters_dict['ResourcePrefix']} "

    for memberregion in parameters_dict['MemberRegionHealth'].split(','):
        # Check if the memberregion is not empty or just whitespace
        if memberregion.strip():  # This ensures that empty strings or whitespace regions are ignored
            Member_stack_name = f"{parameters_dict['ResourcePrefix']}HealthModule-{get_account_id()}-{memberregion}"
            Membercommand = f"sam deploy --stack-name {Member_stack_name} --region {memberregion} --parameter-overrides {memberparameters} \
                    --template-file ../HealthModule/HealthModuleCollectionSetup.yaml --tags {tags} --capabilities CAPABILITY_NAMED_IAM --disable-rollback"
            
            # Deploy the stack
            deploy_stack(Membercommand)
        else:
            print(f"Skipping member Region deployment, no member Region supplied.")

if __name__ == "__main__":
    setup()


