from __future__ import print_function
import boto3
from botocore.exceptions import ClientError
import json

# Setup clients for EC2, SQS, and CloudWatch Events
ec2 = boto3.client('ec2')
sqs = boto3.client('sqs')
events = boto3.client('events')

# Main function
def lambda_handler(event, context):

	# CONFIGURATION REQUIRED FOR THE TWO VARIABLES BELOW
	# You need an SQS queue and a CloudWatch event rule
	# For the start Queue you need to provide the SQS URL as a string
	# For the start rule you need to provide the CloudWarch event rule name as a string
	startQueueUrl = 'YOUR_START_SQS_QUEUE_URL'
	startRule = 'YOUR_START_CLOUDWATCH_EVENT_RULE_NAME'

	# Poll the stop queue for messages. Long polling is used by setting 'WaitTimeSeconds' to '5'
	# so all SQS servers hosting the queue are polled.
	sqsResponse = sqs.receive_message(
		QueueUrl=startQueueUrl,
		AttributeNames=[
			'SentTimestamp'
		],
		MaxNumberOfMessages=1,
		MessageAttributeNames=[
			'All'
		],
		VisibilityTimeout=0,
		WaitTimeSeconds=5
	)

	# If no messages were found 'Messages' will not be in 'sqsResponse'. If that is the case the script ends.
	# If a message was found the attribute from the message (InstanceId) is set equal to 'instanceID'
	# 'receiptHandle' is also set equal to the messages receipt handle so the message can be deleted from
	# the queue if necessary.
	if 'Messages' in sqsResponse:
		messageAttributes = sqsResponse['Messages'][0]['MessageAttributes']
		receiptHandle = sqsResponse['Messages'][0]['ReceiptHandle']
		print('Received message from the start queue. Receipt handle: %s' % receiptHandle)
		
		instanceID = messageAttributes['InstanceId']['StringValue']
	else:
		print('No messages found in the start queue. Stopping function.')
		
		return {
		'statusCode': 200,
		'body': json.dumps('Hello from Lambda!')
	}

	# The following two lines get the state of the instance
	instanceState = ec2.describe_instances(InstanceIds=[instanceID])
	instanceState = instanceState['Reservations'][0]['Instances'][0]['State']['Name']

	# Check to see if the instance state is 'stopped', if it an attempt to start the instance is made. If
	# it is anything other than 'stopped' the SQS message is returned to the queue and the script ends.
	if instanceState.lower() == 'stopped':
		print('The instance is stopped, attempting to start instance %s' % instanceID)

		# Try stopping the specified instance as a DryRun first to verify permissions
		try:
			ec2.start_instances(InstanceIds=[instanceID], DryRun=True)
		except ClientError as e:
			if 'DryRunOperation' not in str(e):
				print("You do not have permission to start instances")
				raise

		# Permissions have been verified. Try stopping the specified instance
		try:
			ec2Response = ec2.start_instances(InstanceIds=[instanceID], DryRun=False)
			print('Instance %s has been started' % instanceID)

			# The instance has been stopped, delete the SQS message
			sqs.delete_message(
				QueueUrl = startQueueUrl,
				ReceiptHandle=receiptHandle
			)
			print('Deleted message: %s' % receiptHandle)

			# Disable the start CloudWatch event rule
			disableRuleResponse = events.disable_rule(Name=startRule)
			print('%s has been disabled' % startRule)

		except ClientError as e:
			print('Error', e)
	else:
		print('Instance %s is not stopped yet' % instanceID)

	return {
		'statusCode': 200,
		'body': json.dumps('Hello from Lambda!')
	}