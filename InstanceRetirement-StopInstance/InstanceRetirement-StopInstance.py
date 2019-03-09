from __future__ import print_function
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import json

# Setup clients for EC2, SQS, and CloudWatch Events
ec2 = boto3.client('ec2')
sqs = boto3.client('sqs')
events = boto3.client('events')

# Main function
def lambda_handler(event, context):

	# CONFIGURATION REQUIRED FOR THE FOUR VARIABLES BELOW
	# You need two SQS queues and two CloudWatch event rules
	# For the stop and start Queue you need to provide the SQS URL as a string
	# For the stop and start rules you need to provide the CloudWarch event rule names as a string
	stopQueueUrl = 'YOUR_STOP_SQS_QUEUE_URL'
	startQueueUrl = 'YOUR_START_SQS_QUEUE_URL'
	stopRule = 'YOUR_STOP_CLOUDWATCH_EVENT_RULE_NAME'
	startRule = 'YOUR_START_CLOUDWATCH_EVENT_RULE_NAME'

	# The next two lines gets the current time in UTC in 'HH:MM:SS' format
	currentDT = datetime.utcnow()
	currentDT = currentDT.strftime('%H:%M:%S')

	# Poll the stop queue for messages. Long polling is used by setting 'WaitTimeSeconds' to '5'
	# so all SQS servers hosting the queue are polled.
	sqsResponse = sqs.receive_message(
		QueueUrl=stopQueueUrl,
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
	# If a message was found the attributes from the message (InstanceId, MaintWindowStartTime, and
	# MaintWindowEndTime) are set them equal to 'instanceID', 'maintWindowStartTime', and 'maintWindowEndTime'
	# respectively. 'receiptHandle' is also set equal to the messages receipt handle so the message can be
	# deleted from the queue if necessary.
	if 'Messages' in sqsResponse:
		messageAttributes = sqsResponse['Messages'][0]['MessageAttributes']
		receiptHandle = sqsResponse['Messages'][0]['ReceiptHandle']
		print('Received message from the stop queue. Receipt handle: %s' % receiptHandle)

		instanceID = messageAttributes['InstanceId']['StringValue']
		maintWindowStartTime = messageAttributes['MaintWindowStartTime']['StringValue']
		maintWindowEndTime = messageAttributes['MaintWindowEndTime']['StringValue']
	else:
		print('No messages found in the stop queue. Stopping function.')
		return {
		'statusCode': 200,
		'body': json.dumps('Hello from Lambda!')
	}

	# Check to see if the current time is within the maintenance window specified. If it is, an attempt
	# to stop the instance is made. If the call to stop the instance is successful, a SQS message is sent
	# to the start queue, the stop CloudWatch event rule is disabled, and the start CloudWatch event rule
	# is enabled so it can begin polling the start queue. 'instanceID' is passed into the message so the
	# 'InstanceRetirement-StartInstance' script can use it. If the current time is not within the maintenance
	# window specified, the script ends and the message is returned to the stop queue.
	if currentDT > maintWindowStartTime and currentDT < maintWindowEndTime:
		print('Within the timeframe, attempting to stop instance %s' % instanceID)

		# Try stopping the specified instance as a DryRun first to verify permissions
		try:
			ec2.stop_instances(InstanceIds=[instanceID], DryRun=True)
		except ClientError as e:
			if 'DryRunOperation' not in str(e):
				print("You do not have permission to stop instances")
				raise

		# Permissions have been verified. Try stopping the specified instance
		try:
			ec2Response = ec2.stop_instances(InstanceIds=[instanceID], DryRun=False)
			print('Instance %s is stopping' % instanceID)

			# The instance has been stopped, delete the SQS message
			sqs.delete_message(
				QueueUrl = stopQueueUrl,
				ReceiptHandle=receiptHandle
			)
			print('Deleted message: %s' % receiptHandle)

			# Disable the stop CloudWatch event rule
			disableRuleResponse = events.disable_rule(Name=stopRule)
			print('%s has been disabled' % stopRule)

			# Send an SQS message to the start queue
			sqsResponse = sqs.send_message(
				QueueUrl=startQueueUrl,
				DelaySeconds=10,
				MessageAttributes={
					'InstanceId' : {
						'DataType': 'String',
						'StringValue': instanceID
					}
				},
				MessageBody=(
					'From InstanceRetirement-StopInstance: Instance start scheduled for ' + instanceID
				)
			)
			print('Message sent to the start queue for %s' % instanceID)

			# Enable the start CloudWatch event rule so it can start polling the start queue
			enableRuleResponse = events.enable_rule(Name=startRule)
			print('%s has been enabled' % startRule)

		except ClientError as e:
			print('Error', e)

	# Current time is not within the maintenance window, the message is returned to the queue
	else:
		print('Not within the timeframe, not stopping %s' % instanceID)

	return {
		'statusCode': 200,
		'body': json.dumps('Hello from Lambda!')
	}