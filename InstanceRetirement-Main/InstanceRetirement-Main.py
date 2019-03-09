from __future__ import print_function
import boto3
from botocore.exceptions import ClientError
import pytz
from datetime import datetime, timedelta
import os
import json

# Setup clients for EC2, SQS, and CloudWatch Events
ec2 = boto3.client('ec2')
sqs = boto3.client('sqs')
events = boto3.client('events')

# Checks if the given timezone is currently in daylight savings time.
# Returns true if it is daylight savings time and false for standard time.
def is_dst(zonename):
	tz = pytz.timezone(zonename)
	now = pytz.utc.localize(datetime.utcnow())
	return now.astimezone(tz).dst() != timedelta(0)

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

	# The following lines get all of the instances in your AWS account and check each one to see if
	# they have a health event that matches 'instance-retirement'. If one is found, 'instanceID' is set
	# to the instance id that has the 'instance-retirement' health event.
	# By default 'instanceID' is set to 'none' and at the end of the lines below it checks to see if
	# this variable is still set equal to 'none', if it is the function ends because no action is needed.
	instanceID = 'none'
	
	instancesResponse = ec2.describe_instances()
	for reservation in instancesResponse['Reservations']:
		for instance in reservation['Instances']:
			statusResponse = ec2.describe_instance_status(InstanceIds=[instance['InstanceId']])
			if len(statusResponse['InstanceStatuses']) > 0:
				if 'Events' in statusResponse['InstanceStatuses'][0]:
					statusResponse = statusResponse['InstanceStatuses'][0]['Events'][0]['Code']
			if statusResponse == 'instance-retirement':
				instanceId = instance['InstanceId']
	if instanceID == 'none':
		print('No instances found with event code = instance-retirement. Ending function')
		return {
			'statusCode':200,
				'body': json.dumps('Hello from Lambda!')
		}

	# The next two lines gets the current time in UTC in 'HH:MM:SS' format
	currentDT = datetime.utcnow()
	currentDT = currentDT.strftime('%H:%M:%S')

	# The variable 'tz' is set equal to the 'TimeZone' environment variable from the lambda function
	# and is passed into 'is_dst()' to determine if it is daylight savings or standard time for that
	# timezone. The result of 'is_dst()' determines which environment variables 'maintWindowStartTime'
	# and 'maintWindowEndTime' are set equal to. DST = Daylight Savings Time, ST = Standard Time.
	tz = os.environ['TimeZone']
	if is_dst(tz):
		maintWindowStartTime = os.environ['MaintWindowStart_DST']
		maintWindowEndTime = os.environ['MaintWindowEnd_DST']
	else:
		maintWindowStartTime = os.environ['MaintWindowStart_ST']
		maintWindowEndTime = os.environ['MaintWindowEnd_ST']

	# Check to see if the current time is within the maintenance window specified. If it is, an attempt
	# to stop the instance is made. If the call to stop the instance is successful, a SQS message is sent
	# to the start queue and the start CloudWatch event rule is enabled so it can begin polling the start
	# queue. 'instanceID' is passed into the message so the 'InstanceRetirement-StartInstance' script can
	# use it. If the current time is not within the maintenance window specified, a SQS message is sent to
	# the stop queue and the stop CloudWatch event rule is enabled so it can begin polling the stop queue.
	# 'instanceID', 'maintWindowStartTime', and 'maintWindowEndTime' are passed into the message so the
	# 'InstanceRetirement-StopInstance' script can use them.	
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
			print('Stopping instance %s' % instanceID)

			# The instance has been stopped, send an SQS message to the start queue
			sqsSendResponse = sqs.send_message(
				QueueUrl=startQueueUrl,
				DelaySeconds=5,
				MessageAttributes={
					'InstanceId' : {
						'DataType': 'String',
						'StringValue': instanceID
					}
				},
				MessageBody=(
					'From InstanceRetirement-Main: Instance start scheduled for ' + instanceID
				)
			)
			print('Message sent to the start queue')

			# Enable the start CloudWatch event rule so it can start polling the start queue
			enableRuleResponse = events.enable_rule(Name=startRule)
			print('%s has been enabled' % startRule)

		except ClientError as e:
			print('Error', e)

	# Current time is not within the maintenance window
	else:
		print('Not within the timeframe, sending message to the stop queue')
		# Send an SQS message to the stop queue
		sqsSendResponse = sqs.send_message(
			QueueUrl=stopQueueUrl,
			DelaySeconds=10,
			MessageAttributes={
				'InstanceId' : {
					'DataType': 'String',
					'StringValue': instanceID
				},
				'MaintWindowStartTime': {
					'DataType': 'String',
					'StringValue': maintWindowStartTime
				},
				'MaintWindowEndTime': {
					'DataType': 'String',
					'StringValue': maintWindowEndTime
				}
			},
			MessageBody=(
				'From InstanceRetirement-Main: Instance stop has been queued for ' + instanceID
			)
		)
		print('Message sent to the stop queue')

		# Enable the stop CloudWatch event rule so it can start polling the stop queue
		enableRuleResponse = events.enable_rule(Name=stopRule)
		print('%s has been enabled' % stopRule)

	return {
		'statusCode': 200,
		'body': json.dumps('Hello from Lambda!')
	}