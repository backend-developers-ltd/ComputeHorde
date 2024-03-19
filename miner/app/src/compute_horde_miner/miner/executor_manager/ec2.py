import logging

import boto3
from compute_horde_miner.miner.executor_manager.base import BaseExecutorManager, ExecutorUnavailable
from django.conf import settings

logger = logging.getLogger(__name__)
ec2_client = boto3.client('ec2', region_name='us-east-1')


class EC2ExecutorManager(BaseExecutorManager):

    miner_address = f'ws://{settings.ADDRESS_FOR_EXECUTORS}:{settings.PORT_FOR_EXECUTORS}'
    executor_image = settings.EXECUTOR_IMAGE
    launch_template_name = 'computehorde-executors'

    async def reserve_executor(self, token):

        # Attempt to launch spot instances first
        success = self.create_fleet_request('spot', token)
        if not success:
            logger.error("Failed to create spot request, trying on-demand")
            success = self.create_fleet_request('on-demand', token)

        if not success:
            raise ExecutorUnavailable('Failed to create EC2 instance')

    def create_fleet_request(self, capacity_type, token):
        resp = ec2_client.create_fleet(
            Type='instant',
            DryRun=False,
            ReplaceUnhealthyInstances=False,
            LaunchTemplateConfigs=[
                {
                    'LaunchTemplateSpecification': {
                        'LaunchTemplateName': self.launch_template_name,
                        'Version': '$Latest'
                    },
                }
            ],
            SpotOptions={
                'AllocationStrategy': 'lowest-price',
                'InstanceInterruptionBehavior': 'terminate'
            },
            OnDemandOptions={
                'AllocationStrategy': 'lowest-price',
            },
            TargetCapacitySpecification={
                'TotalTargetCapacity': 1,
                'DefaultTargetCapacityType': capacity_type
            },
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [
                        {
                            'Key': 'Name',
                            'Value': 'computehorde-executor'
                        },
                        {
                            'Key': 'EXECUTOR_IMAGE',
                            'Value': self.executor_image
                        },
                        {
                            'Key': 'MINER_ADDRESS',
                            'Value': self.miner_address
                        },
                        {
                            'Key': 'EXECUTOR_TOKEN',
                            'Value': token
                        },
                    ]
                },
            ]
        )

        # Output the InstanceIds launched, or error
        instances = resp['Instances']
        if len(instances) > 0:
            logging.info("Created fleet successfully. FleetId: %s, Instances: %s", resp['FleetId'], resp['Instances'])
            return True

        logging.error("Failed to create fleet: %s", resp)
        return False
