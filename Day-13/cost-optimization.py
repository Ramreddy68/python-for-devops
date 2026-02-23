import boto3

def lambda_handler(event, context):
    ec2 = boto3.client('ec2')

    # Get all EBS snapshots owned by this account
    snapshots = ec2.describe_snapshots(OwnerIds=['self'])['Snapshots']

    # Get all active EC2 instances
    instances_response = ec2.describe_instances(
        Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]
    )
    active_instance_ids = {i['InstanceId'] for r in instances_response['Reservations'] for i in r['Instances']}

    for snapshot in snapshots:
        snapshot_id = snapshot['SnapshotId']
        volume_id = snapshot.get('VolumeId')

        if not volume_id:
            # Delete snapshot if it has no volume association
            try:
                ec2.delete_snapshot(SnapshotId=snapshot_id)
                print(f"Deleted snapshot {snapshot_id} (no volume attached).")
            except Exception as e:
                print(f"Error deleting {snapshot_id}: {e}")
        else:
            # Check if the volume still exists
            try:
                volume = ec2.describe_volumes(VolumeIds=[volume_id])['Volumes'][0]
                instance_ids = [att['InstanceId'] for att in volume['Attachments'] if 'InstanceId' in att]

                # If volume is not attached to any active instance, delete snapshot
                if not any(inst in active_instance_ids for inst in instance_ids):
                    ec2.delete_snapshot(SnapshotId=snapshot_id)
                    print(f"Deleted snapshot {snapshot_id} (volume {volume_id} not attached to active instance).")
            except Exception as e:
                # If volume doesn’t exist, snapshot is orphaned → delete
                if "InvalidVolume.NotFound" in str(e):
                    try:
                        ec2.delete_snapshot(SnapshotId=snapshot_id)
                        print(f"Deleted orphaned snapshot {snapshot_id} (volume {volume_id} missing).")
                    except Exception as inner_e:
                        print(f"Error deleting orphaned snapshot {snapshot_id}: {inner_e}")
                else:
                    print(f"Error checking volume {volume_id} for snapshot {snapshot_id}: {e}")

    return {
        'statusCode': 200,
        'body': 'Snapshot cleanup completed.'
    }
