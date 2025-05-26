import time
import uuid

class RDS():

    def __init__(self: object, aws_client: object, credentials: dict = None) -> None:
        """
        A class to manage AWS RDS database instances and clusters.

        This class provides methods to create, delete, and retrieve information 
        about RDS database instances and their clusters.

        Attributes:
            rds_client: RDS client.
            
        """

        params = {}

        if credentials is not None:

            params = credentials

        params['service_name'] = 'rds'

        self._rds_client = aws_client.create_client(**params)

        return None
    

    def _is_cluster_snapshot_ready(self: object, snapshot_id: str) -> None:
        """
        Check whether an RDS cluster snapshot is finished (status == 'available').

        Parameters:
            snapshot_id (str): DBClusterSnapshotIdentifier to check.

        Raises:
            Exception: If snapshot creation fails.

        """

        try:

            while True:
                
                resp = self._rds_client.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=snapshot_id)
                snapshots = resp.get('DBClusterSnapshots', [])

                if not snapshots:
                    
                    raise ValueError(f"Snapshot '{snapshot_id}' not found.")

                status = snapshots[0]['Status']
                
                print(f"Snapshot '{snapshot_id}' status: {status}")

                if status == 'available':
                    
                    print("Snapshot is now available.")
                    
                    break

                time.sleep(60)

        except Exception as e:

            print(f"Error while trying to get RDS Cluster Snapshot: {e}")

            raise

        return None

    
    def _share_rds_snapshot(self: object, snapshot_id: str, destination_account_id: str) -> None:
        """
        Shares a manual Amazon RDS snapshot with another AWS account.

        Parameters:
            snapshot_id (str): The identifier of the RDS snapshot to be shared.
            destination_account_id (str): The AWS account ID with which to share the snapshot.

        Returns:
            None

        """

        try:

            response = self._rds_client.modify_db_cluster_snapshot_attribute(DBClusterSnapshotIdentifier=snapshot_id, AttributeName='restore', ValuesToAdd=[destination_account_id])
            
            print(f"Snapshot {snapshot_id} shared with the AWS account {destination_account_id}")
            
            return response
        
        except Exception as e:
        
            print(f"Error while trying to share snapshot: {e}")
        
        return None
    
    
    def _is_cluster_restored(self:object, db_cluster_identifier: str) -> None:
        """
        Check whether the RDS cluster restored from a snapshot is in 'available' status.

        Parameters:
            db_cluster_identifier (str): Identifier of the cluster being restored.

        Returns:
            None.

        Raises:
            Exception: If AWS call fails.
        """

        rds = self._rds_client

        try:

            while True:

                response = rds.describe_db_clusters(DBClusterIdentifier=db_cluster_identifier)

                clusters = response.get("DBClusters", [])

                if not clusters:

                    raise ValueError(f"Cluster '{db_cluster_identifier}' not found.")

                status = clusters[0]['Status']

                print(f"Cluster {db_cluster_identifier} status: {status}")

                if status == 'available':

                    print("Cluster is now available.")

                    break

                time.sleep(60)

        except Exception as e:

            print(f"Unexpected error happened while trying to reach the restored cluster status: {e}")

            raise

        return None


    def _is_instance_available(self: object, db_instance_identifier: str) -> None:
        """
        Waits until the RDS DB instance is available.

        Parameters:
            db_instance_identifier (str): Identifier of the DB instance to check.

        Returns:
            None

        Raises:
            Exception: If an AWS error occurs.
        """

        rds = self._rds_client

        try:
            
            while True:
                
                response = rds.describe_db_instances(DBInstanceIdentifier=db_instance_identifier)
                
                instances = response.get("DBInstances", [])

                if not instances:
                
                    raise ValueError(f"No DB instance found with identifier '{db_instance_identifier}'.")

                status = instances[0]['DBInstanceStatus']
                
                print(f"Instance '{db_instance_identifier}' status: {status}")

                if status == 'available':
                
                    print(f"Instance '{db_instance_identifier}' is now available.")
                
                    break

                time.sleep(60)

        except Exception as e:
            
            print(f"Error checking DB instance status: {e}")
            
            raise

        return None
    

    def find_snapshots_by_partial_name(self: object, partial_name: str, cluster: bool = False) -> list:

        """
        Searches for RDS snapshots (standard or cluster) whose identifiers contain a given partial name.

        Parameters:
            partial_name (str): Substring to search for in the snapshot identifiers.
            cluster (bool): If True, searches DB cluster snapshots (e.g., Aurora). 
                        If False, searches standard DB instance snapshots.

        Returns:
            list: A list of snapshot names that match the search criteria.

        Raises:
            Exception: If an AWS error occurs.
        """

        snapshots = []

        rds = self._rds_client

        try:

            if cluster:

                response = rds.describe_db_cluster_snapshots(IncludeShared=True)
                key = 'DBClusterSnapshots'
                id_key = 'DBClusterSnapshotIdentifier'

            else:
                
                response = rds.describe_db_snapshots(IncludeShared=True)
                key = 'DBSnapshots'
                id_key = 'DBSnapshotIdentifier'

            for snapshot in response.get(key, []):
                
                snapshot_id = snapshot.get(id_key, '')

                if partial_name.lower() in snapshot_id.lower():
                    
                    snapshots.append(snapshot)

        except Exception as e:

            print(f"Error while searching for snapshots: {e}")

        return snapshots


    def create_rds_cluster_snapshot(self: object, cluster_id: str, snapshot_identifier: str, snapshot_shared_account: str = None) -> None:
        """
        Creates a snapshot of an RDS cluster.

        Parameters:
            cluster_id (str): The identifier of the RDS cluster.
            snapshot_identifier (str): Custom identifier for the snapshot.
            snapshot_shared_account (str): AWS account ID that the snapshot will be shared with.
        
        Returns:
            dict: AWS response containing details of the created cluster snapshot.

        Raises:
            Exception: If snapshot creation fails.
        """

        try:

            self._rds_client.create_db_cluster_snapshot(
                DBClusterIdentifier=cluster_id,
                DBClusterSnapshotIdentifier=snapshot_identifier
            )

            self._is_cluster_snapshot_ready(snapshot_id=snapshot_identifier)

            print(f"Cluster snapshot '{snapshot_identifier}' created successfully for cluster '{cluster_id}'.")

            if snapshot_shared_account is not None:

                self._share_rds_snapshot(snapshot_id=snapshot_identifier, destination_account_id=snapshot_shared_account)

            return None

        except Exception as e:

            print(f"Failed to create cluster snapshot: {e}")

            raise
        
    
    def restore_rds_cluster_from_snapshot(self: object, 
                                          db_cluster_identifier: str, 
                                          snapshot_identifier: str,
                                          engine: str,
                                          db_cluster_instance_class: str, 
                                          db_subnet_group: str, 
                                          vpc_security_group_ids: list,
                                          kms_key_id: str = None,
                                          reset_master_password: bool = False) -> None:
        """
        Restore an RDS cluster from a snapshot and create an associated DB instance.

        Parameters:
            db_cluster_identifier (str): The identifier for the new restored cluster.
            engine (str): The engine used by the RDS cluster.
            db_cluster_instance_class (str): Instance class for the DB instance (e.g., 'db.r5.large').
            db_subnet_group (str): Name of the DB subnet group.
            vpc_security_group_ids (list): List of security group IDs.
            kms_key_id (str, optional): KMS key ARN/ID to use for encryption.
            reset_master_password (bool, optional): If you want to create a Secret Manager with the master credentials.

        Returns:
            None

        Raises:
            Exception: If restore or instance creation fails.
        """

        rds = self._rds_client

        params = {

            'DBClusterIdentifier': db_cluster_identifier,
            'SnapshotIdentifier': snapshot_identifier,
            'DBClusterInstanceClass': db_cluster_instance_class,
            'DBSubnetGroupName': db_subnet_group,
            'VpcSecurityGroupIds': vpc_security_group_ids,
            'Engine': engine

        }

        if kms_key_id is not None:
            
            params['KmsKeyId'] = kms_key_id

        try:

            rds.restore_db_cluster_from_snapshot(**params)

            self._is_cluster_restored(db_cluster_identifier=db_cluster_identifier)

            if reset_master_password:

                rds.modify_db_cluster(DBClusterIdentifier=db_cluster_identifier, ManageMasterUserPassword=True, ApplyImmediately=True)
            
            print(f"Cluster snapshot '{snapshot_identifier}' restored successfully for cluster '{db_cluster_identifier}'.")

            instance_params = {

                'db_name': db_cluster_identifier,
                'instance_type': db_cluster_instance_class,
                'engine': engine

            }

            cluster_name = db_cluster_identifier
            instance_name = self.create_db_instance(**instance_params)

            return cluster_name, instance_name
        
        except Exception as e:

            print(f"Failed to restore cluster based in the snapshot: {e}")

            raise

    
    def create_db_instance(self: object, db_name: str, instance_type: str, engine: str) -> None: 
        """
        Creates a new RDS database instance in the specified cluster.

        Parameters:
            db_name (str): The identifier of the database cluster.
            instance_type (str): The instance class for the new database instance.
            engine: (str): The database engine that are going to be used (Ex: Postgres, MySQL, etc...)

        Returns:
            None

        Raises:
            Exception: If restore or instance creation fails.
        """

        uuid_hex = uuid.uuid4().hex[:5]
        instance_name = f"{db_name}-{uuid_hex}"

        try:

            self._rds_client.create_db_instance(
                DBInstanceIdentifier=instance_name,
                DBInstanceClass=instance_type,
                Engine=engine,
                DBClusterIdentifier=db_name,
            )

            self._is_instance_available(db_instance_identifier=instance_name)
            
            print(f'New database instance created in the {db_name}: {instance_name}')

        except Exception as e:

            print(f"Error while creating RDS instance {instance_name}: {e}")
            
            raise

        return instance_name


    def delete_db_instance(self: object, instance_name: str) -> None:
        """
        Deletes the specified RDS database instance.

        Parameters:
            instance_name (str): The identifier of the database instance to delete.

        Returns:
            None.

        Raises:
            Exception: If restore or instance creation fails.
        """

        try:

            self._rds_client.delete_db_instance(
                DBInstanceIdentifier=instance_name,
                SkipFinalSnapshot=True
            )

            print(f'The AWS RDS Cluster Database {instance_name} is being deleted...')

        except Exception as e:
            
            print(f"Error while trying to delete te RDS instance {instance_name}: {e}")
            
            raise

        return None
    

    def delete_rds_cluster(self: object, cluster_identifier: str) -> None:
        """
        Deletes an Amazon RDS cluster (e.g., Aurora).

        Parameters:
            cluster_identifier (str): The RDS cluster identifier.

        Returns:
            None.

        Raises:
            Exception: If restore or instance creation fails.
        """

        rds = self._rds_client

        try:

            rds.delete_db_cluster(
                DBClusterIdentifier=cluster_identifier,
                SkipFinalSnapshot=True
            )

            print(f'The AWS RDS Cluster {cluster_identifier} is being deleted...')
            
            return None

        except Exception as e:

            print(f"Error deleting RDS cluster: {e}")

            raise
