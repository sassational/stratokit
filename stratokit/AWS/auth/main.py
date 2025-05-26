import boto3

class AWSClientCreator():
    """
    Class used to create AWS Boto3 clients for various AWS services.
    """

    def __init__(self: object, aws_region: str = 'us-east-1') -> object:
        """
        Initialize the AWSClientCreator with a default, or user based AWS region.

        Attributes:
            region_name (str): AWS region to use when creating clients. Default is 'us-east-1'.
        """

        self.region_name = aws_region

        return None
    

    def create_client(self: object, service_name: str, aws_access_key_id: str = None, aws_secret_access_key = None, aws_session_token = None) -> object:
        """
        Create a Boto3 client for the specified AWS service.

        Parameters:
            service_name (str): The name of the AWS service.

        Returns:
            object: The Boto3 client for the requested service.

        Raises:
            Exception: If the client creation fails.
        """

        params = {
            'service_name': service_name,
            'region_name': self.region_name
        }

        if aws_access_key_id is not None and aws_secret_access_key is not None and aws_session_token is not None:

            params['aws_access_key_id'] = aws_access_key_id
            params['aws_secret_access_key'] = aws_secret_access_key
            params['aws_session_token'] = aws_session_token

        elif aws_access_key_id is not None and aws_secret_access_key is not None and aws_session_token is None:

            params['aws_access_key_id'] = aws_access_key_id
            params['aws_secret_access_key'] = aws_secret_access_key
            
        try:

            client = boto3.client(**params)

            return client
        
        except Exception as e:

            print(f'Error creating the AWS service client for "{service_name}": {e}')
            
            raise
