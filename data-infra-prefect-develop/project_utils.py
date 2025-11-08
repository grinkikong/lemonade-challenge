import os

from data_utils.aws_secrets_manager_utils import AWSSecretsManagerDriver

ENV = os.environ["ENV"]
secrets_manager_driver = AWSSecretsManagerDriver(env=ENV)
