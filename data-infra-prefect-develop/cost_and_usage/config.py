import os

ENV = os.environ['ENV']
AWS_GAMELYFT_SERVICE_NAME = 'Amazon GameLift Streams'
TARGET_TABLE = 'anodot_costs'


# Cloud providers configuration with their accounts and metadata
CLOUD_PROVIDERS = {
    'aws': {
        'anodot_account_id': '975957211390',
        'linked_account': {
            '58264306858': {
                'name': 'Staging',
                'request_params': {
                    'periodGranLevel': 'day',
                    'filters[service]': AWS_GAMELYFT_SERVICE_NAME,
                    'groupBy': ['region', 'usagetype']
                }
            },
            '357632654526': {
                'name': 'Main',
                'request_params': {
                    'periodGranLevel': 'day',
                    'filters[service]': AWS_GAMELYFT_SERVICE_NAME,
                    'groupBy': ['region', 'usagetype']
                }
            }
        }
    },
    'gcp': {
        'anodot_account_id': 'Ludeo-ebe4fd',
        'linked_account': {
            '653067671343': {
                'name': 'Staging',
                'request_params': {
                    'periodGranLevel': 'day',
                    'groupBy': ['service', 'region', 'instancetype']
                }
            },
            '914179264610': {
                'name': 'Main',
                'request_params': {
                    'periodGranLevel': 'day',
                    'groupBy': ['service', 'region', 'instancetype']
                }
            }
        }
    }
}
