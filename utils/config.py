"""
This file reads config file and returns a dictionary.
"""

import yaml


def get_config():
    """
    This function reads the config and returns dictionary.
    :return: config
    """
    with open('config.yaml') as file:
        return yaml.load(file, Loader=yaml.FullLoader)
