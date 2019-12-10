# pylint: disable=import-error
import os
import yaml
import json


class ConfigManager:
    @classmethod
    def get_config(cls, config_file=None):
        config_file = config_file or os.environ["DEFAULT_CONFIG"]
        config = cls.__load_config(config_file)
        return config

    @classmethod
    def get_config_value(cls, component, value=None, config_file=None):
        config = cls.get_config(config_file)
        return config[component][value] if value is not None else config[component]

    @staticmethod
    def __load_config(filename):
        try:
            config = yaml.load(open(filename), yaml.SafeLoader)
            config = replace_env(config)
        except Exception as e:
            raise Exception("Error: Can't parse config file {}. {}".format(filename, str(e)))
        return config


def replace_env(d):
    for k, v in d.items():
        if type(v) is dict:
            d[k] = replace_env(v)
        elif type(v) is str:
            env_var = find_env(v)
            if env_var:
                value = os.environ[env_var[2:-1]]
                d[k] = v.replace(env_var, value)
    return d


def find_env(s):
    start = s.find("${")
    if start < 0:
        return None
    end = s.find("}")
    if end < 0:
        return None
    return s[start : end + 1]
