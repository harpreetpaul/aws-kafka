__author__ = 'rakesh.varma'
from ConfigParser import *

class ConfigOps:
    def __init__(self):
        self.config = SafeConfigParser()
        self.config.read('config.ini')

    @property
    def kafka_nodes(self):
        kafka_nodes =  eval(self.config.get("main", "nodes"))
        return list(kafka_nodes['kafkanodes'])

    @property
    def zookeper_nodes(self):
        zook_nodes =  eval(self.config.get("main", "nodes"))
        return list(zook_nodes['zookepernodes'])

    @property
    def aws_key_location(self):
        return self.config.get("main", "aws_key_location")

    @property
    def aws_region(self):
        return self.config.get("main", "aws_region")

    @property
    def aws_access_key_id(self):
        return self.config.get("main", "aws_access_key_id")

    @property
    def aws_secret_access_key(self):
        return self.config.get("main", "aws_secret_access_key")

    @property
    def aws_image_id(self):
        return self.config.get("main", "aws_image_id")

    @property
    def aws_key_name(self):
        return self.config.get("main", "aws_key_name")

    @property
    def aws_instance_type(self):
        return self.config.get("main", "aws_instance_type")

    @property
    def aws_security_group(self):
        return self.config.get("main", "aws_security_group")

    @property
    def aws_user(self):
        return self.config.get("main", "aws_user")

    @property
    def saltmaster(self):
        return self.config.get("main","salt_master")

    @property
    def all_kafka_cluster_nodes(self):
        nodes = []
        for kafka_node in self.kafka_nodes:
            nodes.append(kafka_node)
        for zook_node in self.zookeper_nodes:
            nodes.append(zook_node)
        return nodes

    @property
    def all_nodes(self):
        nodes = self.all_kafka_cluster_nodes
        nodes.append(self.saltmaster)
        return nodes
