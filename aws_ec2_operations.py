__author__ = 'harpreet.paul'
import boto.ec2
import time
from ConfigParser import SafeConfigParser

class aws_ec2_operations:

    ec2 = None

    def __init__(self, region, access_key_id, secret_access_key):
        self.ec2 = boto.ec2.connect_to_region(region, aws_access_key_id = access_key_id, aws_secret_access_key = secret_access_key)

    def getInstance(self, name):
        for instance in self.ec2.get_only_instances():
            if instance.state <> 'terminated':
                if instance.tags['Name'] == name:
                    return instance
        return None

    def update_tags(self, instances):
        c = SafeConfigParser()
        c.add_section("main")
        kafka_cfgfile = open("aws_kafka.cfg", 'w')

        for instance in instances:
            inst = self.getInstance(instance)
            d = {'private_ip_address':inst.private_ip_address, 'ip_address':inst.ip_address, 'dns_name':inst.private_dns_name,
                 'public_dns_name':inst.public_dns_name}
            c.set("main",instance, str(d))
        c.write(kafka_cfgfile)
        kafka_cfgfile.close()

    def create_instances(self, image_id, key_name, instance_type, security_group, instances):
        #create ec2 instances.
        reservation = self.ec2.run_instances(
                                                image_id = image_id,
                                                key_name = key_name,
                                                instance_type = instance_type,
                                                security_groups =[security_group],
                                                min_count = len(instances),
                                                max_count = len(instances)
        )

        time.sleep(10)
        #add tags to the created instances.
        for index, instance in enumerate(reservation.instances):
            instance.add_tag("Name", instances[index])


        time.sleep(10)
        #update the aws_hadoop_hosts.cfg file with the instance tag and the public ip address.
        self.update_tags(instances)
