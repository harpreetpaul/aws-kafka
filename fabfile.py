__author__ = 'harpreet.paul'
from node_operations import *
from config_operations import *
from aws_ec2_operations import *
import time
from fabric_helper import *

c = ConfigOps()
kafka_cluster = KafkaCluster()

@task
def create_aws_kafka_cluster():
    local('python kafka_cluster.py')

@task
def update_config():
    ec2 = aws_ec2_operations(
        region=c.aws_region,
        access_key_id=c.aws_access_key_id,
        secret_access_key=c.aws_secret_access_key
    )
    ec2.update_tags(c.all_nodes)


@task
def test_config():
    print kafka_cluster.getNode(c.all_kafka_cluster_nodes).ip_address


@task
def install_salt():
    #Install Salt Master
    fb = fabric_helper(
        host_ip  = kafka_cluster.getNode(c.saltmaster).ip_address,
        host_user = c.aws_user,
        host_key_file = c.aws_key_location
    )
    fb.install_salt_master()
    time.sleep(5)

    #Install Salt Minions
    hosts = c.all_kafka_cluster_nodes
    for host in hosts:
        fb = fabric_helper(
            host_ip  = kafka_cluster.getNode(host).ip_address,
            host_user = c.aws_user,
            host_key_file = c.aws_key_location
        )
        fb.install_salt_minion(master = kafka_cluster.getNode(c.saltmaster).private_ip_address, minion = host)
    time.sleep(5)

    #Accept Salt minions keys in Salt Master.
    fb = fabric_helper(
        host_ip  = kafka_cluster.getNode(c.saltmaster).ip_address,
        host_user = c.aws_user,
        host_key_file = c.aws_key_location
    )
    fb.salt_master_keys_accept()
    fb.run_salt_master_ping()
    time.sleep(5)

@task
def setup_kafka_nodes_access():
    env.user = c.aws_user
    env.key_filename = c.aws_key_location
    hosts = c.all_hadoop_nodes
    for host in hosts:
        env.host_string = kafka_cluster.getNode(host).ip_address

        #Changing the host name of hadoop nodes to EC2 private dns name.
        cmd_change_hostname = 'hostname {0}'.format(kafka_cluster.getNode(host).dns_name)
        sudo(cmd_change_hostname)

        #Changing /etc/hosts file to remove localhost and replacing it with the dns name and 127.0.0.1 with localip.
        sudo('sed -i -e "s/localhost/{0}/" /etc/hosts'.format(kafka_cluster.getNode(host).dns_name))
        sudo('sed -i -e "s/127.0.0.1/{0}/" /etc/hosts'.format(kafka_cluster.getNode(host).private_ip_address))


    # Setting up passwordless login from hadoopnamenode to all other hadoop nodes.
    env.host_string = kafka_cluster.getNode(c.hadoop_namenode).ip_address
    #generating ssh keys in id_rsa, no passphrase.
    run('ssh-keygen -t rsa -f /home/ubuntu/.ssh/id_rsa -q -N ""')
    #adding StrictHostKeyChecking no in the .ssh/config file so that ssh login is not prompted.
    run('echo "{0}" > /home/ubuntu/.ssh/config'.format("Host *"))
    run('echo "{0}" >> /home/ubuntu/.ssh/config'.format("   StrictHostKeyChecking no"))
    #Getting public key from hadoopnamenode
    public_key = sudo('cat /home/ubuntu/.ssh/id_rsa.pub')

    env.host_string = kafka_cluster.getNode(c.saltmaster).ip_address

    #Issuing a minion blast of public key to all hadoop nodes to enable passwordless login.
    minion_cmd = "echo '{0}' >> /home/ubuntu/.ssh/authorized_keys".format(public_key)
    sudo('salt "*" cmd.run "{0}"'.format(minion_cmd))
    time.sleep(2)

@task
def install_jdk_kafka_nodes():
    env.host_string = kafka_cluster.getNode(c.saltmaster).ip_address
    env.user = c.aws_user
    env.key_filename = c.aws_key_location
    with settings(warn_only = True):
        sudo('salt "*" cmd.run "sudo apt-get update"')
        sudo('salt "*" cmd.run "sudo add-apt-repository ppa:webupd8team/java"')
        sudo('salt "*" cmd.run "echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections"')
        sudo('salt "*" cmd.run "sudo apt-get update && sudo apt-get install -y oracle-java8-installer"')
        sudo('salt "*" cmd.run "sudo apt-get -f -y -q install"')

@task
def install_packages():
    env.host_string = kafka_cluster.getNode(c.saltmaster).ip_address
    env.user = c.aws_user
    env.key_filename = c.aws_key_location

    # Install zookeeper
    sudo('salt -C "zookepernode*" cmd.run "wget http://download.nextag.com/apache/zookeeper/zookeeper-3.4.9/zookeeper-3.4.9.tar.gz -P /home/ubuntu"')
    sudo('salt -C "zookepernode*" cmd.run "tar -xzvf /home/ubuntu/zookeeper-3.4.9.tar.gz -C /home/ubuntu"')
    sudo('salt -C "zookepernode*" cmd.run "mv /home/ubuntu/zookeeper-3.4.9 /home/ubuntu/zookeeper"')
    sudo('salt -C "zookepernode*" cmd.run "rm -rf /home/ubuntu/zookeeper-3.4.9.tar.gz"')

    # Install kafka
    sudo('salt -C "kafkanode*" cmd.run "wget http://mirrors.sonic.net/apache/kafka/0.10.0.1/kafka_2.11-0.10.0.1.tgz -P /home/ubuntu"')
    sudo('salt -C "kafkanode*" cmd.run "tar -xzvf /home/ubuntu/kafka_2.11-0.10.0.1.tgz -C /home/ubuntu"')
    sudo('salt -C "kafkanode*" cmd.run "mv /home/ubuntu/kafka_2.11-0.10.0.1 /home/ubuntu/kafka"')
    sudo('salt -C "kafkanode*" cmd.run "rm -rf /home/ubuntu/kafka_2.11-0.10.0.1.tgz"')

    #changing the hadoop directory owner to ubuntu.
    sudo('salt -C "zookepernode*" cmd.run "sudo chown -R ubuntu /home/ubuntu/zookeeper"')
    sudo('salt -C "kafkanode*" cmd.run "sudo chown -R ubuntu /home/ubuntu/kafka"')

    #cmd = "echo '{0}' >> /home/ubuntu/.bashrc".format("export HADOOP_CONF=/home/ubuntu/hadoop/etc/hadoop")
    #sudo('salt "*" cmd.run "{0}"'.format(cmd))

    cmd = "echo '{0}' >> /home/ubuntu/.bashrc".format("export ZOOKEEPER_PREFIX=/home/ubuntu/zookeeper")
    sudo('salt -C "zookepernode*" cmd.run "{0}"'.format(cmd))

    cmd = "echo '{0}' >> /home/ubuntu/.bashrc".format("export KAFKA_PREFIX=/home/ubuntu/kafka")
    sudo('salt -C "kafkanode*" cmd.run "{0}"'.format(cmd))

    cmd = "echo '{0}' >> /home/ubuntu/.bashrc".format("export JAVA_HOME=/usr/lib/jvm/java-8-oracle")
    sudo('salt "*" cmd.run "{0}"'.format(cmd))

    #cmd = "echo '{0}' >> /home/ubuntu/.bashrc".format("export PATH='$'PATH:'$'HADOOP_PREFIX/bin")
    #sudo('salt "*" cmd.run "{0}"'.format(cmd))

@task
def deploy_zookeeper_config():
    env.host_string = kafka_cluster.getNode(c.saltmaster).ip_address
    env.user = c.aws_user
    env.key_filename = c.aws_key_location

    sudo('salt -C "zookepernode*" cmd.run "mkdir /home/ubuntu/zookeeper/data"')
    sudo('salt -C "zookepernode*" cmd.run "cp /home/ubuntu/zookeeper/conf/zoo_sample.cfg /home/ubuntu/zookeeper/conf/zoo.cfg"')
    sudo('salt -C "zookepernode*" cmd.run "sudo chown -R ubuntu /home/ubuntu/zookeeper"')

    # config file setup
    sudo('salt -C "zookepernode*" cmd.run "echo \'tickTime=2000\' > /home/ubuntu/zookeeper/conf/zoo.cfg"')
    sudo('salt -C "zookepernode*" cmd.run "echo \'dataDir=/home/ubuntu/zookeeper/data\' >> /home/ubuntu/zookeeper/conf/zoo.cfg"')
    sudo('salt -C "zookepernode*" cmd.run "echo \'clientPort=2181\' >> /home/ubuntu/zookeeper/conf/zoo.cfg"')
    sudo('salt -C "zookepernode*" cmd.run "echo \'initLimit=5\' >> /home/ubuntu/zookeeper/conf/zoo.cfg"')
    sudo('salt -C "zookepernode*" cmd.run "echo \'syncLimit=2\' >> /home/ubuntu/zookeeper/conf/zoo.cfg"')
    sudo('salt -C "zookepernode*" cmd.run "echo \'dataLogDir=/home/ubuntu/zookeeper/log\' >> /home/ubuntu/zookeeper/conf/zoo.cfg"')
    i = 0
    for zook_node in c.zookeper_nodes:
        i = i + 1
        sudo('salt -C "{0}" cmd.run "echo {0} > /home/ubuntu/zookeeper/data/myid"'.format(zook_node, i))
        sudo('salt -C "{0}" cmd.run "echo \'server.{1}={2}:2888:3888\' >> /home/ubuntu/zookeeper/conf/zoo.cfg"'.format(zook_node, i, kafka_cluster.getNode(zook_node).dns_name))
    sudo('salt -C "zookepernode*" cmd.run "sudo chown -R ubuntu /home/ubuntu/zookeeper"')

@task
def start_zookeeper():
    env.user = c.aws_user
    env.key_filename = c.aws_key_location

    for zook_node in c.zookeper_nodes:
        env.host_string = kafka_cluster.getNode(zook_node).ip_address
        run("/home/ubuntu/zookeeper/bin/zkServer.sh start")
        run("jps")


def deploy_kafka_config():
    env.host_string = kafka_cluster.getNode(c.saltmaster).ip_address
    env.user = c.aws_user
    env.key_filename = c.aws_key_location

    sudo('salt -C "kafkanode*" cmd.run "echo \"port=9092" > /home/ubuntu/kafka/config/server.properties"')
    sudo('salt -C "kafkanode*" cmd.run "echo \"num.partitions=4\" >> /home/ubuntu/kafka/config/server.properties"')
    i = 0;
    for kaf_node in c.kafka_nodes:
        i = i + 1
        sudo('salt -C "{0}" cmd.run "echo \"broker.id={1}\" >> /home/ubuntu/kafka/config/server.properties"'.format(kaf_node, i))
        sudo('salt -C "{0}" cmd.run "echo \"host.name={1}\" >> /home/ubuntu/kafka/config/server.properties"'.format(kaf_node, kafka_cluster.getNode(kaf_node).dns_name))

    ','.join(kafka_cluster.getNode(str(x)).dns_name for x in c.zookeper_nodes)
    sudo('salt -C "kafkanode*" cmd.run "echo \"zookeeper.connect={0}\" >> /home/ubuntu/kafka/config/server.properties"'.format(','.join(kafka_cluster.getNode(str(x)).dns_name + ":2181" for x in c.zookeper_nodes)))

@task
def start_kafka():
    env.user = c.aws_user
    env.key_filename = c.aws_key_location

    for kaf_node in c.kafka_nodes:
        env.host_string = kafka_cluster.getNode(kaf_node).ip_address
        run("/home/ubuntu/kafka/bin/kafka-server-start.sh /home/ubuntu/kafka/config/server.properties")
        run("jps")

@task
def provision_kafka_cluster():
    execute(create_aws_kafka_cluster)
    execute(update_config)
    execute(install_salt)
    execute(install_packages)
    execute(install_jdk_kafka_nodes)
    execute(deploy_zookeeper_config)
    execute(start_zookeeper)
    execute(deploy_kafka_config)
    execute(start_kafka)




