""" Create a raftis cluster in openstack."""

import os
import sys
import time
import socket
import json

from collections import Counter
from tempfile import NamedTemporaryFile
import threading

from fabric.decorators import hosts
from fabric.api import task, env, sudo, puts, parallel
from fabric.operations import put

from novaclient.client import Client


CREATE_TIMEOUT = 720.0
DEFAULT_IMAGE = 'emi-ubuntu-14.04-server-amd64'


if 'OS_PASSWORD' in os.environ:
    env.password = os.environ['OS_PASSWORD']


def get_nova_creds():
    """ Translate environment variables into nova keyword parameters. """
    d = {}
    d['username'] = os.environ['OS_USERNAME']
    d['api_key'] = os.environ['OS_PASSWORD']
    d['auth_url'] = os.environ['OS_AUTH_URL']
    d['project_id'] = os.environ['OS_TENANT_NAME']
    return d



def raftis_cluster_hosts():
  nova = Client("1.1", **get_nova_creds())
  return [str(host.metadata['fqdn']) for host in nova.servers.list()]


raftis_config_template = """description "raftis"

script
  export PATH={home_dir}/bin:$$PATH
  # The 'storestream' command reads config using the 'miloconfig' package.
  # We want it to use the same logging configuration as everyone else.
  exec raftis -r {myaddr}:6379 -i {myaddr}:1103 -d {home_dir}/var/raftis -p {peersaddr}
end script"""


#
# deploy to cluster
#

@task
@parallel
@hosts(raftis_cluster_hosts())
def deploy():
  config_file = NamedTemporaryFile()
  gen_raftis_config(outfile=config_file)

  config_file.seek(0)
  raftis_cfg = json.load(config_file)

  home_dir = '/opt/raftis'

  _, shard, replica, _ = env.host_string.split('.')[0].split('-')

  mypeers = ','.join(
    ['{}:1103'.format(peer['host'])
      for peer in raftis_cfg['shards'][int(shard)]['hosts']])

  raftis_config_template.format(
    home_dir=home_dir, myaddr=env.host_string, peersaddr=mypeers)

  init_script = NamedTemporaryFile()
  init_script.write(raftis_config_template.format(
    home_dir=home_dir, myaddr=env.host_string, peersaddr=mypeers))
  init_script.flush()

  sudo('mkdir -p {0}/bin {0}/etc {0}/var/raftis'.format(home_dir))

  put(config_file.name, '{}/etc/raftis.conf'.format(home_dir), use_sudo=True)
  put('raftis', '{}/bin/raftis'.format(home_dir), use_sudo=True)
  put(init_script.name, '/etc/init/raftis.conf', use_sudo=True)
  sudo('chmod +x {}/bin/raftis'.format(home_dir))


@task
@parallel
@hosts(raftis_cluster_hosts())
def start():
  sudo('start raftis')

@task
@parallel
@hosts(raftis_cluster_hosts())
def stop():
  from fabric.api import settings
  service_name = 'raftis'

  with settings(warn_only=True):
    res = sudo('service {} status | grep running'.format(service_name))
    if res.return_code == 0:
      sudo('stop {}'.format(service_name))

@task
@hosts(raftis_cluster_hosts())
def install():
  sudo('apt-get install liblmdb0')
  sudo('ln -s /usr/lib/x86_64-linux-gnu/liblmdb.so.0.0.0 /usr/lib/x86_64-linux-gnu/liblmdb.so')


def gen_raftis_config(slots=1, outfile=sys.stdout):
  nova = Client("1.1", **get_nova_creds())

  hosts = sorted([{'host': host.metadata['fqdn'],
            'group': host.name.split('-')[2],
            'shard': int(host.name.split('-')[1])}
              for host in nova.servers.list()],
                 key=lambda host: host['shard'])

  cnt = Counter()
  for host in hosts:
      cnt[host['group']] += 1

  if cnt.values().count(cnt.values()[0]) != len(cnt.values()):
      # I actually don't know how to handle fabric errors
      # This will do for now
      raise SystemExit

  shards = cnt.values()[0]

  shardscfg = [{} for _ in range(shards)]
  for host in hosts:
    shardscfg[host['shard']].setdefault('hosts', []). append(
      {'host': host['host'], 'group': host['group']})
    if 'slots' not in shardscfg[host['shard']]:
        shardscfg[host['shard']]['slots'] = range(
          host['shard'], shards * slots, shards)

  raftiscfg = {
    "numSlots": shards * slots,
    "shards": shardscfg
  }

  json.dump(raftiscfg, outfile)
  outfile.flush()


@task
@hosts('localhost')
def cluster(shards=5, flavor='tiny', image=DEFAULT_IMAGE, key_name='raftis'):
  # Let's figure out what hosts we need
  datacenters = set(['lvs', 'phx', 'slc'])
  expected_hosts = {
    'raftis-{0:0{width}}-{datacenter}'.format(
      shard, width=len(str(shards)), datacenter=datacenter)
        for shard in range(shards) for datacenter in datacenters}

  nova = Client("1.1", **get_nova_creds())

  if not nova.keypairs.findall(name=key_name):
    with open(os.path.expanduser('~/.ssh/id_rsa.pub')) as fpubkey:
      nova.keypairs.create(name=key_name, public_key=fpubkey.read())


  create_params = dict(
    flavor=nova.flavors.find(name=flavor),
    image=nova.images.find(name=image),
    key_name=key_name
  )

  # for now sequentially, something is going wrong wiht gevent and nova
  raftis_cluster = []
  for host in expected_hosts:
    raftis_instance = threading.Thread(
      target=create_raftis_instance,
      args=(nova, host, create_params))
    raftis_instance.start()
    raftis_cluster.append(raftis_instance)

  while True:
    rm_list = []
    for oper in raftis_cluster:
      if not oper.is_alive():
        rm_list.append(oper)

    for rmop in rm_list:
      raftis_cluster.remove(rmop)

    if len(raftis_cluster) == 0:
      break


def create_raftis_instance(nova, name, create_params):
    puts("Creating '{}'".format(name))
    instance = nova.servers.create(name=name, **create_params)

    # Poll at 5 second intervals, until the status is no longer 'BUILD'
    status = instance.status
    while status == 'BUILD':
        time.sleep(5)
        # Retrieve the instance again so the status field updates
        instance = nova.servers.get(instance.id)
        status = instance.status

    puts("'{}' built, waiting for DNS...".format(name))
    fqdn = instance.metadata['fqdn']
    # This fqdn is not immediately available to us so wait here
    wait_for_dns_update(fqdn)
    puts("'{}' successfully installed".format(name))



def wait_for_dns_update(fqdn):
    """ Wait until the host appears in DNS.

        OpenStack reports the fqdn before its actually ssh-able.
    """
    while True:
        try:
            socket.gethostbyname(fqdn)
            break
        except Exception:
            time.sleep(0.2)


