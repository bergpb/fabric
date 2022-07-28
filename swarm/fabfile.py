from os import environ
from fabric import Connection, SerialGroup, ThreadingGroup, task

username = environ.get("USER")

CLUSTER = {
    "all": {
        "address": ["node1", "node2", "node3"],
    },
    "main": {
        "address": ["node1"],
    },
    "workers": {
        "address": ["node2", "node3"],
    },
    "private_key": f"/home/{username}/.ssh/id_rsa",
    "username": "ubuntu"
}


def con(host, user, private_key):
    """A connection to an SSH daemon, with methods for commands and file transfer."""
    return Connection(
        host=host,
        user=user,
        connect_kwargs={"key_filename": private_key},
    )


def sg(hosts, user, private_key):
    """Executes in simple, serial fashion."""
    return SerialGroup(
        *hosts,
        user=user,
        connect_kwargs={"key_filename": private_key},
    )


def tg(hosts, user, private_key):
    """Uses threading to execute concurrently."""
    return ThreadingGroup(
        *hosts,
        user=user,
        connect_kwargs={"key_filename": private_key},
    )


def _open_main_ports(c):
    c.sudo(
    """
        sed -i "s/-A INPUT -j REJECT --reject-with icmp-host-prohibited//" /etc/iptables/rules.v4 && \
        netfilter-persistent flush && \
        netfilter-persistent start
    """, hide=True, warn=True
    )


def _system_upgrade(c):
    print("Installing requirements...")
    c.sudo("apt-get update && sudo apt-get upgrade -y", hide=True)


def _install_and_configure_docker(c):
    print("Installing docker using convenience script...")
    c.sudo("curl -sSL https://get.docker.com | sh -", hide=True)
    c.sudo("usermod -aG docker $USER", hide=True)
    c.run("id -g", hide=True)


def _configure_main_swarm(c, main_address):
    print("Initializing Docker Swarm cluster...")
    c.run(f"docker swarm init --advertise-addr {main_address}", hide=True)


def _get_join_worker_token(c):
    print("Getting worker token...")
    output = c.run("docker swarm join-token worker -q", hide=True)
    worker_token = output.stdout.replace("\n", "")
    # Use this when you need to return stdout from a GroupResult
    # for _, result in output.items():
    #     worker_token = result.stdout.replace("\n", "")
    return worker_token


def _configure_workers_swarm(c, token, main_address):
    print("Joining worker nodes to Docker Swarm...")
    c.run(f"docker swarm join --token {token} {main_address}:2377")


def _destroy_cluster(c):
    c.run("docker swarm leave -f", hide=True, warn=True)


def _deploy_application(c):
    print("Deploying visualizer service...")
    c.run("""
        docker service create \
        --detach \
        --name=viz \
        --publish=8080:8080/tcp \
        --constraint=node.role==manager \
        --mount=type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        alexellis2/visualizer-arm:latest
    """, hide=True)


@task
def swarm(c, upgrade="no", destroy="no"):
    """
    Deploy a Swarm cluster and create a visualizer service running on port 8080
    Usage: fab swarm -u yes -d yes
    -c, --upgrade - upgrade system
    -d, --destroy - destroy Swarm cluster before create
    """

    all = CLUSTER["all"]
    leaders = CLUSTER["leaders"]
    followers = CLUSTER["followers"]
    private_key = CLUSTER["private_key"]
    username = CLUSTER["username"]

    if destroy == "yes":
        with tg(all['address'], username, private_key) as c:
            _destroy_cluster(c)

    if upgrade == "yes":
        with tg(all['address'], username, private_key) as c:
            _system_upgrade(c)
            _install_and_configure_docker(c)

    for leader in leaders['address']:
        with con(leader, username, private_key) as c:
            _open_main_ports(c)
            _configure_main_swarm(c, leader)
            token = _get_join_worker_token(c)
            _deploy_application(c)

    with sg(followers['address'], username, private_key) as c:
        leaders_address = leaders["address"][0]
        _configure_workers_swarm(c, token, leaders_address)
