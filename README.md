<<<<<<< HEAD

# Materials for Kafka learning

This is a collection of materials to learn the basics of [Apache Kafka](https://kafka.apache.org/).

## Starting the cluster

To bring up a small KRaft-based Kafka cluster and a [`kafka-ui`](https://github.com/provectus/kafka-ui) server, there are two options:

- using a Docker Desktop Dev Environment, which will clone the repository for you.
- cloning the repository yourself and using Docker Compose directly.

### Docker Desktop Dev Environment

Open [this link](https://open.docker.com/dashboard/dev-envs?url=https://github.com/agarciadom/kafka-materials/tree/main).

When asked for a name by Docker Desktop, enter `kafka-materials`.
_NOTE_: if you do not do this, the scripts in the `scripts` folder will not work!

### Docker Compose

Clone this repository with your favourite Git client.

From a Bash shell (e.g. Git Bash on Windows), run this command from the main directory of your clone:

```sh
./compose-dev.sh up -d
```

## Web-based UI for Kafka

After some time from startup, the `kafka-ui` server should be available from:

http://localhost:8080
