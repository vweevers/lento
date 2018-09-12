# Tests

## Unit tests

```
npm t
```

## Integration tests

Requirements: `docker-compose` must be available in `PATH`.

### One-Time Run

```
npm run test-docker
```

### Repeated Runs

Manually start the cluster with `docker-compose up`, (re)run the tests with `node test/integration` and when you're done testing, stop the cluster with `docker-compose down -v`. The `-v` flag removes volumes.

The cluster exposes various web interfaces:

- Presto Coordinator: `http://<docker ip>:8080`
- Hadoop namenode: `http://<docker ip>:50070`
- Hadoop datanode: `http://<docker ip>:50075`
- Hadoop historyserver: `http://<docker ip>:8188`
- Hadoop Nodemanager: `http://<docker ip>:8042`
- Hadoop Resource manager: `http://<docker ip>:8088`
