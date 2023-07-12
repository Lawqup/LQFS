# LQFS

My own attempt at building a distributed filesystem.

## TODO

* [ ] Build nodes' APIs (not REST endpoints or anything yet, just functions that can be called) for IO + deal with duplicates/delays/misorderings
* [ ] Use Raft for replication
* [ ] Build client to figure out which nodes to contact and how to split up the file into fragments
* [ ] Change APIs to http endpoints and test using Kubernetes
* [ ] Set up nodes on the cloud
* [ ] Profit

## Project filestructure

- [node](./node): The server binary that handles distributed file storage
- [client](./client): The client that interfaces with the user and coordinates requests
