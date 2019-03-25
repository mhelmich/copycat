# Design Notes

## Raft Groups

Raft Groups are the primitives of data ownership. A Raft Group consists of a few connected rafts. Raft Groups are in itself independent units owning a data structure.

They all run the same raft state machine and communicate using the raft protocol.

As already indicated different Raft Groups have no knowledge of each other and are the safe-keepers of a data structure. They elect leaders, trigger the addition of new rafts to their group, and detect failures.

## Membership Metadata

Common cluster metadata includes:
* cluster information (e.g. node id)
* network address (e.g. host, port)
* physical location (e.g. datacenter, rack)
* information about data structures and their local raft ids

All of this info is slammed into the serf tags (one ```map[string]string```). Well-known keys start with a special character (```_```). All other keys are assumed to be of shape ```data structure id -> raft id```. All these keys are taken out of the tags and denormalized according to their most common lookup needs (e.g. ```raft id -> network address```).

## Membership, Rafts, Crush, and their circular Dependency

* Membership stashes the node topology (including data structure distribution)
* Crush needs access to the topology to compute replica nodes
* Rafts are figuring out leadership in their group by themselves and raft leaders should be responsible to creating new replicas (if needed)
