# Welcome to CopyCat
CopyCat is a library providing long-range (inter data center) data transport with ordering and exactly-once guarantees. On top of that CopyCat tries to make an effort to keep your data highly available by creating replicas.

The basic promise of CopyCat is that users are able to create and query arbitrary data structures and CopyCat copies them consistent across multiple other nodes in order from them to be highly available. For that purpose CopyCat heavily relies on the [excellent raft consensus implementation of etcd](https://github.com/coreos/etcd/tree/master/raft). The foundation of their work is the paper outlining the [raft protocol as an understandable consensus algorithm](https://raft.github.io/raft.pdf). CopyCat does not require a central repository for meta data (such as Zookeeper) instead it is trading its meta data via the gossip-like [SWIM protocol](http://www.cs.cornell.edu/Info/Projects/Spinglass/public_pdfs/SWIM.pdf).

## What is CopyCat not?
* CopyCat is not a data structure server (like Redis). CopyCat is not a service at all! It's just a library. The responsibility to serve up data in a particular format lies with the consumer of CopyCat. It would actually fairly easy to build a data structure server on top of CopyCat and further down in this readme we will do exactly that.
* CopyCat does not offer consistency between its nodes.

## Sample Code

In this section we will explain how to use CopyCat by walking through an example. In smaller pieces we will build a distributed hashmap on top of CopyCat.

First we need to start a CopyCat node by **creating the CopyCat config** object and passing the config to CopyCat.

```
ccc := copycat.DefaultConfig()
// Sets advertised host name within the CopyCat cluster.
// This hosts need to reachable via this host name.
ccc.Hostname = "127.0.0.1"
ccc.CopyCatPort = 7458
ccc.GossipPort = 7459
ccc.CopyCatDataDir = "./copy-cat-db/"
// Starting CopyCat with no peers to contact (nil or empty slice)
// will cause this node to start a new cluster
// If you want this node to connect to an existing node,
// you need to provide the gossip port of at least one other node in the cluster.
// This would look like so:
// ccc.PeersToContact = "127.0.0.1:17459 127.0.0.1:27459"
// Notice the space-separated list of peers to contact!!
ccc.PeersToContact = nil
err = os.MkdirAll(ccc.CopyCatDataDir, os.ModePerm)
if err != nil {
  log.Panicf("Can't create CopyCat directory: %s", err)
}

cc, err := copycat.NewCopyCat(ccc)
if err != nil {
  log.Panicf("Can't start CopyCat: %s", err.Error())
}
```

After CopyCat is started the instance needs to be used to connect to all data structures that want to be managed by CopyCat.

**Subscribing to a data structure managed by CopyCat** works like this:

```
type theMap struct {
	data             map[string]string
	mutex            *sync.RWMutex
	// used to propose changes to the data structure
	proposeCh        chan<- []byte
	// if proposed changes are committed, they will be replayed to you via this channel
	commitCh         <-chan []byte
	// if proposed changes cause an error, it will be replayed via this channel
	errorCh          <-chan error
	snapshotConsumer copycat.SnapshotConsumer
}

func newMap(cc copycat.CopyCat) (*theMap, error) {
	var err error
	m := &theMap{
		data:  make(map[string]string),
		mutex: &sync.RWMutex{},
	}

	m.proposeCh, m.commitCh, m.errorCh, m.snapshotConsumer, err = cc.SubscribeToDataStructureWithStringID("01CFR5R4JJE9KNRAJFFEAQ2SAJ", m.snapshotProvider)
	if err != nil {
		return nil, err
	}

	go m.serveChannels()
	return m, nil
}
```

Subscribing to a data structure returns a few channels and includes passing of function pointers. Let's take a look at them one by one (don't worry, we will look at code later too):
* Propose Channel: The propose channel is a write-only channel consuming arbitrary byte slices. These byte slices can contain pretty much anything. CopyCat will (under the covers) take this byte slice and forward it to all other rafts in the respective raft group and make sure there is consensus about the existence of these byte slices and the order at which they were consumed. Once all rafts in the raft group agree, a change is considered committed. This channel emits a nil value instead of a byte slice in case CopyCat received a new snapshot that is supposed to loaded.
* Commit Channel: The commit channel is a read-only channel conveying all committed byte slices in the order they were sent to the propose channel. These byte slices are relayed in the exact same order as they were received (and committed) by the propose channel. In addition CopyCat guarantees that each byte slice is relayed to this channel exactly once.
* Error Channel: Errors that occur while trying to commit change proposals, this read-only channel will relay them to you.
* Snapshot Provider Function: At regular intervals CopyCat will ask you to provide a consistent snapshot of your data structure. It indicates that to you by calling this function. This function is supposed to serialize your data structure into a byte slice and return it (back to CopyCat).
* Snapshot Consumer Function: You will receive this function pointer from CopyCat. When the commit channel emits a nil-value, CopyCat has a new snapshot ready for you to load. This is done by calling this function. Essentially this function will provide you the same byte slice you created in the snapshot provider. You're supposed to deserialize this byte slice and set your data structure to comply to the state found in this byte slice.

If all of this sounds complicated, don't be worried. We will look at what to do with these channels and functions in our example.

**Put and get method implementations** for our hashmap would look like this:

```
func (m *theMap) get(key string) string {
	// nothing fancy here
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.data[key]
}

func (m *theMap) put(key, value string) {
	// take key and value and create an operation
	item := &pb.Operation{
		Key:   key,
		Value: value,
	}

	// marshal operation into byte slice
	bites, err := item.Marshal()
	if err != nil {
		log.Errorf("Can't marshal op: %s", err.Error())
	}

	// send byte slice to propose channel
	m.proposeCh <- bites
}
```

How to **serve the channels** we received from CopyCat.

```
func (m *theMap) serveChannels() {
	for {
		select {
		case data, ok := <-m.commitCh:
			if !ok {
				return
			}

			// the committed entry is nil, that means we need to reload the
			// data structure from a consistent snapshot
			if data == nil {
				// retrieve the byte slice containing the snapshot from CopyCat
				bites, err := m.snapshotConsumer()
				if err != nil {
					log.Errorf("Error getting snapshot: %s", err.Error())
				}

				// deserialize the byte slice
				tcData := &pb.ReplicatMap{}
				err = proto.Unmarshal(bites, tcData)
				if err != nil {
					log.Errorf("Error unmarshaling snapshot: %s", err.Error())
				}

				// set internal structure to reflect the new state
				m.mutex.Lock()
				m.data = tcData.M
				m.mutex.Unlock()
			} else {
				// data is not nil and therefore contains a valid byte slice
				// in this case an operation
				// unmarshal byte slice into operation
				newOp := &pb.Operation{}
				err := newOp.Unmarshal(data)
				if err != nil {
					log.Errorf("Can't unmarshal operation: %s", err.Error())
				}

				// apply operation
				m.mutex.Lock()
				if newOp.Value == "" {
					// empty value means "delete"
					delete(m.data, newOp.Key)
				} else {
					// non-nil value means "set"
					m.data[newOp.Key] = newOp.Value
				}
				m.mutex.Unlock()
			}

		case err, ok := <-m.errorCh:
			if !ok {
				return
			}
			log.Errorf("Ran into error: %s", err.Error())
		}
	}
}
```

The last piece is the **snapshot provider**.

```
func (m *theMap) snapshotProvider() ([]byte, error) {
	// take current state and marshal it into a byte slice
	daMap := &pb.ReplicatMap{M: m.data}
	return daMap.Marshal()
}
```

## Internals

This section will go into more detail about the inner workings of CopyCat.

### Terminology
**There is no point in reading on further if you don’t know how the raft protocol works. [Luckily there’s a great illustrative explanation here.](http://thesecretlivesofdata.com/raft/)**

* **Raft Node (or just "a raft"):** A collection of threads with a defined API running the raft protocol. A raft does not map to a single machine (or container). Multiple rafts can be colocated in the same process. Rafts come in two flavors: detached and interactive. Detached rafts are only available via CopyCats internal API and the consumer doesn’t know of their existence. Detached rafts main purpose is to provide high availability. These rafts are not reachable via a public API and cannot be interacted with. Interactive rafts are exposed via the API and can be manipulated by the consumer of CopyCat.
* **Raft Group:** A group of rafts that own a particular piece of data and keep consensus amongst them about what the current state is. A raft group is the unit of leader election and consistency in CopyCat. As long as at least one member of a raft group is alive, your data is safe. If you lose an entire raft group, your data became unavailable (or will be lost depending on your setup).
* **Channels:** Are the major API primitive of CopyCat. Items within a channel have a natural order. Making channels CopyCats API primitives is CopyCats way of telling you that the order of events you send to CopyCat is your problem. At the same time channels are great for scaling processing of items.
* **Gossip:** As CopyCat has no central coordination mechanism, meta data about nodes and rafts in the cluster is traded between nodes using gossip. No consumer data is ever transported with gossip only meta data about the existence and location of data is traded.

## TODOs
* link full source code example
