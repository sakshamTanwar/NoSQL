/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

transaction::transaction(int id, string key, string value, int time) {
	this->id = id;
	this->key = key;
	this->value = value;
	this->quorumCount = 0;
	this->time = time;
}

transaction::transaction() {}

void transaction::increaseQuorum() {
	this->quorumCount++;
}

int transaction::getQuorum() {
	return this->quorumCount;
}

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	if(hasListChanged(curMemList)) {
		change = true;
		ring = curMemList;
	}
	



	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring

	if(ring.size() > 0 && change)
		stabilizationProtocol();

}

/**
 * FUNCTION NAME: hasListChanged
 * 
 * DESCRIPTION: This function checks if the list has changed
 * */

bool MP2Node::hasListChanged(vector<Node> newList) {
	if(newList.size() != ring.size())
		return true;

	for(int i = 0; i<(int)newList.size(); i++) {
		if(ring[i].getAddress()->getAddress() != newList[i].getAddress()->getAddress()) {
			return true;
		}
	}
	return false;
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: sendMessage
 * 
 * DESCRIPTION: Send Message to the given address
 */
void MP2Node::sendMessage(Address *toAddr, Message msg) {
	emulNet->ENsend(&memberNode->addr, toAddr, msg.toString());
}

/**
 * Message Handlers
 */

void MP2Node::createMessageHandler(Message msg) {
	bool success = createKeyValue(msg.key, msg.value, msg.replica);
	if(success) {
		log->logCreateSuccess(&memberNode->addr, false, msg.transID, msg.key, msg.value);
	}
	else {
		log->logCreateFail(&memberNode->addr, false, msg.transID, msg.key, msg.value);
	}
	Message repMes(msg.transID, memberNode->addr, REPLY, success);
	sendMessage(&msg.fromAddr, repMes);
}

void MP2Node::readMessageHandler(Message msg) {
	string _value = readKey(msg.key);
	if(_value == "") {
		log->logReadFail(&memberNode->addr, false, msg.transID, msg.key);
		return;
	}
	log->logReadSuccess(&memberNode->addr, false, msg.transID, msg.key, _value);
	Message repMes(msg.transID, memberNode->addr, READREPLY, _value);
	repMes.value = _value;
	sendMessage(&msg.fromAddr, repMes);
}

void MP2Node::updateMessageHandler(Message msg) {
	bool success = updateKeyValue(msg.key, msg.value, msg.replica);
	if(success) {
		log->logUpdateSuccess(&memberNode->addr, false, msg.transID, msg.key, msg.value);
	}
	else {
		log->logUpdateFail(&memberNode->addr, false, msg.transID, msg.key, msg.value);
	}
	Message repMes(msg.transID, memberNode->addr, REPLY, success);
	sendMessage(&msg.fromAddr, repMes);
}

void MP2Node::deleteMessageHandler(Message msg) {
	bool success = deletekey(msg.key);
	if(success) {
		log->logDeleteSuccess(&memberNode->addr, false, msg.transID, msg.key);
	}
	else {
		log->logDeleteFail(&memberNode->addr, false, msg.transID, msg.key);
	}
	Message repMes(msg.transID, memberNode->addr, REPLY, success);
	sendMessage(&msg.fromAddr, repMes);
}

void MP2Node::replyMessageHandler(Message msg) {
	if(msg.success) {
		if(trans.count(msg.transID) != 0) {
			trans[msg.transID].increaseQuorum();
		}
	}
}

void MP2Node::readReplyMessageHandler(Message msg) {
	if(trans.count(msg.transID) != 0) {
		trans[msg.transID].increaseQuorum();
		trans[msg.transID].value = msg.value;
	}
}


/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */

	vector<Node> addr_vec = findNodes(key);
	int id = g_transID++;
	transType[id] = CREATE;
	trans[id] = transaction(id, key, value, par->getcurrtime());
	Message prMsg(id, memberNode->addr, CREATE, key, value, PRIMARY);
	Message seMsg(id, memberNode->addr, CREATE, key, value, SECONDARY);
	Message teMsg(id, memberNode->addr, CREATE, key, value, TERTIARY);
	
	sendMessage(addr_vec[0].getAddress(), prMsg);
	sendMessage(addr_vec[1].getAddress(), seMsg);
	sendMessage(addr_vec[2].getAddress(), teMsg);

}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */

	vector<Node> addr_vec = findNodes(key);
	int id = g_transID++;
	transType[id] = READ;
	trans[id] = transaction(id, key, "", par->getcurrtime());
	Message msg(id, memberNode->addr, READ, key);
	for(auto it : addr_vec) {
		sendMessage(it.getAddress(), msg);
	}

}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */

	vector<Node> addr_vec = findNodes(key);
	int id = g_transID++;
	transType[id] = UPDATE;
	trans[id] = transaction(id, key, value, par->getcurrtime());
	Message prMsg(id, memberNode->addr, UPDATE, key, value, PRIMARY);
	Message seMsg(id, memberNode->addr, UPDATE, key, value, SECONDARY);
	Message teMsg(id, memberNode->addr, UPDATE, key, value, TERTIARY);
	
	sendMessage(addr_vec[0].getAddress(), prMsg);
	sendMessage(addr_vec[1].getAddress(), seMsg);
	sendMessage(addr_vec[2].getAddress(), teMsg);	

}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */

	vector<Node> addr_vec = findNodes(key);
	int id = g_transID++;
	transType[id] = DELETE;
	trans[id] = transaction(id, key, "", par->getcurrtime());
	Message msg(id, memberNode->addr, DELETE, key);
	for(auto it : addr_vec) {
		sendMessage(it.getAddress(), msg);
	}

}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table

	if(ht->count(key) > 0) {
		return false;
	}

	return ht->create(key, value);
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	if(ht->count(key) != 1) {
		return false;
	}
	return ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	if(ht->count(key) != 1) {
		return false;
	}

	return ht->deleteKey(key);

}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */
		

		Message msg(message);
		if(msg.type == CREATE) {
			createMessageHandler(msg);
		}
		else if(msg.type == READ) {
			readMessageHandler(msg);
		}
		else if(msg.type == UPDATE) {
			updateMessageHandler(msg);
		}
		else if(msg.type == DELETE) {
			deleteMessageHandler(msg);
		}
		else if(msg.type == REPLY) {
			replyMessageHandler(msg);
			// cout<<message<<"\n";
		}
		else if(msg.type == READREPLY) {
			readReplyMessageHandler(msg);
		}

	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */

	manageTransactions();
}

void MP2Node::manageTransactions() {
	for(auto it : trans) {
		int id = it.first;
		transaction t = it.second;

		if(t.getQuorum() >= 2) {
			MessageType type = transType[id];
			if(type == CREATE) {
				log->logCreateSuccess(&memberNode->addr, true, id, t.key, t.value);
			}
			else if(type == READ) {
				log->logReadSuccess(&memberNode->addr, true, id, t.key, t.value);
			}
			else if(type == UPDATE) {
				log->logUpdateSuccess(&memberNode->addr, true, id, t.key, t.value);
			}
			else if(type == DELETE) {
				log->logDeleteSuccess(&memberNode->addr, true, id, t.key);
			}

			trans.erase(id);

		}
		else if(par->getcurrtime() - t.time > T_TIMEOUT) {
			MessageType type = transType[id];
			if(type == CREATE) {
				log->logCreateFail(&memberNode->addr, true, id, t.key, t.value);
			}
			else if(type == READ) {
				log->logReadFail(&memberNode->addr, true, id, t.key);
			}
			else if(type == UPDATE) {
				log->logUpdateFail(&memberNode->addr, true, id, t.key, t.value);
			}
			else if(type == DELETE) {
				log->logDeleteFail(&memberNode->addr, true, id, t.key);
			}

			trans.erase(id);
		}

	}
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */

	

}
