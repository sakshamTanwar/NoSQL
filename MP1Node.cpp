/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <chrono>

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        msg = new MessageHdr();

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        msg->memberList = memberNode->memberList;
        msg->addr = &memberNode->addr;

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, sizeof(MessageHdr));

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
    return 1;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

void MP1Node::sendMessage(Address *to, MsgTypes msgType)
{
    MessageHdr *msg = new MessageHdr();

    msg->msgType = msgType;
    vector<MemberListEntry> newList;
    for(auto it : memberNode->memberList)
    {
        if(!checkFailed(it))
        {
            newList.push_back(it);
        }
    }
    msg->memberList = newList;

    msg->addr = &memberNode->addr;

    emulNet->ENsend(&memberNode->addr, to, (char *)msg, sizeof(MessageHdr));
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
	 MessageHdr *msg = (MessageHdr *)data;
	 /*
	 	Format for JOINREP {Header Welcome}
		*/

	 if(msg->msgType == JOINREQ) {
        mergeMemberList(msg->memberList);
        sendMessage(msg->addr, JOINREP);
        return true;
	 } else if(msg->msgType == JOINREP) {
        memberNode->inGroup = true;
        mergeMemberList(msg->memberList);
        return true;
	 }
     else if(msg->msgType == PING) {
         mergeMemberList(msg->memberList);
         for(auto &it : memberNode->memberList)
         {
             if(it.getid() == getId(&memberNode->addr))
             {
                it.setheartbeat(it.getheartbeat() + 1);
                it.settimestamp(par->getcurrtime());
                break;
             }
         }
         return true;
     }

     return false;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

    vector<MemberListEntry> newList;

    for(auto it : memberNode->memberList)
    {
        if(par->getcurrtime() - it.gettimestamp() < TREMOVE)
        {
            newList.push_back(it);
        }
        else
        {
            Address add = getAddrFromId(it.getid(), it.getport());
            log->logNodeRemove(&memberNode->addr, &add);
            // if(memberNode->addr.getAddress() == "1:0")
                // cout<<memberNode->addr.getAddress()<<" removed node "<<add.getAddress()<<"\n";
        }
        
    }

    memberNode->memberList = newList;


    

    srand(chrono::high_resolution_clock::now().time_since_epoch().count());

    // int id = getId(&memberNode->addr);

    int cnt = 0;

    while(cnt < GSPREAD)
    {
        if(memberNode->memberList.size() != 0)
        {
            int sendIndex = rand() % memberNode->memberList.size();
            Address addr = getAddrFromId(memberNode->memberList[sendIndex].getid(), memberNode->memberList[sendIndex].getport());
            sendMessage(&addr, PING);
            cnt++;
        }
        else
        {
            break;
        }
            
        // int sendIndex = 0;
        // if(memberNode->memberList[sendIndex].getid() != id)
        // {
            
        // }
    }

    // for(auto it : memberNode->memberList)
    // {
    //     if(it.getid() != id)
    //     {
    //         Address to = getAddrFromId(it.getid(), it.getport());
    //         sendMessage(&to, PING);
    //     }
    // }

    return;
}

bool MP1Node::checkFailed(MemberListEntry memEntry)
{
    return par->getcurrtime() - memEntry.gettimestamp() >= TFAIL;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

void MP1Node::mergeMemberList(vector<MemberListEntry> recTable)
{
    for(auto it : recTable)
    {
        bool found = false;
        for(int i = 0; i<(int)memberNode->memberList.size(); i++)
        {
            if(memberNode->memberList[i].getid() == it.getid())
            {
                found = true;
                if(!checkFailed(memberNode->memberList[i]))
                {
                    if(it.getheartbeat() > memberNode->memberList[i].getheartbeat())
                    {
                        memberNode->memberList[i].setheartbeat(it.getheartbeat());
                        memberNode->memberList[i].settimestamp(par->getcurrtime());
                    }
                }
                break;
            }
        }

        if(!found)
        {
            MemberListEntry entry(it.getid(), it.getport(), it.getheartbeat(), par->getcurrtime());
            memberNode->memberList.push_back(entry);
            Address add = getAddrFromId(it.getid(), it.getport());
            log->logNodeAdd(&memberNode->addr, &add);
            // cout<<memberNode->addr.getAddress()<<" added node "<<add.getAddress()<<"\n";
        }
    }
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
    MemberListEntry selfEntry(getId(&memberNode->addr), getPort(&memberNode->addr), memberNode->heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(selfEntry);
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;
}

int MP1Node::getId(Address *addr)
{
    string address = addr->getAddress();
    size_t pos = address.find(":");
    return stoi(address.substr(0, pos));
}

short MP1Node::getPort(Address *addr)
{
    string address = addr->getAddress();
    size_t pos = address.find(":");
    return (short)stoi(address.substr(pos + 1, address.size()-pos-1));
}

Address MP1Node::getAddrFromId(int id, short port)
{
    Address addr;
    memcpy(&addr.addr[0], &id, sizeof(int));
	memcpy(&addr.addr[4], &port, sizeof(short));
    return addr;
}
