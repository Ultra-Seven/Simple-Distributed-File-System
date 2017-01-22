# First Phase #
## What I have done ##
- Client

> 1. implement the functions in SDFSInputStream and SDFSOutputStream which can be used to read and write to blocks.

> 2. implement the ISimpleDistributedFileSystem interface so that the client could operate files through the interface.

- NameNode

> 1. implement open, create, mkdir, close, addBlock, rm functions.

> 2. the consistency of namenode

> 3. could recieve the heart beat from the datanode and verify whether it is alive.

- DataNode

> 1. implement read, write functions in the interface in datanode.

> 2. send heat beat(all block id) to namenode every one hour periodically.

> 3. store blocks in the disk.

- MapReduce

> 1. implement jobtracker which can split the job into tasks and send map task and reduce task to tasktracker.

> 2. implement tasktracker which can call map and reduce function and write the output files into distributed file system.

> ! however, because of not supporting map/reduce functions provided by clients flexibly, the MapReduce component can not pass an entire test.

- Connection

> 1. java rmi remote calling


## Problems ##

1. At first, I didn't know how to implement the interconnection among client, namenode and datanode. To enhance the distribution of these different components, I use rmi to acheieve this goal. Every component has their own server and main function to initiate according process in a distributed way. 
2. When reading bytes from datanode, using java rmi to call the function, one of the argument byte array can not use the feature of passing by reference. To solve this problem, I modify the interface and it can return a byte array, which can be received by clients.
3. How to allow clients to provide their own map and reduce function? It seems to use java class loading...???(I have no time to try it out...)

## Extra ##
1. using java rmi to implement distributed components genuinely.
2. implement rm function to allow client remove a existing file in the system.
3. MapReduce function framework, but it can not work without errors completely.
4. implement heart beat in the datanode to verify if the datanode should be removed from the file system.


## Test Procedure ##
1. you should make sure that you have following directories: dir, meta, data in the project directory.
2. run SDFSMaster.java to initiate the NameNode.
3. run SDFSSlave.java to initiate the DataNode.
4. new a SimpleDistributedFileSystem object and you can call the function in this object to attain either SDFSInputStream or SDFSOutputStream. Then you can operate some data through the stream.
 
Test.java is an example of testing the simple distributed file system. 


# Second Phase #

## RPC Design ##
- Client 

	1. client can only see interfaces, so we could distribute interfaces to the client respectively, which can improve extensibility and safty of the code.
	2. if client want to call the specific function defined in the interface, getProxy() will be called and then the implementing class having the same hashcode with real class will be delivered to the client, using object in/output stream.

- Sever
	1. both datanode server and namenode server implement a server interface which has register and call function mainly. The register is responsible for putting a specific class into the hash map and the call function will fetch the class from the hash map.
	2. Invocation is such a set of class properties used as one of argument between transportation.
	3. Listen is a thread to listen to the port of server. Once there is a request, the listener will invoke the call function in server and write target object into stream.
	4. RPC and RPCClient have responsibility to return a class to the client. To some extent they are just like a server stub.

- Architecture design

![](http://i.imgur.com/CawcIQx.png)

## Cache Design ##
- locatedBlocksCache

> When a SDFSFileChannel is set up, all of located blocks are pulled down from the name node. 
> 
> During read/write operations, any modification of located block is only visible in locatedBlocksCache. 
> 
> Ideally, we assume that the cache size is unlimited  which means that new blocks can be added into cache freely. 
> 
> At the end of reading/writing, all of modifications should be updated in namenode synchronously with the help of RPC.

- dataBlocksCache

> Differentiating with locatedBlocksCache, the size of dataBlocksCache is fixed during the lifespan of SDFSFileChannel, which means the content of some block may not have vaccancy in the cache.
> 
> So replacement algorithm should be exploited in such situation. In this lab, I implement LRU and CClock algorithm to supersede a stale block contents. If the element kicked out is dirty, the content of this block should be updated in remote data node with the help of RPC.
> 
> At the end of reading/writing, all of modifications of block contents should be updated in namenode synchronously with the help of RPC.

## Problems and Solutions ##
- problem description: to implement a better seperation of client and server, I expect to provie client with interfaces only and client is blind to real object which is sent by name node. In this way, the file system could have a better extensibility.
> Solution: I could use dynamic proxy object which can search a properiate concrete object associating with an given interface and send it to the client dynamically.

- problem description: because I use a dynamic proxy to implement RPC instead of conventional socket communication, it seems difficult to throw an exception from name node to client node.
> Solution: I came up with two solutions. I could either add an exception interface handling possible exception throw or append an exception as one of attribute in NameNode. Considering the compleity of the implementation, the later one is adopted.
 
## Architecture ##
no significant change

## Extra work ##
- dynamic proxy RPC
- delete file implementation
- rename file implementation
- CClock algorithm


# Third Phase #
## Thread-Safe Design ##
As the intention of less locks used in this lab, I attempted to contrive a distributed file system with the "lock-free" feature. A well-designed thread-safe funtion should consist of some atomic operations. To implement these operations, atomic directives and locks must be taken into consideration. However, none of locks is impossible, at least hard to me. Therefore, I use some data structures encapsulated by java concurrent labrary and optimistic concurrency control designed by myself to organise multi-service which may cause some conflicts and that is why I put the quote on.


- ConcurrentHashMap

> The ConcurrentHashMap is a hash table supporting full concurrency of retrievals and adjustable expected concurrency for updates. And there is not any support forlocking the entire table but a lock for every segment, instead. In this lab, the name node(NameNodeMetaData.java) maintain two concurrent hash tables including directory table and file table which could ensure that every operation inserting or removing from the table is thread-safe.

- ConcurrentQueue
> A analogous data structure implemented by java natively, which provides some thread-safe operations without locks. Differentiating from concurrent hashmap, we could use CAS, a kind of atomic instruction in CPU, to implement a simple, fast, and practical non-Blocking Concurrent Queue. The specific algorithm is [http://www.cs.rochester.edu/research/synchronization/pseudocode/queues.html#nbq](http://www.cs.rochester.edu/research/synchronization/pseudocode/queues.html#nbq). In this lab, I use this concurrent queue to store commands requested by clients and only in this way can I implement a optimistic concurrency control as below.

- optimistic concurrency control

> Optimistic concurrency control (OCC) is a concurrency control method applied to transactional systems such as relational database management systems and software transactional memory. OCC assumes that multiple transactions can frequently complete without interfering with each other. While running, transactions use data resources without acquiring locks on those resources. Before committing, each transaction verifies that no other transaction has modified the data it has read. If the check reveals conflicting modifications, the committing transaction rolls back and can be restarted.-----WIKI

>In this lab, we can think of a request from a client as a transaction executed in the namenode which only modifies the structure of name node file tree. So imitating the OCC, I divide every function into 4 phase: Begin, Modify, Commit, End(Abort).

> - Begin: The begin phase indicates the commencement of a transaction. For example, openreadwrite, the client will get a copy of target filenode for following operations without interrupting other clients' operations.
> - Modify: The modification of the copy
> - Commit: Completeing all modifications, the validation of the transaction will be decided. If invalid, the copy will be aborted, otherwise, it will be commited. So this implementation is a kind of all-or-nothing.
> - End: If valid, the copy will be writen into memory. At last, the phase will transmit to End.

> All these phases are marked as a object(Command.java) enqueued into concurrent queue when triggering a phase. And every time we find a patch of complete transaction phases(Begin->End or Begin->Abort), all of them will be dequeued from the commands.

- Correctness Proof 
> optimistic concurrency control implemented by thread-safe data structure can ensure that every operation is thread-safe.

## Muti-Clients Solution ##
- When a client open or create a file, he will receive a copy of the file node. So any operations on this node actually is executed on one of the copy. So client can read or write without concerning about other clients.
- Because of copies, we introduce consistency problems. Multi-clients who want to read is permitted, while multi-write is forbidden. So I maintain a confict concurrent hash map in name node and every time the same file is opened as read-write twice, Overlapping Exception will be thrown.
- Due to obtaining a copy of the file node, every write operations is invisable to those readers who has opened a file before. Besides, only when the writer has closed the file which means every operation is updated in namenode and datanode, following readers can see the changes.
- BTW, Because I use OCC, a fine-grained multi-writers is possible. I could allow more than one writers to modify the same file node. When committing, we could check if any conflicts happen. It writers modify two seperate blocks, then modifications are enabled.

## Problems ##
- problem: Non-locks thread-safe function implementation

> Solution: I could use a thread-safe command queue to represent the sequence of different clients. With the help of sequence, conflicts will be solved.

- problem: baffled with logic of copyonwrite procedure
> My interpretation: user writes data into channel cache. If the data was flushed into datanode, datanode will send a copyonwrite request to name node and then received a new located block mapping from name node. From then on, every operations will be exerted on new block, former blocks will be removed 

## Extra Work ##
- Optimistic Concurrency Control.
- Thread-Safe delete operation.
- Thread-Safe rename operation.
- Garbage Collection. The Slave server will run a timetasker and every 2000s the replicated block files will be purged from the data node.

## Log Design ##
- format: 
> Considering the log stored in the disk is not only readable to program but also to adminstrators, so I translate every log object into string which will be saved in the form of jason. Therefore, logs can be legible to human.

- writing process:

> I divide operations into two kind of type, immediate operation and delaying operation. Such an immediate operation contains, say, mkdir, renam, rm which take operations into effect immediately. In contrary, writestart, for example, has less operation in global data structure. 
>
1. If such a operation is a delaying one (write start), I adopt a lazy-write strategy pushing it into a concurrent queue. If the operation is immediate, judge whether it can pass validating phase in optimistic concurrency control. If pass, then write all of logs in queue into disk. (Imlemented using dispathLog() in NameNode.java) 
2. When a flushdisk operation is occured, a CheckPointLog would be writen into disk. While, you can classify it into either of two categories.


- recovery process:
> To improve the performance, I use only one time reading of log.

> 
1. If the log is instance of mkdir log( or other immediate operations), add it into immediate recovery queue. Or if the log is instance of read or write log, then push it into a delaying recovery queue which is solely marked by file access token.
2. If the log is instance of check point log, then purge clear up every immediate recovery queue and part of delaying queue where the write commit log or write abort log has been pushed into the queue.
3. If the reading process comes to the end, then recover every operation survived in the queue in turn.

## Flush Name Node Design ##
In the requirement document, the flushdataintodisk is a consistent interval which is fixed before running actual program. So, If within a long time, there is no one requesting any operations, then the waste of flushing is unnecessary. Enlighted by the scheduling of TCP RTT, I design a kind of adaptive timer which can tinker up the flush disk interval according to request numbers.

- elapse

> I hope that it would be better that every k(coefficient) operations will trigger a flush operation. So elapse = (endTime - startTime) / size which represents SampleInterval.

- flushDiskInternalSeconds

> flushDiskInternalSeconds is a estimated interval which is a expected flushing interval.
> 
> flushDiskInternalSeconds = (int) ((1 - Constants.ALPHA) * flushDiskInternalSeconds + Constants.ALPHA * elapse); In that, ALPHA = 0,125

- DEV
> DEV depicts a variance of different requesting interval. With the help of that variable, the interval could adjust itself automatically.

>DEV = (long) ((1 - Constants.BETA) * DEV + Constants.BETA * Math.abs(elapse - flushDiskInternalSeconds)); In that, BETA = 0.25. 

- interval
> Actual interval set into timer schedualing which decided when next flushing operation will take place.

> interval = flushDiskInternalSeconds + 4 * DEV;

Notice: All constants above is a heuristic practice. Maybe other choice is better. Who knows?

## Profiling ##
- tool: jprofiler_windows-x64_9_2.exe
- test specification: overview, Call_Tree, Hot_Spots, Thread_History
- test: ClientTest, NameNodeServerTest, DataNodeServerTest
- results: the results are saved in directories named by according test.

### analysis ###
Take NameNodeServerTest as an example 

![](http://i.imgur.com/njEkOhK.png)

![](http://i.imgur.com/Dvrbmpw.png)

![](http://i.imgur.com/0VYgqH7.png)

As profiling results shown in the pictures, the main overhead is caused by logger which takes up mojarity of time writing logs and flushing data into disk. To mitigate that callapse, I design that logging mechanism delaying and batching as more logs as possible. In addition, I contrive an adaptive flushing strategy to reduce unnecessary writing. By the way, the printState which only prints file node tree overhead is avoidable. 

To improve performance further, I put forward two solutions:
1. replace objecter mapper with a simpler saving method. Because translating an object to jason and writing it to disk is expensive, so I should utilize a raw method to save objects.

2. I could sacrifice some correctness to implement a less log writing file system. For example, we could regard every operation as delaying operations, so we have ability of batching all of these logs and write them to the disk only once.

## Problems ##
- Problem: When I tested all cases in the same main function, I appallingly found that the environment variable was out of work, which means in the logTest, files in former tests were copied into this test case mistakenly. However, if I run the logTest individually, it can pass successfully. So my cods logic is non-erronous.
> Assumption: I have no idea about that problem, but I think one of possible presupposition is the effect of bugs in intellij.
> 

- Problem: When I write log objects into file, java throws EOF exception and AC exception
> Solution: I spent a lot of time trying to figure out what hell is going on. After attempting many methods, I failed to tackle with that. So I translate every object into jason and finally it does work.

- Problem: In this lab, I try to reduce three part of reading and writing: log writing, log reading, flushing
> Solution: I did it by contriving three read/write schemes.

- Problem: Timer in java seems not to change the interval once the schedule function is called.
> Solution: destroy a former one and create a new timer. So in my profiling, the number of new threads is increasing.

## Change ##
- source code:
	- I modify the NameNodeDataNodeProtocal to support heart beat sending 

- test code:(The modified test codes are in src/test/java/sdfs)
	- setupSpec()
	- fc.fileNode.blockAmount->fc.getBlockAmount()
	- every block size is represented as Constants.DEFAULT_BLOCK_SIZE
	- Client level copy on write in DataNodeServerTest is nonsense to me, so I remove it
	- In LogTest, the last sentence is modified to "new File(dir4, "namenode.log").size() == 0" because I write jason string to files without appending header(4 bytes)


## Extra work ##
- Flush disk scheduling(more details in #flush name node design#)
- reading and recovering logs using batching strategy(more details in #log design#)
- gabbage collection
> The Slave server will run a timetasker in order that after every 2000s, replicated block files will be purged from the data node.)

- token consistent
> In my design, the client can not notice a happening crash of the name node which means the token obtained in an early time is available after the recovering of name node, which provides a user-friendly service.

- Thread-Safe delete operation.
- Thread-Safe rename operation.
- Optimistic Concurrency Control.
> In the first checkpoint, I mention that because I use Optimistic Concurrency Control to assure the correctness of different transactions, multi-writers transaction is possible if they modify different blocks. Now I implement more concurrent opernreadwrite and closereadwrite functions as con_openReadwrite and con_closeReadwriteFile. In these two funtions, I redesign commit protocal to make sure the true conflict can be detected.

- heart beat
> Every init of data node will accompany a sending message informing the name node that I am alive. After every half an hour, the name node will check the data node list to remove those ones who don't send heat beat.(Specific implementation is in DataNodeList.java)