# Distributed-File-System-gRPC
# Goal
To implement a distributed file system to synchronize files across multiple clients and a server using methods of STORE, FETCH, and DELETE implemented with gRPC
# Design
The architecture involves two mechanisms to update the files in the client. One involves the use of inotify library to detect changes to the file structure in a mounted folder. And the other involves the asynchronous transfer of file table from the server which the client compares with its own to request changes to perform the required changes.
At the server end, there are two threads running to service the client requests and also to asynchronously send the file table to the client.
# Flow of operation
The server initiates its service accessible through a requested address. It loads the existing files in the mounted directory and waits for client requests. The client loads the existing files onto its file table and requests the server to send its table. The server then sends its table asynchronously. The client also receives the inotifiy events signaling any change in the mounted directory. The client then accordingly requests the server either to fetch, store, or delete in order to synchronize the files. The server receives its requests from multiple clients which it will serve.
Fig1, Simple illustration of the different communication paths in the DFS system.
# Implementation and testing
The two threads in the client serving the inotify and the asynchronous callback can simultaneously issue a request. This is serialized using synchronization constructs like the guarded_lock. To ensure that the same data is not requested to be stored, fetched and deleted at the same time, these operations need to acquire the lock form the server which allows them to use the data exclusively till the end of its operation.
At the server side, multiple functions are made atomic to ensure that there is no overlap between them. Including the asynchronous request handler, write lock issue, and other methods.
The DFS system was tested to ensure that all the clients and server eventually reach synchronous state where all the files between them are the same.
# References
the implementation details of the asynchronous grpc were referred from the grpc site. Details on different thread lock schemes in c++ were also referenced.
