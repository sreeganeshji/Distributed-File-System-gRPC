#include <map>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <dirent.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>

#include "proto-src/dfs-service.grpc.pb.h"
#include "src/dfslibx-call-data.h"
#include "src/dfslibx-service-runner.h"
#include "dfslib-shared-p2.h"
#include "dfslib-servernode-p2.h"

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

using dfs_service::DFSService;

using namespace std;
//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using in your `dfs-service.proto` file
// to indicate a file request and a listing of files from the server
//
//using FileRequestType = FileRequest;
//using FileListResponseType = FileList;
using FileRequestType = dfs_service::FileRequest;
using FileListResponseType = dfs_service::FileMap;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// As with Part 1, the DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in your `dfs-service.proto` file.
//
// You may start with your Part 1 implementations of each service method.
//
// Elements to consider for Part 2:
//
// - How will you implement the write lock at the server level?
// - How will you keep track of which client has a write lock for a file?
//      - Note that we've provided a preset client_id in DFSClientNode that generates
//        a client id for you. You can pass that to the server to identify the current client.
// - How will you release the write lock?
// - How will you handle a store request for a client that doesn't have a write lock?
// - When matching files to determine similarity, you should use the `file_checksum` method we've provided.
//      - Both the client and server have a pre-made `crc_table` variable to speed things up.
//      - Use the `file_checksum` method to compare two files, similar to the following:
//
//          std::uint32_t server_crc = dfs_file_checksum(filepath, &this->crc_table);
//
//      - Hint: as the crc checksum is a simple integer, you can pass it around inside your message types.
//
class DFSServiceImpl final :
    public DFSService::WithAsyncMethod_CallbackList<DFSService::Service>,
        public DFSCallDataManager<FileRequestType , FileListResponseType> {

private:

    /** The runner service used to start the service and manage asynchronicity **/
    DFSServiceRunner<FileRequestType, FileListResponseType> runner;

    /** The mount path for the server **/
    std::string mount_path;
            
    //mutex for writelock
    std::mutex writeAccessMutex;
            
    //mutex for atomic functions
            std::mutex atomicFnMutex;
            
            std::mutex alreadyExistsMutex;
            std::mutex requestLockMutex;
            
            //function mutex. for all functions
            std::mutex functionMutex;
            
            std::mutex function2;
            std::mutex function3;
  
    /** Mutex for managing the queue requests **/
    std::mutex queue_mutex;

    /** The vector of queued tags used to manage asynchronous requests **/
    std::vector<QueueRequest<FileRequestType, FileListResponseType>> queued_tags;


    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }

    /** CRC Table kept in memory for faster calculations **/
    CRC::Table<std::uint32_t, 32> crc_table;

public:
            dfs_service::FileMap fileTable;
//        std::unordered_map<string, FileAttr> fileTable;
            
    DFSServiceImpl(const std::string& mount_path, const std::string& server_address, int num_async_threads):
        mount_path(mount_path), crc_table(CRC::CRC_32()) {
            cout<<"constructing from thread "<<std::this_thread::get_id()<<endl;

        this->runner.SetService(this);
        this->runner.SetAddress(server_address);
        this->runner.SetNumThreads(num_async_threads);
        this->runner.SetQueuedRequestsCallback([&]{ this->ProcessQueuedRequests(); });

            //make a table of all files in hte mount dir.
            //load all files in mount_dir
            if (DEBUG) cout<<"loading files from mount dir "<<mount_path<<endl;
            
            struct dirent *fileInfo;
            struct stat fileStatus;
            int fdes;
            DIR *folder;
            if((folder = opendir(mount_path.c_str())) != NULL)
            {
                while((fileInfo = readdir(folder)) != NULL)
                {
                    if(fileInfo->d_type == DT_REG)
                    {
                        //found a file. add to fileTable.
                        char buffer[100];
                        memset(buffer,0,100);
                        strcpy(buffer, mount_path.c_str());
                        strcat(buffer, fileInfo->d_name);
                        fdes = open(buffer,O_RDWR,0644);
                        if(fdes<0)
                        {
                            cout<<"Couldn't open files"<<endl;
                            cout<<strerror(errno)<<endl;
                            closedir(folder);
                            return;
                        }
                        fstat(fdes, &fileStatus);
                        //add to map.
                        if(DEBUG) cout<<"loading "<<fileInfo->d_name<<" size: "<<fileStatus.st_size<<endl;
                        dfs_service::FileStatus thisStatus;
                        thisStatus.set_fdes(fdes);
                        thisStatus.set_mtime((fileStatus.st_mtim).tv_sec);
                        
                        thisStatus.set_filename(fileInfo->d_name);
                        thisStatus.set_ctime((fileStatus.st_ctim).tv_sec);
                        thisStatus.set_todelete(false);
                        
                        thisStatus.set_size(fileStatus.st_size);
                        thisStatus.set_crc(dfs_file_checksum(buffer, &this->crc_table));
                        
                        thisStatus.set_ownerclientid("");
     fileTable.mutable_mapval()->insert(google::protobuf::MapPair<string, dfs_service::FileStatus>(fileInfo->d_name, thisStatus));
//                        (*fileTable.mutable_mapval())[fileInfo->d_name] = thisStatus;
                        
//                        fileTable[fileInfo->d_name] = FileAttr(fdes, fileStatus.st_size, (fileStatus.st_mtim).tv_sec, (fileStatus.st_ctim).tv_sec);
                    }
                }
                closedir(folder);
            }
            
    }

    ~DFSServiceImpl() {
        this->runner.Shutdown();
    }

    void Run() {
        this->runner.Run();
    }

    /**
     * Request callback for asynchronous requests
     *
     * This method is called by the DFSCallData class during
     * an asynchronous request call from the client.
     *
     * Students should not need to adjust this.
     *
     * @param context
     * @param request
     * @param response
     * @param cq
     * @param tag
     */
    void RequestCallback(grpc::ServerContext* context,
                         FileRequestType* request,
                         grpc::ServerAsyncResponseWriter<FileListResponseType>* response,
                         grpc::ServerCompletionQueue* cq,
                         void* tag) {
        
//        cout<<"request callback "<<std::this_thread::get_id()<<endl;
//        cout<<"^";
        std::lock_guard<std::mutex> lock(queue_mutex);
        this->queued_tags.emplace_back(context, request, response, cq, tag);
//        if(DEBUG) cout<<"RequestCallback"<<*request->mutable_name()<<endl;
    }

    /**
     * Process a callback request
     *
     * This method is called by the DFSCallData class when
     * a requested callback can be processed. You should use this method
     * to manage the CallbackList RPC call and respond as needed.
     *
     * See the STUDENT INSTRUCTION for more details.
     *
     * @param context
     * @param request
     * @param response
     */
    void ProcessCallback(ServerContext* context, FileRequestType* request, FileListResponseType* response) {
        
//        cout<<"Process callback "<<std::this_thread::get_id()<<endl;

        //
        // STUDENT INSTRUCTION:
        //
        // You should add your code here to respond to any CallbackList requests from a client.
        // This function is called each time an asynchronous request is made from the client.
        //
        // The client should receive a list of files or modifications that represent the changes this service
        // is aware of. The client will then need to make the appropriate calls based on those changes.
        //
//        if (DEBUG) cout<<"ProcessCallback "<<*request->mutable_name()<<endl;
//        response = &fileTable;
     
        {
            std::lock_guard<std::mutex> lock(functionMutex);
        
//            atomicFnMutex.lock();
//            writeAccessMutex.lock();
            
//            cout<<"-";
            
//            auto itr = fileTable.mutable_mapval()->begin();
//            while(itr != fileTable.mutable_mapval()->end())
//            {
//                if((itr->second).todelete())
//                {
//                    cout<<"to delete "<<itr->first<<endl;
//                }
//                itr++;
//            }
            
        response->CopyFrom(fileTable);
//            writeAccessMutex.unlock();
//            atomicFnMutex.unlock();
            
           
            
        }
        
//        cout<<" sending response"<<endl;
        //send the file map
        
    }

    /**
     * Processes the queued requests in the queue thread
     */
    void ProcessQueuedRequests() {
        if(DEBUG) cout<<"ProcessQueuedRequests "<<std::this_thread::get_id()<<endl;
        while(true) {
            

            //
            // STUDENT INSTRUCTION:
            //
            // You should add any synchronization mechanisms you may need here in
            // addition to the queue management. For example, modified files checks.
            //
            // Note: you will need to leave the basic queue structure as-is, but you
            // may add any additional code you feel is necessary.
            //
//            std::lock_guard<std::mutex> lock(functionMutex);

            // Guarded section for queue
            {
//                atomicFnMutex.lock();
//                writeAccessMutex.lock();
//                cout<<"_";
                dfs_log(LL_DEBUG2) << "Waiting for queue guard";
                std::lock_guard<std::mutex> lock(queue_mutex);


                for(QueueRequest<FileRequestType, FileListResponseType>& queue_request : this->queued_tags) {
                    
                    
//                    if (DEBUG) cout<< queue_request.request-> name()<<endl;
                    
                    this->RequestCallbackList(queue_request.context, queue_request.request,
                        queue_request.response, queue_request.cq, queue_request.cq, queue_request.tag);
                    queue_request.finished = true;
                }

                // any finished tags first
                this->queued_tags.erase(std::remove_if(
                    this->queued_tags.begin(),
                    this->queued_tags.end(),
                    [](QueueRequest<FileRequestType, FileListResponseType>& queue_request) { return queue_request.finished; }
                ), this->queued_tags.end());

//                writeAccessMutex.unlock();
//                atomicFnMutex.unlock();
            }
//            usleep(10000);
        }
    }

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // the implementations of your rpc protocol methods.
    //

::grpc::Status getServerStatus(::grpc::ServerContext* context, const ::dfs_service::FileRequest* request, ::dfs_service::FileStatus* response) override
            {
                cout<<"get server status "<<request->name()<<endl;
                std::lock_guard<std::mutex> lock(functionMutex);
                auto itr = fileTable.mutable_mapval()->find(request->name());
                
                if(itr != fileTable.mutable_mapval()->end())
                {
                    dfs_service::FileStatus resList;
                    resList.CopyFrom((itr->second));
                    response->CopyFrom(resList);
                    return Status::OK;
                }
                
                return Status(StatusCode::NOT_FOUND,"not found");
                
            }
            
::grpc::Status alreadyExists(::grpc::ServerContext* context, const ::dfs_service::FileStatus* request, ::dfs_service::Access* response) override
            {
               
                
                std::lock_guard<std::mutex> lock(functionMutex);
                if (DEBUG) cout<<"already exists "<<std::this_thread::get_id()<<endl;
//
//                atomicFnMutex.lock();
//                cout<<"atom lock"<<endl;
//                 alreadyExistsMutex.lock();
//                cout<<"existlock"<<endl;
                //check for deadline
                if(context->IsCancelled())
                {
                    cout<<"Deadline Exceeded"<<endl;
                    return Status(StatusCode::DEADLINE_EXCEEDED,"Deadline exceeded");
                }
                
//                writeAccessMutex.lock();
//                cout<<"write access lock"<<endl;
                
                auto itr = (*fileTable.mutable_mapval()).find(request->filename());
                
                if(itr == (*fileTable.mutable_mapval()).end())
                {
                 dfs_service::Access res;
                 res.set_accessval(false);
                                 
                 response->CopyFrom(res);
//                 writeAccessMutex.unlock();
//                    cout<<"write access unlock"<<endl;
//
//                alreadyExistsMutex.unlock();
//                    cout<<"already exist unlock"<<endl;
//                    atomicFnMutex.unlock();
//                    cout<<"atom unlock"<<endl;
                 return Status(StatusCode::OK,"ok");
                }
                
                dfs_service::FileStatus serverCopy;
                
                    
              
                 serverCopy.CopyFrom((*fileTable.mutable_mapval())[request->filename()]) ;
                 
                uint32_t serverCRC = dfs_file_checksum(WrapPath(request->filename()), &crc_table);
                
                dfs_service::Access res;
               if (DEBUG) cout<<"comparing "<<request->filename()<<" server "<<serverCRC<<" client "<<(*request).crc()<<endl;
                if(!serverCopy.todelete() && (*request).crc() ==  serverCRC && (serverCRC != 0))
                {
                    //identical fileStatus
                   
                    res.set_accessval(true);
                }
                else{
                    res.set_accessval(false);
                }
                response->CopyFrom(res);
//                writeAccessMutex.unlock();
//                cout<<"write unlock"<<endl;
//                alreadyExistsMutex.unlock();
//                cout<<"already exist unlock"<<endl;
//                atomicFnMutex.unlock();
//                cout<<"atom unlock"<<endl;
                return Status(StatusCode::OK,"ok");
                
            }
            
            

::grpc::Status RequestWriteLock(::grpc::ServerContext* context, const ::dfs_service::AccessRequest* request, ::dfs_service::Access* response) override
            {
               if (DEBUG) cout<<"request write lock "<<std::this_thread::get_id()<<endl;
                std::lock_guard<std::mutex> lock(functionMutex);
                string shortClientID = request->clientid();
//                               shortClientID = shortClientID.substr(24,3);
//                cout<<"requesting requestLockMutex"<<endl;
//                atomicFnMutex.lock();
//                cout<<"atom lock"<<endl;
//                requestLockMutex.lock();
//                cout<<"request lock lock"<<endl;
            
//check for deadline
     if(context->IsCancelled())
     {
         cout<<"Deadline Exceeded"<<endl;
         return Status(StatusCode::DEADLINE_EXCEEDED,"Deadline exceeded");
     }
                
            
                
//                cout<<"writeAccessMutex.lock()"<<endl;
//                    writeAccessMutex.lock();
//                cout<<"write access lock"<<endl;
//                    cout<<"locked request write lock"<<endl;
                    
                    string filename = request->fstatval().filename();
                    
                    //find if the lock for this request is free.
                   auto itr = fileTable.mutable_mapval()->find(filename);
                    //if there is no such file, create a new entry
                    if (itr == fileTable.mutable_mapval()->end())
                    {
                        //no entry found
                      if (DEBUG)  cout<<"file "<<filename<<" not found "<<shortClientID<<endl;
//                        fileTable.mutable_mapval()->insert(google::protobuf::MapPair<string, dfs_service::FileStatus>(filename, request->fstatval()));
                        (*fileTable.mutable_mapval())[filename].set_ownerclientid(request->clientid());
                        
                        (*fileTable.mutable_mapval())[filename].set_filename(filename);
                        
                        (*fileTable.mutable_mapval())[filename].set_size(0);
                        
                        (*fileTable.mutable_mapval())[filename].set_mtime(0);
                        
                        (*fileTable.mutable_mapval())[filename].set_ctime(0);
                        
                        (*fileTable.mutable_mapval())[filename].set_ownerclientid(request->clientid());
            
                        (*fileTable.mutable_mapval())[filename].set_crc(0);
                        (*fileTable.mutable_mapval())[filename].set_todelete(false);
                        response->set_accessval(true);
                        //issue the lock
                       if (DEBUG) cout<<"lock issued for "<<filename<<" " <<shortClientID<<endl;
//                        cout<<"writeAccessMutex.unlock()"<<endl;
//                        writeAccessMutex.unlock();
//                        cout<<"write access unlock"<<endl;
////                        cout<<"requestLockMutex.unlock()"<<endl;
//                        requestLockMutex.unlock();
//                        cout<<"request lock unlock"<<endl;
//                        atomicFnMutex.unlock();
//                        cout<<"atom unlock"<<endl;
                        return Status(StatusCode::OK,"ok");
                        
                    }
                    //if file is found and the owner is empty, issue.
                    else
                    {
                        if((itr->second).ownerclientid() == "")
                        {
                            //issue the lock
                           if (DEBUG) cout<<"lock issued for "<<(itr->first)<<" "<<shortClientID <<endl;
                        (itr->second).set_ownerclientid(request->clientid());
//                        (itr->second).set_todelete(false);
                            response->set_accessval(true);
//                            cout<<"writeAccessMutex.unlock()"<<endl;
//                            writeAccessMutex.unlock();
//                            cout<<"write access unlock"<<endl;
////                            cout<<"requestLockMutex.unlock()"<<endl;
//                            requestLockMutex.unlock();
//                            cout<<"request lock unlock"<<endl;
//                            atomicFnMutex.unlock();
//                            cout<<"atom unlock"<<endl;
                            return Status(StatusCode::OK,"ok");
                            
                        }
                        else if((itr->second).ownerclientid() == request->clientid())
                        {
//                            cout<<"lock matching client id "<< (itr->first)<<endl;
                            response->set_accessval(true);
//                            cout<<"writeAccessMutex.unlock()"<<endl;
//                           writeAccessMutex.unlock();
//                        cout<<"write access unlock"<<endl;
////                            cout<<"requestLockMutex.unlock()"<<endl;
//                        requestLockMutex.unlock();
//                        cout<<"request lock unlock"<<endl;
//                        atomicFnMutex.unlock();
//                        cout<<"atom unlock"<<endl;
                            (itr->second).set_todelete(false);
                            return Status(StatusCode::OK,"ok");
                        }
                        else
                        {
                            //lock not issued
                           if (DEBUG) cout<<"lock not issued for "<<(itr->first)<<" "<<shortClientID<<endl;
                        response->set_accessval(false);
                            
//                            writeAccessMutex.unlock();
//                            cout<<"write access unlock"<<endl;
////                            cout<<"requestLockMutex.unlock()"<<endl;
//                            requestLockMutex.unlock();
//                            cout<<"request lock unlock"<<endl;
//                            atomicFnMutex.unlock();
//                            cout<<"atom unlock"<<endl;
                            return Status(StatusCode::RESOURCE_EXHAUSTED,"resource exhausted");
                        }
                    }
                  
//                    cout<<"unlocked request write lock"<<endl;
                
//writeAccessMutex.unlock();
                            cout<<"write access unlock"<<endl;
//                            cout<<"requestLockMutex.unlock()"<<endl;
//                            requestLockMutex.unlock();
//                            cout<<"request lock unlock"<<endl;
//                            atomicFnMutex.unlock();
//                            cout<<"atom unlock"<<endl;
//                cout<<"lock cancelled"<<endl;
                return Status(StatusCode::CANCELLED,"cancelled");
            }
            


Status storeFile(::grpc::ServerContext* context, ::grpc::ServerReader< ::dfs_service::Data>* reader, ::dfs_service::Msg* response) override{
    
    cout<<"1 store file "<<std::this_thread::get_id()<<endl;
    
    std::lock_guard<std::mutex> lock(functionMutex);
    cout<<"1 got store lock "<<std::this_thread::get_id()<<endl;
    
//    atomicFnMutex.lock();
//    cout<<"1 atom lock"<<endl;
    
    //see make a method for already exists.
    
    
    /*
     1. use the reader to read the file name and file size.
     2. create a file of that name.
     3. successively read and store the data into the file.
     4. in parallel, check that the deadline is not exceeded.
     
     */
    //reading fileName and Size
    string fileName;
    size_t fileSize;
    dfs_service::Data fileData;
    reader->Read(&fileData);
    fileName = fileData.dataval();
    cout<<"1 store file "<<fileName<<endl;
    
    
    //getmtime
    int thisMtime = 0;
    reader->Read(&fileData);
    thisMtime = stoi(fileData.dataval());
    
    auto tableItr = fileTable.mutable_mapval()->find(fileName);
    if(tableItr != fileTable.mutable_mapval()->end())
    {
        int serverMtime = (tableItr->second).mtime();
        if(serverMtime > thisMtime)
        {
            cout<<"1 server has later version "<<fileName<<endl;
            Status(StatusCode::CANCELLED,"already exists");
        }
        
        (tableItr->second).set_todelete(false);
    }
    
    
  
    //get filesize
    reader->Read(&fileData);
    fileSize = stoi(fileData.dataval());
    
    //create a file of that name
    string fullPath = WrapPath(fileName);
    remove(fullPath.c_str());
    int fdes = open(fullPath.c_str(),O_CREAT|O_WRONLY, 0664);
    cout<<"1 file path "<<fullPath<<endl;
    struct stat fileStat;
    
    //write into the file
    char buffer[BUFF_SIZE];
    size_t totalWrite = 0;
    size_t writeLeft = fileSize;
    size_t thisChunk = BUFF_SIZE;
    size_t nowWrite = 0;
    memset(buffer, 0, BUFF_SIZE);
    cout<<"1 starting store"<<fileName<<endl;
    while(writeLeft > 0)
    {
        //check for deadline
        if(context->IsCancelled())
        {
            cout<<"Deadline Exceeded"<<endl;
//            writeAccessMutex.lock();
//            cout<<"1 write access lock"<<endl;
            (*fileTable.mutable_mapval())[fileName].set_ownerclientid("");
//            writeAccessMutex.unlock();
//            cout<<"1 write access unlock"<<endl;
//            atomicFnMutex.unlock();
//            cout<<"1 atom unlock"<<endl;
            return Status(StatusCode::DEADLINE_EXCEEDED,"Deadline exceeded");
        }
        thisChunk = BUFF_SIZE;
        if (writeLeft < BUFF_SIZE)
        {
            thisChunk = writeLeft;
        }
        reader->Read(&fileData);
        memcpy(buffer, fileData.dataval().c_str(), thisChunk);
        nowWrite = pwrite(fdes, buffer, thisChunk, totalWrite);
//        cout<<"wrote "<<nowWrite<<endl;;
        
        //update the counters
        totalWrite += nowWrite;
        writeLeft -= nowWrite;
//        cout<<"1 "<<fileName<<" write left "<<writeLeft<<endl;
    }
    

    
    
       //add entry to fileTable
    fstat(fdes, &fileStat);
    
//     writeAccessMutex.lock();
    
    dfs_service::FileStatus currentListing;
    
    currentListing.CopyFrom((*fileTable.mutable_mapval())[fileName]);
    
   
    
    
    currentListing.set_mtime(thisMtime);
    currentListing.set_ctime((fileStat.st_ctim).tv_sec);
    currentListing.set_ownerclientid("");
    currentListing.set_todelete(false);
    currentListing.set_size(fileSize);
    currentListing.set_fdes(fdes);
    currentListing.set_filename(fileName);
   
    close(fdes);
    cout<<"1 almost done storing"<<endl;
   currentListing.set_crc(dfs_file_checksum(fullPath, &crc_table));

    (*fileTable.mutable_mapval())[fileName].CopyFrom(currentListing);
//        writeAccessMutex.unlock();
//    cout<<"1 write access unlock"<<endl;
        
    
    cout<<"1 done storing "<<fileName<<endl;
    
//    fileTable[fileName] = FileElement(fdes, fileStat);
//    atomicFnMutex.unlock();
//    cout<<"1 atom unlock"<<endl;
    return Status::OK;
    
}

::grpc::Status fetchFile(::grpc::ServerContext* context, const ::dfs_service::Msg* request, ::grpc::ServerWriter< ::dfs_service::Data>* writer) override
{
  if (DEBUG)  cout<<"Fetch file "<<std::this_thread::get_id()<<endl;
    std::lock_guard<std::mutex> lock(functionMutex);
//    atomicFnMutex.lock();
//    cout<<"2 atom lock"<<endl;
    cout<<"2 fetch "<<request->msgval()<<endl;
    /*
     the message will have the filename.
     1. get the fd
     2. open the file.
     3. read it into a buffer in chuncks and send it successively through the writer.
     4. Finish() the writer
     5. handle FILE_NOT_FOUND, DEADLINE_EXCEEDED and other faults.
     */
    ::dfs_service::Data fileData;
    
//    unordered_map<string, FileElement>::iterator fileItr = fileTable.find(request->msgval());
    ::google::protobuf::Map< ::std::string, ::dfs_service::FileStatus >::iterator fileItr;
//    {
//        std::lock_guard<std::mutex> lock(writeAccessMutex);
//    writeAccessMutex.lock();
//    cout<<"2 write access lock"<<endl;
     fileItr = (*fileTable.mutable_mapval()).find(request->msgval());
    
    if (fileItr == (*fileTable.mutable_mapval()).end())
    {
        //send -1 for the size
        fileData.set_dataval(to_string(-1));
        writer->Write(fileData);
        
       if (DEBUG) cout<<"2 no file found "<<request->msgval()<<endl;
//        writeAccessMutex.unlock();
//        cout<<"2 write access unlock"<<endl;
//        atomicFnMutex.unlock();
//        cout<<"2 atom unlock"<<endl;
        return Status(StatusCode(StatusCode::NOT_FOUND),"file not found");
    }
    else if (((fileItr)->second).todelete())
    {
       if (DEBUG) cout<<"2 file "<<request->msgval()<<" is deleted "<<endl;
        //send -2 for the size
        fileData.set_dataval(to_string(-2));
        writer->Write(fileData);
        
//        writeAccessMutex.unlock();
//        cout<<"2 write access unlock"<<endl;
//        atomicFnMutex.unlock();
//        cout<<"2 atom unlock"<<endl;
        
        return Status(StatusCode(StatusCode::CANCELLED),"INVALID");
    }
//    writeAccessMutex.unlock();
//    cout<<"2 write access unlock"<<endl;
   
//    }
    dfs_service::FileStatus thisFile(fileItr->second);

//        create buffer
    char buffer[BUFF_SIZE];
//        find file size
    size_t fileSize = thisFile.size();
    
//        send this to the client
    fileData.set_dataval(to_string(fileSize));
    writer->Write(fileData);
    
//        use the writer to write these chunks
    size_t totalWritten = 0;
    size_t leftBytes = fileSize;
    size_t thisChunk = BUFF_SIZE;
    string fullPath = WrapPath(thisFile.filename());
    int fdes = open(fullPath.c_str(),O_RDONLY,0664);
    cout<<"2 starting to send "<<thisFile.filename()<<endl;
    while(totalWritten < fileSize)
    {
        //check for deadline
        if(context->IsCancelled())
           {
              if (DEBUG) cout<<"timeout error"<<endl;
//               atomicFnMutex.unlock();
//               cout<<"2 atom unlock"<<endl;
              (*fileTable.mutable_mapval())[request->msgval()].set_ownerclientid("");
               
              (*fileTable.mutable_mapval())[request->msgval()].set_todelete(false);
               return(Status(StatusCode::DEADLINE_EXCEEDED,"timeout"));
           }
        
        //set the chunk size;
        thisChunk = BUFF_SIZE;
        
        if (leftBytes < BUFF_SIZE)
        {
            thisChunk = leftBytes;
        }
        memset(buffer, 0, BUFF_SIZE);
        int thisWriteBytes = pread(fdes, buffer, thisChunk, totalWritten);
        fileData.set_dataval(buffer, thisChunk);
        writer->Write(fileData);
        
        //update counters
        totalWritten += thisWriteBytes;
        leftBytes -= thisWriteBytes;
    }
    
    //send mtime
    fileData.set_dataval(to_string((*fileTable.mutable_mapval())[request->msgval()].mtime()));
    writer->Write(fileData);
   if (DEBUG) cout<<"2 finished fetching "<<request->msgval()<<endl;
    close(fdes);
//    atomicFnMutex.unlock();
//    cout<<"2 atom unlock"<<endl;
    (*fileTable.mutable_mapval())[request->msgval()].set_ownerclientid("");
    (*fileTable.mutable_mapval())[request->msgval()].set_todelete(false);
  
    return Status::OK;
}

//delete file
Status deleteFile(::grpc::ServerContext* context, const ::dfs_service::Msg* request, ::dfs_service::Msg* response) override
{
   if (DEBUG) cout<<"Delete file "<<std::this_thread::get_id()<<endl;
    std::lock_guard<std::mutex> lock(functionMutex);
//    atomicFnMutex.lock();
//    cout<<"3 atom lock"<<endl;
    /*
     1. use the request to identify the file in the table.
     1.1 if not, send not found
     2. add the mount path to the file name adn remove it.
     3. remove its entry in the file table.
     4. send ok
     */
//    unordered_map<string, FileElement>::iterator fileItr;
    //find the requested file
//    writeAccessMutex.lock();
//    cout<<"3 write access lock"<<endl;
   if (DEBUG) cout<<"3 delete "<<request->msgval()<<endl;
    auto fileItr = (*fileTable.mutable_mapval()).find(request->msgval());
    if(fileItr == (*fileTable.mutable_mapval()).end())
    {
        //file not found.
        if (DEBUG) cout<<"3 File not found "<<request->msgval()<<endl;
        (*fileTable.mutable_mapval())[(request->msgval())].set_ownerclientid("");
//        writeAccessMutex.unlock();
//        cout<<"3 write access unlock"<<endl;
//        atomicFnMutex.unlock();
//        cout<<"3 atom unlock"<<endl;
        return Status(StatusCode::NOT_FOUND, "FILE NOT FOUND");
    }
    else if((fileItr->second).todelete())
    {
        //already deleted
       if (DEBUG) cout<<"3 Already deleted "<<request->msgval()<<endl;
        (*fileTable.mutable_mapval())[(request->msgval())].set_ownerclientid("");
//        writeAccessMutex.unlock();
//        cout<<"3 write access unlock"<<endl;
//        atomicFnMutex.unlock();
//        cout<<"3 atom unlock"<<endl;
        return(Status::OK);
    }
    //create the whole path
    string wholePath = WrapPath(request->msgval());
   if (DEBUG) cout<<"3 file path "<<wholePath<<endl;
    
    //delete the file
    remove(wholePath.c_str());
    
    //delete entry from table.
//    (*fileTable.mutable_mapval()).erase(request->msgval());
    
    //mark as deleted.
   
   (*fileTable.mutable_mapval())[(request->msgval())].set_crc(0);
    (*fileTable.mutable_mapval())[(request->msgval())].set_todelete(true);
    (*fileTable.mutable_mapval())[(request->msgval())].set_ownerclientid("");
   if (DEBUG) cout<<"3 delete done "<<request->msgval()<<endl;
//    writeAccessMutex.unlock();
//    cout<<"3 write access unlock"<<endl;
//    atomicFnMutex.unlock();
//    cout<<"3 atom unlock"<<endl;
    return Status::OK;
}


};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly
// to add additional startup/shutdown routines inside, but be aware that
// the basic structure should stay the same as the testing environment
// will be expected this structure.
//
/**
 * The main server node constructor
 *
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        int num_async_threads,
        std::function<void()> callback) :
        server_address(server_address),
        mount_path(mount_path),
        num_async_threads(num_async_threads),
        grader_callback(callback) {}
/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
}

/**
 * Start the DFSServerNode server
 */
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path, this->server_address, this->num_async_threads);


    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    service.Run();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional definitions here
//
