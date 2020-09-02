#include <regex>
#include <mutex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>
#include <utime.h>

#include "src/dfs-utils.h"
#include "src/dfslibx-clientnode-p2.h"
#include "dfslib-shared-p2.h"
#include "dfslib-clientnode-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;

using namespace std;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using to indicate
// a file request and a listing of files from the server.
//
//using FileRequestType = FileRequest;
//using FileListResponseType = FileList;
using FileRequestType = dfs_service::FileRequest;
using FileListResponseType = dfs_service::FileMap;

DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {}
DFSClientNodeP2::~DFSClientNodeP2() {}

void DFSClientNodeP2::loadMountFiles()
{
    std::lock_guard<std::mutex> lock(Function2);
   //make a table of all files in hte mount dir.
    //load all files in mount_dir
    if (DEBUG) cout<<"loading files from mount dir "<<mount_path<<endl;
    cout<<"wrapped path"<<WrapPath("mypath")<<endl;

    cout<<"some more text"<<endl;
    
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
                thisStatus.set_ctime((fileStatus.st_ctim).tv_sec);
  
                thisStatus.set_crc(dfs_file_checksum(buffer, &crc_table));
                
                  thisStatus.set_todelete(true);
                thisStatus.set_filename(fileInfo->d_name);
                
                thisStatus.set_ownerclientid(ClientId());
                
                thisStatus.set_size(fileStatus.st_size);
           //                        fileTable.mutable_mapval()->insert(pair<string, dfs_service::FileStatus>(fileInfo->d_name, thisStatus));
              
//                {
//                    std::lock_guard<std::mutex> lock(writeAccessMutex);
//                writeAccessMutex.lock();
                (*fileTable.mutable_mapval())[fileInfo->d_name].CopyFrom(thisStatus);
//                writeAccessMutex.unlock();
//                }
                //                        fileTable[fileInfo->d_name] = thisStatus;
                
    //                        fileTable[fileInfo->d_name] = FileAttr(fdes, fileStatus.st_size, (fileStatus.st_mtim).tv_sec, (fileStatus.st_ctim).tv_sec);
                        }
                    }
                    closedir(folder);
                }
}





bool DFSClientNodeP2::isServerDelete(const string & filename)
{
    dfs_service::FileRequest request;
    request.set_name(filename);
    dfs_service::FileStatus response;
    
    ::grpc::ClientContext context;
        //create deadline
      std::chrono::system_clock::time_point deadline =
      std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
      context.set_deadline(deadline);


    ::grpc::Status resultStatus = service_stub->getServerStatus(&context,request,&response);
    
    if (resultStatus.error_code() == StatusCode::OK)
    {
        return(response.todelete());
    }
    return false;
    
}

grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename) {
    std::lock_guard<std::mutex> lock(Function2);
    
//    RequestWriteLockMutex.lock();

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to obtain a write lock here when trying to store a file.
    // This method should request a write lock for the given file at the server,
    // so that the current client becomes the sole creator/writer. If the server
    // responds with a RESOURCE_EXHAUSTED response, the client should cancel
    // the current file storage
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
    /*
     1. if the entry is not in client, create a new one with filname.
     */
    
//    string shortClientID = ClientId().substr(24,3);
    string shortClientID = ClientId();
    
    if (DEBUG) cout<<std::this_thread::get_id()<<" request Write Access "<<filename<<" id "<<shortClientID<<endl;
    ::grpc::ClientContext context;
    //create deadline
      std::chrono::system_clock::time_point deadline =
      std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
      context.set_deadline(deadline);
    
    ::dfs_service::AccessRequest request;
    ::dfs_service::Access response;
    ::dfs_service::FileStatus requestStatus;
    request.set_clientid(ClientId());
    
    
//        std::lock_guard<std::mutex> lock(writeAccessMutex);
//        writeAccessMutex.lock();
    auto itr = fileTable.mutable_mapval()->find(filename);
    if(itr == fileTable.mutable_mapval()->end())
    {
        //not with client
        ::dfs_service::FileStatus newFileStatus;
        newFileStatus.set_ownerclientid(ClientId());
        newFileStatus.set_filename(filename);
        newFileStatus.set_todelete(true);
        newFileStatus.set_crc(0);
        newFileStatus.set_mtime(0);
        newFileStatus.set_fdes(0);
        requestStatus.CopyFrom(newFileStatus);
        
    }
    else
    {
        //entry already exists.
        requestStatus.CopyFrom(itr->second);
    }
       
    

//    request.set_allocated_fstatval(&requestStatus);
    request.mutable_fstatval()->CopyFrom(requestStatus);
    
      
    ::grpc::Status resultStatus = service_stub->RequestWriteLock(&context,request,&response);
        
//           writeAccessMutex.unlock();
    
    
//    RequestWriteLockMutex.unlock();
    return resultStatus.error_code();
    
}

bool DFSClientNodeP2::ExistsInServer(const string &filename)
{
    std::lock_guard<std::mutex> lock(Function2);
//    ExistsinServerMutex.lock();
    ::grpc::ClientContext context;
    //create deadline
      std::chrono::system_clock::time_point deadline =
      std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
      context.set_deadline(deadline);
    
    ::dfs_service::FileStatus request;
//    {
//        std::lock_guard<std::mutex> lock(writeAccessMutex);
//        request = (*fileTable.mutable_mapval())[filename];
//    writeAccessMutex.lock();
    request.CopyFrom((*fileTable.mutable_mapval())[filename]);
//    writeAccessMutex.unlock();
//    }
    
    ::dfs_service::Access response;
    
    auto result = service_stub->alreadyExists(&context,request, &response);
//    ExistsinServerMutex.unlock();
    return response.accessval();
}

grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename) {
    std::lock_guard<std::mutex> lock(Function1);

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // stored is the same on the server (i.e. the ALREADY_EXISTS gRPC response).
    //
    // You will also need to add a request for a write lock before attempting to store.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::ALREADY_EXISTS - if the local cached file has not changed from the server version
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
    
    if (!loadedFromMountStatus)
    {
        loadMountFiles();
        loadedFromMountStatus = true;
    }
    
    cout<<std::this_thread::get_id()<<" 1 store "<<filename<<endl;
    /*
     1. create Context and response message pointer.
     2. set teh deadline
     3. obtain the client writer.
     4. open the file
     5. send the file size.
     6. successively use the writer to send the values out.
     7. once done, observe the status of the return value without concerning about the response.
     */
    
    ::grpc::ClientContext context;
    //create deadline
    std::chrono::system_clock::time_point deadline =
    std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(deadline);
    
    //create response msg
    dfs_service::Msg response;
    dfs_service::Data fileData;
    
    //open the file
    string fullPath = WrapPath(filename);
//    cout<<"1 file "<<filename<<" path "<<fullPath<<endl;
    int fdes = open(fullPath.c_str(),O_RDONLY,0664);
        //handline file not found.
    if (fdes < 0)
    {
       if (DEBUG) cout<<std::this_thread::get_id()<<" 1 file not found"<<endl;
        return StatusCode::NOT_FOUND;
    }
    struct stat fileStat;
    fstat(fdes, &fileStat);
     size_t fileSize = fileStat.st_size;
    
    //check if file is on the filetable.
    {
//        std::lock_guard<std::mutex> lock(writeAccessMutex);
//        writeAccessMutex.lock();
     auto itr = fileTable.mutable_mapval()->find(filename);
     if (itr == fileTable.mutable_mapval()->end())
     {
         if (DEBUG) cout<<std::this_thread::get_id()<<" 1 creating new entry in fileTable for "<<filename<<endl;
       dfs_service::FileStatus thisStatus;
       thisStatus.set_fdes(fdes);
       thisStatus.set_mtime((fileStat.st_mtim).tv_sec);
       thisStatus.set_ctime((fileStat.st_ctim).tv_sec);

       thisStatus.set_filename(filename);
       thisStatus.set_crc(dfs_file_checksum(fullPath, &crc_table));

       thisStatus.set_size(fileStat.st_size);
       thisStatus.set_todelete(true);
                       
       thisStatus.set_ownerclientid(ClientId());
   (*fileTable.mutable_mapval())[filename].CopyFrom(thisStatus);
     }
     else
     {
         if (DEBUG) cout<<std::this_thread::get_id()<<" 1 previously existed filename "<<filename<<endl;
         dfs_service::FileStatus thisStatus;
         thisStatus.CopyFrom((*fileTable.mutable_mapval())[filename]);
         thisStatus.set_crc(dfs_file_checksum(fullPath, &crc_table));
         thisStatus.set_mtime((fileStat.st_mtim).tv_sec);
         thisStatus.set_ctime((fileStat.st_ctim).tv_sec);
         thisStatus.set_fdes(fdes);
         thisStatus.set_size(fileStat.st_size);
         thisStatus.set_todelete(true);
         thisStatus.set_ownerclientid(ClientId());
         
    (*fileTable.mutable_mapval())[filename].CopyFrom(thisStatus);
     }
//        writeAccessMutex.unlock();
}
    
    
    
    
    //check if already exists
    if (ExistsInServer(filename))
    {
       if (DEBUG) cout<<std::this_thread::get_id()<<" 1 same file as exists "<<filename<<endl;
        return StatusCode::ALREADY_EXISTS;
    }
    
    if (DEBUG) cout<<std::this_thread::get_id()<<" 1 waiting to get the lock "<<filename<<endl;
    
    //get file lock
     ::grpc::StatusCode fileAccessStatus = RequestWriteAccess(filename);
     
    (*fileTable.mutable_mapval())[filename].set_todelete(true);
     
     if(fileAccessStatus == StatusCode::DEADLINE_EXCEEDED)
     {
         cout<<"1 Store "<<filename<<" deadline exceeded"<<endl;
         return StatusCode::DEADLINE_EXCEEDED;
     }
     
     if(fileAccessStatus == StatusCode::RESOURCE_EXHAUSTED)
    {
         cout<<"1 Store "<<filename<<" recource exhausted"<<endl;
         return StatusCode::RESOURCE_EXHAUSTED;
     }
     
     if(fileAccessStatus == StatusCode::CANCELLED)
     {
         cout<<"1 Store "<<filename<<" cancelled"<<endl;
         return StatusCode::CANCELLED;
     }
     cout<<std::this_thread::get_id()<<" 1 got the lock "<<filename<<endl;
    
    
    //create clinet writer.
      std::unique_ptr< ::grpc::ClientWriter< ::dfs_service::Data>> writer = service_stub->storeFile(&context,&response);
    
    //send the filename to the server
    fileData.set_dataval(filename);
    writer->Write(fileData);
    
        //send the mtime.
    //    {
    //    writeAccessMutex.lock();
    //   std::lock_guard<std::mutex> lock(writeAccessMutex);
        //send mtime
           fileData.set_dataval(to_string((*fileTable.mutable_mapval())[filename].mtime()));
           //    writeAccessMutex.unlock();
           //    }
               writer->Write(fileData);
    
    //get filesize and send to server
    fileData.set_dataval(to_string(fileSize));
    writer->Write(fileData);
    
    //start sending file in chunks
    size_t leftBytes = fileSize;
    size_t sentTotal = 0;
    size_t thisChunk = BUFF_SIZE;
    size_t sentNow = 0;
    char buffer[BUFF_SIZE];
    memset(buffer, 0, BUFF_SIZE);
    cout<<"1 starting to store "<<filename<<endl;
    while(leftBytes > 0)
    {
        thisChunk = BUFF_SIZE;
        if (leftBytes < BUFF_SIZE)
        {
            thisChunk = leftBytes;
        }
        sentNow = pread(fdes, buffer, thisChunk, sentTotal);
        fileData.set_dataval(buffer, sentNow);
        writer->Write(fileData);
        //update counters
        leftBytes -= sentNow;
        sentTotal += sentNow;
        memset(buffer, 0, BUFF_SIZE);
//        cout<<"1 "<<filename<<" write left "<<leftBytes<<endl;
    }
    cout<<"1 done storing "<<filename<<endl;

  
    
    Status serverStatus = writer->Finish();
    
    
    if(serverStatus.error_code() == StatusCode::OK)
    {
//        if(ExistsInServer(filename))
//        {
//           if (DEBUG) cout<<std::this_thread::get_id()<<" 1 verified with server"<<endl;
//        }
//        writeAccessMutex.lock();
//        (*fileTable.mutable_mapval())[filename].set_todelete(true);
//        writeAccessMutex.unlock();
       if (DEBUG) cout<<std::this_thread::get_id()<<" 1 finished storing "<<filename<<endl;
        return StatusCode::OK;
    }
    else if(serverStatus.error_code() == StatusCode::DEADLINE_EXCEEDED)
    {
       if (DEBUG) cout<<"1 Deadline exceeded "<<filename<<endl;
        return StatusCode::DEADLINE_EXCEEDED;
    }
    else if(serverStatus.error_code() == StatusCode::CANCELLED)
    {
        cout<<"1 server already has a later version "<<filename<<endl;
    }
    
    if (DEBUG) cout<<std::this_thread::get_id()<<" 1 Cancelled "<<filename<<endl;
    return StatusCode::CANCELLED;

}


grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename) {
    
    std::lock_guard<std::mutex> lock(Function1);

    
    if (!loadedFromMountStatus)
    {
        loadMountFiles();
        loadedFromMountStatus = true;
    }

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // fetched is the same on the client (i.e. the files do not differ
    // between the client and server and a fetch would be unnecessary.
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // DEADLINE_EXCEEDED - if the deadline timeout occurs
    // NOT_FOUND - if the file cannot be found on the server
    // ALREADY_EXISTS - if the local cached file has not changed from the server version
    // CANCELLED otherwise
    //
    // Hint: You may want to match the mtime on local files to the server's mtime
    //
    
    /*
     1. check if the file is with the clinet.
     1.2. if so, check if its CRC matches with that of the Servers using an rpc call. if so, return already exists.
     2. if not, create a temp entry. don't add to the filetable.
     3. set the filename and send it over with the clientid variable.
     4.
     
     */
    cout<<"2 fetch "<<filename<<endl;
//    {
//    std::lock_guard<std::mutex> lock(writeAccessMutex);
//    writeAccessMutex.lock();
    
    auto itr = (fileTable.mutable_mapval())->find(filename);
    
    bool exists = true;
    
    if(isServerDelete(filename))
    {
        if (DEBUG) cout<<"2 deleted file in fetch"<<filename<<endl;
    
                return StatusCode::CANCELLED;
    }
    
    if(itr == (fileTable.mutable_mapval())->end())
    {
        //check if it is due to inotify or callback. if inotify, the file already exists, jsut need to add it to teh table.
        
       
        if ( FILE * clientFile = fopen(WrapPath(filename).c_str(), "r"))
        {
            exists = true;
            fclose(clientFile);
            int fdes = open(WrapPath(filename).c_str(),O_WRONLY|O_CREAT,0644);
            dfs_service::FileStatus thisStatus;
            cout<<"2 already exists "<<filename<<endl;
            struct stat fileStat;
            fstat(fdes,&fileStat);
            
            thisStatus.set_filename(filename);
            thisStatus.set_ctime((fileStat.st_ctim).tv_sec);
            thisStatus.set_mtime((fileStat.st_mtim).tv_sec);
            thisStatus.set_size(fileStat.st_size);
            thisStatus.set_fdes(fdes);
            thisStatus.set_ownerclientid(ClientId());
            thisStatus.set_todelete(true);
       
            close(fdes);
        thisStatus.set_crc(dfs_file_checksum(WrapPath(filename), &crc_table));
            
        (*fileTable.mutable_mapval())[filename].CopyFrom(thisStatus);
         
            cout<<"2 loading existing file "<<filename<<endl;
            
        }
        else
        {
            exists = false;
            dfs_service::FileStatus thisStatus;
            thisStatus.set_todelete(true);
            thisStatus.set_crc(0);
            thisStatus.set_ownerclientid(ClientId());
            thisStatus.set_size(0);
            thisStatus.set_filename(filename);
        (*fileTable.mutable_mapval())[filename].CopyFrom(thisStatus);
        }
    }
    else
    {
        (itr->second).set_todelete(true);
        exists = true;
    }

 
//        writeAccessMutex.unlock();
        //entry in the client
        //check that its not hte same.
        if(ExistsInServer(filename))
        {
           if (DEBUG) cout<<"2 existing unmodified file "<<filename<<endl;
            
            return StatusCode::ALREADY_EXISTS;
        }
        else{
           if (DEBUG) cout<<"2 existing modified file "<<filename<<endl;
        }
    
    
    //get file lock
     ::grpc::StatusCode fileAccessStatus = RequestWriteAccess(filename);
     
    (*fileTable.mutable_mapval())[filename].set_todelete(true);
     
     if(fileAccessStatus == StatusCode::DEADLINE_EXCEEDED)
     {
         cout<<"2 Store "<<filename<<" deadline exceeded"<<endl;
         return StatusCode::DEADLINE_EXCEEDED;
     }
     
     if(fileAccessStatus == StatusCode::RESOURCE_EXHAUSTED)
    {
         cout<<"2 Store "<<filename<<" recource exhausted"<<endl;
         return StatusCode::RESOURCE_EXHAUSTED;
     }
     
     if(fileAccessStatus == StatusCode::CANCELLED)
     {
         cout<<"2 Store "<<filename<<" cancelled"<<endl;
         return StatusCode::CANCELLED;
     }
     cout<<std::this_thread::get_id()<<"2 got the lock "<<filename<<endl;
    
//    }
    
    /*
     1. use the stub to call the server and get the clientReader.
     2. Check if the writer is still present to see if the file is found. if not, return.
     3. create a file and start receiving the message and storing in the file.
     */
    //create reader
    unique_ptr<::grpc::ClientReader<::dfs_service::Data>> reader;
    //create context and return request.
    ::grpc::ClientContext clientContext;
    //create deadline
    std::chrono::system_clock::time_point deadline =
    std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    clientContext.set_deadline(deadline);
    
    //message to request file
    ::dfs_service::Msg request;
    
    //fill in the file name into the request
    request.set_msgval(filename);
    
    //send request to server by calling the stub and get the reader.
    reader = service_stub->fetchFile(&clientContext,request);
    
    //use the reader to read into a local data variable
    ::dfs_service::Data fileData;
    
    // Read the file size. if -1, didn't find.
    reader->Read(&fileData);
    
    long long int fileSize = stoi(fileData.dataval());
    if (fileSize == -1)
    {
       if (DEBUG) cout<<"2 file not found"<<endl;
//        writeAccessMutex.lock();
        (*fileTable.mutable_mapval()).erase(filename);
//        writeAccessMutex.unlock();

        return StatusCode::NOT_FOUND;
    }
    else if(fileSize == -2)
    {
       if (DEBUG) cout<<"2 deleted file "<<filename<<endl;
//        writeAccessMutex.lock();
        (*fileTable.mutable_mapval()).erase(filename);
//        writeAccessMutex.unlock();
        return StatusCode::CANCELLED;
    }
    //receive the data into a buffer and write it into a file.
        //create a complete file path with the mount
    string wholeName = WrapPath(filename);
    char buffer[BUFF_SIZE];
    if (exists)
    remove(wholeName.c_str());
    int fdes = open(wholeName.c_str(),O_WRONLY|O_CREAT,0644);
    
    cout<<"2 file path "<<wholeName<<endl;
    //transfer to file
    size_t bytesLeft = fileSize;
    size_t bytesTransferred = 0;
    size_t chunkSize = BUFF_SIZE;
    
    while(reader->Read(&fileData))
          {
              //set chunk size
              chunkSize = BUFF_SIZE;
              if (bytesLeft < BUFF_SIZE)
              {
                  chunkSize = bytesLeft;
              }
              memset(buffer, 0, BUFF_SIZE);
              memcpy(buffer, fileData.dataval().c_str(), BUFF_SIZE);
              
              //write it into the file
              size_t thisTransferBytes = pwrite(fdes, buffer, chunkSize, bytesTransferred);
              
              //update counters
              bytesTransferred += thisTransferBytes;
              bytesLeft -= thisTransferBytes;
    }
    
    reader->Read(&fileData);
    
    long long int thisMtime = stoi(fileData.dataval());
   
    Status serverStatus =  reader->Finish();
    if (serverStatus.error_code() == StatusCode::OK)
    {
        //its all ok.
        //add it to the map
        struct stat fileStat;
        fstat(fdes, &fileStat);
        ::dfs_service::FileStatus newFileStatus;
        newFileStatus.set_filename(filename);
        newFileStatus.set_ctime((fileStat.st_ctim).tv_sec);
        newFileStatus.set_mtime(thisMtime);
        uint32_t crcRes = dfs_file_checksum(wholeName, &crc_table);
        newFileStatus.set_crc(crcRes);
        newFileStatus.set_size(fileStat.st_size);
        newFileStatus.set_fdes(fdes);
        newFileStatus.set_ownerclientid(ClientId());
        newFileStatus.set_todelete(true);
        
        {
//       std::lock_guard<std::mutex> lock(writeAccessMutex);
//            writeAccessMutex.lock();
            (*fileTable.mutable_mapval())[filename].CopyFrom(newFileStatus);
//            writeAccessMutex.unlock();
        }
        //close the reader.
           close(fdes);
        cout<<"2 done reading "<<filename<<endl;
        return StatusCode::OK;
    }
    else if(serverStatus.error_code() == StatusCode::DEADLINE_EXCEEDED)
    {
        if(DEBUG) cout<<"2 Deadline exceeded"<<filename<<endl;
        //close the reader.
           close(fdes);
        return StatusCode::DEADLINE_EXCEEDED;
    }
    
    //close the reader.
       close(fdes);
    return StatusCode::CANCELLED;
}

grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename) {
    std::lock_guard<std::mutex> lock(Function1);

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You will also need to add a request for a write lock before attempting to delete.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    
    if (!loadedFromMountStatus)
    {
        loadMountFiles();
        loadedFromMountStatus = true;
    }
    cout<<"3 Delete "<<filename<<endl;
    bool exists = false;
    
    auto temp_itr = fileTable.mutable_mapval()->find(filename);
    if(temp_itr == fileTable.mutable_mapval()->end())
    {
        cout<<"3 no file to be deleted "<<filename<<endl;
        return StatusCode::CANCELLED;
    }
    
    else if( FILE * clientFile = fopen(WrapPath(filename).c_str(), "r"))
    {
        cout<<"3 file exists "<<endl;
        exists = true;
        fclose(clientFile);
        if((temp_itr->second).todelete())
          {
//              (temp_itr->second).set_todelete(false);
              cout<<"3 no file to be deleted "<<filename<<endl;
              return StatusCode::CANCELLED;
          }
    }
   
    
    //acquire lock
    //get file lock
   if (DEBUG) cout<<"3 requesting lock "<<filename<<endl;
    ::grpc::StatusCode fileAccessStatus = RequestWriteAccess(filename);
    
    if(fileAccessStatus == StatusCode::DEADLINE_EXCEEDED)
    
    {
      if (DEBUG)  cout<<"3 deadline exceeded "<<filename<<endl;
        return StatusCode::DEADLINE_EXCEEDED;
    }
    
    if(fileAccessStatus == StatusCode::RESOURCE_EXHAUSTED)
    {
       if (DEBUG) cout<<"3 resource exhausted "<<filename<<endl;
    return StatusCode::RESOURCE_EXHAUSTED;
    }
    
    if(fileAccessStatus == StatusCode::CANCELLED)
    {
       if (DEBUG) cout<<"3 cancelled "<<filename<<endl;
    return StatusCode::CANCELLED;
    }
    
   if (DEBUG) cout<<"3 acquired lock "<<filename<<endl;
    /*
     send the file to be deleted.
     1. create stub which returns the status. look at the status. thats. it.
     
     */
    //create context.
    ::grpc::ClientContext clientContext;
    //create deadline
    std::chrono::system_clock::time_point deadline =
    std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    clientContext.set_deadline(deadline);
    
    //Create request
    dfs_service::Msg request;
    request.set_msgval(filename);
    
    //response
    dfs_service::Msg response;
    
    //call method
    Status serverStatus = service_stub->deleteFile(&clientContext,request,&response);
    
    //delete if exists
    if (exists)
    remove(WrapPath(filename).c_str());
    
    //delete from fileTable
//    writeAccessMutex.lock();
    auto itr = fileTable.mutable_mapval()->find(filename);
    if(itr != fileTable.mutable_mapval()->end())
    (*fileTable.mutable_mapval()).erase(filename);
//    writeAccessMutex.unlock();
    
    //OK, NOTFOund, deadline and other
    if (serverStatus.error_code() == StatusCode::OK)
    {
        if(DEBUG) cout<<"3 status ok "<<filename<<endl;
        return StatusCode::OK;
    }
    else if (serverStatus.error_code() == StatusCode::NOT_FOUND)
    {
        if(DEBUG) cout<<"3 status not found"<<filename<<endl;
        return StatusCode::NOT_FOUND;
    }
    else if (serverStatus.error_code() == StatusCode::DEADLINE_EXCEEDED)
    {
        if(DEBUG) cout<<"3 status deadline exceeded"<<filename<<endl;
        return StatusCode::DEADLINE_EXCEEDED;
    }
    
    cout<<"3 delete done "<<filename<<endl;
    return StatusCode::CANCELLED;

}

grpc::StatusCode DFSClientNodeP2::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list files here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // listing details that would be useful to your solution to the list response.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //
    if (DEBUG) cout<<"List "<<endl;
    return StatusCode::OK;
}

grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // status details that would be useful to your solution.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //
    if (DEBUG) cout<<"Stat "<<endl;
     return StatusCode::OK;
}

void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback) {

    //
    // STUDENT INSTRUCTION:
    //
    // This method gets called each time inotify signals a change
    // to a file on the file system. That is every time a file is
    // modified or created.
    //
    // You may want to consider how this section will affect
    // concurrent actions between the inotify watcher and the
    // asynchronous callbacks associated with the server.
    //
    // The callback method shown must be called here, but you may surround it with
    // whatever structures you feel are necessary to ensure proper coordination
    // between the async and watcher threads.
    //
    // Hint: how can you prevent race conditions between this thread and
    // the async thread when a file event has been signaled?
    //

   
    
    {
        std::lock_guard<std::mutex> lock(Function3);
//        std::lock_guard<std::mutex> lock(functionMutex);
        if (!loadedFromMountStatus)
           {
               loadMountFiles();
               loadedFromMountStatus = true;
           }
//        std::lock_guard<std::mutex> lock(writeAccessMutex);
//        functionMutex.lock();
         if (DEBUG) cout<<"Inotify callback"<<endl;
    callback();
//        functionMutex.unlock();
    }

}

//
// STUDENT INSTRUCTION:
//
// This method handles the gRPC asynchronous callbacks from the server.
// We've provided the base structure for you, but you should review
// the hints provided in the STUDENT INSTRUCTION sections below
// in order to complete this method.
//
void DFSClientNodeP2::HandleCallbackList() {
    
    if (!loadedFromMountStatus)
    {
        loadMountFiles();
        loadedFromMountStatus = true;
    }
    
    if(DEBUG) cout<<"Handle Callback List"<<endl;

    void* tag;

    bool ok = false;

    //
    // STUDENT INSTRUCTION:
    //
    // Add your file list synchronization code here.
    //
    // When the server responds to an asynchronous request for the CallbackList,
    // this method is called. You should then synchronize the
    // files between the server and the client based on the goals
    // described in the readme.
    //
    // In addition to synchronizing the files, you'll also need to ensure
    // that the async thread and the file watcher thread are cooperating. These
    // two threads could easily get into a race condition where both are trying
    // to write or fetch over top of each other. So, you'll need to determine
    // what type of locking/guarding is necessary to ensure the threads are
    // properly coordinated.
    //

    // Block until the next result is available in the completion queue.
    while (completion_queue.Next(&tag, &ok)) {
        {
            //
            // STUDENT INSTRUCTION:
            //
            // Consider adding a critical section or RAII style lock here
            //
            std::lock_guard<std::mutex> lock(Function3);
            
//              std::lock_guard<std::mutex> lock(writeAccessMutex);
//            functionMutex.lock();
//            if(DEBUG) cout<<"Handle Callback List returned"<<endl;
            // The tag is the memory location of the call_data object
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            dfs_log(LL_DEBUG2) << "Received completion queue callback";

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            // GPR_ASSERT(ok);
            if (!ok) {
                dfs_log(LL_ERROR) << "Completion queue callback not ok.";
            }

            if (ok && call_data->status.ok()) {
                
                
                

                dfs_log(LL_DEBUG3) << "Handling async callback ";

                //
                // STUDENT INSTRUCTION:
                //
                // Add your handling of the asynchronous event calls here.
                // For example, based on the file listing returned from the server,
                // how should the client respond to this updated information?
                // Should it retrieve an updated version of the file?
                // Send an update to the server?
                // Do nothing?
                //
                
                auto itr = ((call_data->reply).mutable_mapval())->begin();
//                dfs_service::FileMap thisMap = (*call_data);
                /*
                 check the received map against clients.
                 if client didn't have an entry or if mtime of client is older than that in the received map, issue fetch.
                 if server didn't have an entry or if mtime of server is older than that of client, issue store.
                 if server wanted the clinet to delete a file, it should keep a table entry with a unique state.
                 */

                //for each element in server map, if there is no element or if mtime is less, fetch.
                int i = 1;
                while(itr != ((call_data->reply).mutable_mapval())->end())
                {
                    
//                    auto findInClient = (fileTable.mutable_mapval())->find(*(*itr).mutable_name()));
//                    cout<<i++<<" server entry "<<(itr->first)<<" deleted "<<(itr->second).todelete() << endl;
                    
//                    writeAccessMutex.lock();
                    auto findInClient = (fileTable.mutable_mapval())->find(itr->first);

                    //not found
                    if(findInClient == (fileTable.mutable_mapval())->end() )
                    {
//                        writeAccessMutex.unlock();
                        if( !(itr->second).todelete())
                        {
                        cout<<itr->first<<" async not found in client, need to fetch"<<endl;
                        
                        Fetch(itr->first);
                        }
                    }
                    
                    else{
                        
                        //found
                        //need to update server
                        
                        int clientCRC = (findInClient->second).crc();
                        int serverCRC = (itr->second).crc();
                        
                        //check for files to be deleted

                        if( (itr->second).todelete())
                        {
                           
                             //remove file from local dir
                            if(isServerDelete(itr->first))
                            {
                (findInClient->second).set_todelete(false);
                  string wholePath = WrapPath(itr->first);
                 cout<<"async removing file "<<(itr->first)<<" "<<wholePath<<endl;
//                  remove(wholePath.c_str());
//                  (*fileTable.mutable_mapval()).erase(itr->first);
//                writeAccessMutex.unlock();
                            Delete(itr->first);
                            }
                        }
                        
                         else if(clientCRC != serverCRC)
                        {
                        if (  (findInClient->second).mtime() > ((itr->second).mtime()))
                        {
                            cout<<" file "<<itr->first<<"async updating server with mtime "<< (itr->second).mtime() <<" clinet "<<(findInClient->second).mtime()<<endl;
//                            writeAccessMutex.unlock();
                            
                            Store(itr->first);
                            
                        }else if ((findInClient->second).mtime() < ((itr->second).mtime()))
                        {
                           
                   
                            
                            cout<<" file "<<itr->first<<"async updating client, server with mtime "<< (itr->second).mtime() <<" clinet "<<(findInClient->second).mtime()<<endl;
//                            writeAccessMutex.unlock();
                                Fetch(itr->first);
                        
                            
                        }
                        else if ((findInClient->second).mtime() == ((itr->second).mtime()))
                        {
                            //do nothing
//                            writeAccessMutex.unlock();
                        }
                    }
                        else if(clientCRC == serverCRC)
                        {
//                            cout<<"the file "<<itr->first<<" is same in both"<<endl;
//                            writeAccessMutex.unlock();
                        }
                    }
                    
                    itr++;
                }
                
                //need to look for entries in client which are not in the server
                
//                writeAccessMutex.lock();
                auto citr = (fileTable.mutable_mapval())->begin();
//                writeAccessMutex.unlock();
                if(citr != (fileTable.mutable_mapval())->end()){
                while(citr != (fileTable.mutable_mapval()->end()))
                {
                    //for the entries not present in server.
                    auto sitr = (call_data->reply).mutable_mapval()->find(citr->first);
                    if (sitr == ((call_data->reply).mutable_mapval()->end()))
                    {
                        cout<<"file "<<citr->first<<"async not found in server "<<endl;
                        Store(citr->first);
                    }
                    citr++;
                    
                }
                }
                

            } else {
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }

            // Once we're complete, deallocate the call_data object.
            delete call_data;

            //
            // STUDENT INSTRUCTION:
            //
            // Add any additional syncing/locking mechanisms you may need here
//            functionMutex.unlock();
        }


        // Start the process over and wait for the next callback response
        dfs_log(LL_DEBUG3) << "Calling InitCallbackList";
        InitCallbackList();

    }
}

/**
 * This method will start the callback request to the server, requesting
 * an update whenever the server sees that files have been modified.
 *
 * We're making use of a template function here, so that we can keep some
 * of the more intricate workings of the async process out of the way, and
 * give you a chance to focus more on the project's requirements.
 */
void DFSClientNodeP2::InitCallbackList() {
//    if(DEBUG) cout<<"call back list "<<endl;
    CallbackList<FileRequestType, FileListResponseType>();
}

//
// STUDENT INSTRUCTION:
//
// Add any additional code you need to here
//


