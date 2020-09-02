#ifndef PR4_DFSLIB_SHARED_H
#define PR4_DFSLIB_SHARED_H

#include <algorithm>
#include <cctype>
#include <locale>
#include <cstddef>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <thread>
#include <sys/stat.h>

#include<unordered_map>
#include<fcntl.h>
#include<dirent.h>
#include<sys/types.h>
#include<string>
#include<stdio.h>

#include "src/dfs-utils.h"
#include "proto-src/dfs-service.grpc.pb.h"

using namespace std;

//
// STUDENT INSTRUCTION
//
// The following defines and methods must be left as-is for the
// structural components provided to work.
//
#define DFS_RESET_TIMEOUT 3000
#define DFS_I_EVENT_SIZE (sizeof(struct inotify_event))
#define DFS_I_BUFFER_SIZE (1024 * (DFS_I_EVENT_SIZE + 16))

#define BUFF_SIZE 1024

/** A file descriptor type **/
typedef int FileDescriptor;

/** A watch descriptor type **/
typedef int WatchDescriptor;

/** An inotify callback method **/
typedef void (*InotifyCallback)(uint, const std::string&, void*);

/** The expected struct for the inotify setup in this project **/
struct NotifyStruct {
    FileDescriptor fd;
    WatchDescriptor wd;
    uint event_type;
    std::thread * thread;
    InotifyCallback callback;
};

/** The expected event type for the event method in this project **/
struct EventStruct {
    void* event;
    void* instance;
};

//
// STUDENT INSTRUCTION:
//
// Add any additional shared code here
//

class FileAttr
{
    //need file table
public:
    int fdes;
    int32_t size;
    int32_t mtime;
    int32_t ctime;
    FileAttr(int des, int32_t size, int32_t mtime, int32_t ctime):fdes(des),size(size),mtime(mtime),ctime(ctime){};
    
    FileAttr() {};
};


#define DEBUG 1

#endif

