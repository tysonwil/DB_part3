// Tianchu Hunang - 906 257 9744
// Tim Zodrow - 906 516 7760
// Tyson Williams - 906 352 9276
// The purpose of this file is to define the inner logic of the BufMgr
// function calls. 

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
           cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
         } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

// allocBuf takes a reference to a frame number and looks for an
// open frame in the buffer based on the valid bit, ref bit, and pinCnt.
// Returns the status of the function call, OK if it was successful, 
// BUFFEREXCEEDED if the buffer was full, ERROR status if error occured.

const Status BufMgr::allocBuf(int &frame) 
{
    bool frameFound = false;
    int numScanned = 0;
    Status s;

    // Start scanning for an open frame. We need to scan twice in case
    // if all frames have their reference bit set.
    while (numScanned < 2*numBufs) {
        advanceClock();
        numScanned++;

        // If the frame is not valid, we will use it.
        if (!bufTable[clockHand].valid) {
            frameFound = true;
            break;
        }

        // Check to see if page has been referenced recently.
        if (!bufTable[clockHand].refbit) {
            // Check to see if the page is pinned.
            if (bufTable[clockHand].pinCnt == 0) {
                // Page hasn't been referenced and is not pinned, so let's use it.
                frameFound = true;
                // Remove page from hash table.
                hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
                break;
            }
        }
        // refbit has been set, so it needs to be cleared.
        else
            bufTable[clockHand].refbit = false;
    }

    // If all pages are pinned, we have a full buffer pool.
    if (!frameFound)
        return BUFFEREXCEEDED;

    // If page is dirty, it needs to be written back to disk.
    if (bufTable[clockHand].dirty) {
        if((s = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &bufPool[clockHand])) != OK){
            return s;
        }
    }

    // Return new frame.
    frame = clockHand;

    return OK;
}

// readPage takes a file pointer, a page number, and a reference to a page.
// It will look to see if the page is in the Hash Table, otherwise it will
// call a read file on the file pointer and then store a reference to the 
// page in the page reference and return a status. If status is OK, it was
// successful, if ERROR occured, it will return that status error.
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNumber = 0;
    Status s;

    // Check to see if the page is already in the buffer pool.
    if (hashTable->lookup(file, PageNo, frameNumber) == OK) {

        // Page is in buffer pool. Reference bit and pinCnt are updated.
        bufTable[frameNumber].refbit = true;
        bufTable[frameNumber].pinCnt++;
        page = &bufPool[frameNumber];
    }
    else {
        // Page is NOT in the buffer pool.

        // Allocate a new frame and get frameNumber.
        if((s = allocBuf(frameNumber)) != OK){
            return s;
        }

        // Read page into the new frame created above.
        if((s = file->readPage(PageNo, &bufPool[frameNumber])) != OK){
            return s;
        }

        //bufPool[frameNumber] = *page;

        // Update the hash table.
        if((s = hashTable->insert(file, PageNo, frameNumber)) != OK){
            return s;
        }

        // Properly setup this frame.
        bufTable[frameNumber].Set(file, PageNo);
        page = &bufPool[frameNumber];
    }

    return OK;
}

// unPinPage will take a file pointer, a page number, and dirty bit.
// It will remove the page from the hash table and it will set the dirty
// bit on the buffer if the dirty bit was true and it will decrement the
// pin count on the buffer. If successful, it will return OK, if the pinCnt was
// 0, then PAGENOTPINNED will be returned, and on ERROR, it will return the error.
const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty) 
{
    int frameNumber = 0;
    Status s;
    // Retrieve page from the hash table. (in order to get frameNumber)
    // Throws HASHNOTFOUND if not found.
    if((s = hashTable->lookup(file, PageNo, frameNumber)) != OK){
        return s;
    }

    // Mark the page as dirty if necessary.
    if (dirty == true)
        bufTable[frameNumber].dirty = true;

    // Check to see if pin count is already 0. Return PAGENOTPINNED if so.
    if (bufTable[frameNumber].pinCnt == 0)
        return PAGENOTPINNED;
    else    // Otherwise decrement pin count.
        bufTable[frameNumber].pinCnt--;

    return OK;
}

// allocPage will take a file pointer, a reference to a page number, and
// a reference to a page pointer. It will allocate the page in the file,
// then it will create a buffer frame for the page, and insert it into 
// the hash table. It will set the page number and page reference as outputs.
// If successful, it will return OK, otherwise on ERROR it will return the 
// error status.
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    int frameNumber = 0;
    Status s;

    // Allocate an empty page in the specified file and return pageNo.
    if((s = file->allocatePage(pageNo)) != OK){
        return s;
    }

    // Obtain a buffer pool frame and return frameNumber.
    if((s = allocBuf(frameNumber)) != OK){
        return s;
    }

    // Insert entry into hash table.
    if((s = hashTable->insert(file, pageNo, frameNumber)) != OK){
        return s;
    }

    // Setup this new frame.
    bufTable[frameNumber].Set(file, pageNo);

    // Assign the page pointer.
    page = &bufPool[frameNumber];

    return OK;

}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {
        if (tmpbuf->pinCnt > 0)
            return PAGEPINNED;
        if (tmpbuf->dirty == true) {
            #ifdef DEBUGBUF
              cout << "flushing page " << tmpbuf->pageNo
                         << " from frame " << i << endl;
            #endif
            if ((status = tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]))) != OK)
                return status;
            tmpbuf->dirty = false;
        }

        hashTable->remove(file,tmpbuf->pageNo);

        tmpbuf->file = NULL;
        tmpbuf->pageNo = -1;
        tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
        return BADBUFFER;
    }

    return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
