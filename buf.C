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


const Status BufMgr::allocBuf(int &frame) 
{
    bool frameFound = false;
    int numScanned = 0;

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
        bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &bufPool[clockHand]);
    }

    // Reset frame entry.
    bufTable[clockHand].Clear();

    // Return new frame.
    frame = clockHand;

    return OK;
}

  
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNumber = 0;

    // Check to see if the page is already in the buffer pool.
    if (hashTable->lookup(file, PageNo, frameNumber) != HASHNOTFOUND) {

        // Page is in buffer pool. Reference bit and pinCnt are updated.
        bufTable[frameNumber].refbit = true;
        bufTable[frameNumber].pinCnt++;
        page = &bufPool[frameNumber];
    }
    else {
        // Page is NOT in the buffer pool.

        // Allocate a new frame and get frameNumber.
        allocBuf(frameNumber);

        // Read page into the new frame created above.
        file->readPage(PageNo, page);
        bufPool[frameNumber] = *page;

        // Update the hash table.
        hashTable->insert(file, PageNo, frameNumber);

        // Properly setup this frame.
        bufTable[frameNumber].Set(file, PageNo);
    }

    return OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty) 
{
    int frameNumber = 0;
    // Retrieve page from the hash table. (in order to get frameNumber)
    // Throws HASHNOTFOUND if not found.
    hashTable->lookup(file, PageNo, frameNumber);

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

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    int frameNumber = 0;

    // Allocate an empty page in the specified file and return pageNo.
    file->allocatePage(pageNo);

    // Obtain a buffer pool frame and return frameNumber.
    allocBuf(frameNumber);

    // Insert entry into hash table.
    hashTable->insert(file, pageNo, frameNumber);

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
