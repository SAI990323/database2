/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb
{

BufMgr::BufMgr(std::uint32_t bufs)
		: numBufs(bufs)
{
	bufDescTable = new BufDesc[bufs];

	for (FrameId i = 0; i < bufs; i++)
	{
		bufDescTable[i].frameNo = i;
		bufDescTable[i].valid = false;
	}

	bufPool = new Page[bufs];

	int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
	hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

	clockHand = bufs - 1;
}
BufMgr::~BufMgr()
{
	// flushes out all dirty pages
	for (FrameId i = 0; i < numBufs; i++) {
		if (bufDescTable[i].dirty && bufDescTable[i].valid)
		{
			bufDescTable[i].file->writePage(bufPool[i]);
			bufDescTable[i].Clear();
		}
	}

	//deallocates the buffer pool
	delete[] bufPool;

	//deallocates the BufDesc table
	delete[] bufDescTable;
}

void BufMgr::advanceClock()
{
	clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId &frame)
{
	std::int64_t pinned = 0;
	advanceClock();

	while (bufDescTable[clockHand].valid)
	{
		if (bufDescTable[clockHand].refbit)
		{
			bufDescTable[clockHand].refbit = false;
			advanceClock();
			continue;
		}

		if (bufDescTable[clockHand].pinCnt)
		{
			pinned++;
			if (pinned == numBufs)
			{
				throw BufferExceededException();
			}
			advanceClock();
			continue;
		}

		if (bufDescTable[clockHand].dirty)
		{
			bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
			bufDescTable[clockHand].dirty = false;
		}

		hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
		break;
	}

	bufDescTable[clockHand].Clear();
	frame = clockHand;
}

void BufMgr::readPage(File *file, const PageId pageNo, Page *&page)
{
	FrameId frameId;
	try
	{
		hashTable->lookup(file, pageNo, frameId);
		bufDescTable[frameId].refbit = true;
		bufDescTable[frameId].pinCnt++;
	}
	catch (HashNotFoundException e)
	{
		allocBuf(frameId);

		Page newPage = file->readPage(pageNo);
		hashTable->insert(file, pageNo, frameId);
		bufPool[frameId] = newPage;
		bufDescTable[frameId].Set(file, pageNo);
	}

	page = &bufPool[frameId];
}

void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
{
	try
	{
		FrameId frameId;
		hashTable->lookup(file, pageNo, frameId);

		if (bufDescTable[frameId].pinCnt == 0)
		{
			throw PageNotPinnedException(file->filename(), pageNo, frameId);
		}
		
		bufDescTable[frameId].pinCnt--;

		if (dirty)
		{
			bufDescTable[frameId].dirty = true;
		}
	}
	catch (HashNotFoundException e)
	{
	}
}

void BufMgr::flushFile(const File *file)
{
	for (FrameId i = 0; i < numBufs; i++)
	{
		if (bufDescTable[i].file == file)
		{

			if (bufDescTable[i].pinCnt)
			{
				throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, i);
			}

			if (!bufDescTable[i].valid)
			{
				throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
			}

			if (bufDescTable[i].dirty)
			{
				bufDescTable[i].file->writePage(bufPool[i]);
				bufDescTable[i].dirty = false;
			}

			hashTable->remove(file, bufDescTable[i].pageNo);

			bufDescTable[i].Clear();
		}
	}
}

void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page)
{
	Page newPage = file->allocatePage();

	FrameId frameId;
	allocBuf(frameId);

	pageNo = newPage.page_number();
	hashTable->insert(file, pageNo, frameId);
	bufPool[frameId] = newPage;
	bufDescTable[frameId].Set(file, pageNo);

	page = &bufPool[frameId];
}

void BufMgr::disposePage(File *file, const PageId PageNo)
{
	try
	{
		FrameId frameId;

		hashTable->lookup(file, PageNo, frameId);
		hashTable->remove(file, PageNo);
		bufDescTable[frameId].Clear();
	}
	catch (HashNotFoundException e)
	{
	}

	file->deletePage(PageNo);
}

void BufMgr::printSelf(void)
{
	BufDesc *tmpbuf;
	int validFrames = 0;

	for (std::uint32_t i = 0; i < numBufs; i++)
	{
		tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

		if (tmpbuf->valid == true)
			validFrames++;
	}

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

} // namespace badgerdb
