#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define RC_QUEUE_IS_EMPTY 5;
#define RC_NO_FREE_BUFFER_ERROR 6;

SM_FileHandle *filehandler;
Queue *queue;
int readIO,writeIO;


RC pinPageLRU(BM_BufferPool * const bm, BM_PageHandle * const page,const PageNumber pageNum);  //pins the page with page number with LRU replacement strategy
RC pinPageFIFO(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum); // pins the page with page number with FIFO replacement strategy


//creating a queue for frame list
void createQueue(BM_BufferPool *const bm)
{
	pageInfo *newPage[bm->numPages];
	int lastPage = (bm->numPages) - 1;
	int n = 0;
	while (n <= lastPage){
		newPage[n] = (pageInfo*) malloc(sizeof(pageInfo));
		n++;
	}
	n = 0;
	while (n <= lastPage){

		(*newPage[n]).frameNum = n;
		(*newPage[n]).isDirty = 0;
		(*newPage[n]).fixCount = 0;
		(*newPage[n]).pageNum = -1;
		newPage[n]->bufferData = (char*) calloc(PAGE_SIZE, sizeof(char));
		n++;
	}
	int i = 0;
	while(i <= lastPage){
		int flag = 0;
		if(flag == 0)
		{
			int n = i;

			if((n!=lastPage) && (n!=0)){
				newPage[n]->nextPageInfo = newPage[n + 1];
				newPage[n]->prevPageInfo = newPage[n - 1];
			}
			else{
				newPage[n]->prevPageInfo = (n==0) ? NULL : newPage[n-1];
				newPage[n]->nextPageInfo = (n==0) ? newPage[n+1] : NULL;

			}
			
		}
		
		i++;
	}
	(*queue).head = newPage[0];
	(*queue).tail = newPage[lastPage];
	(*queue).filledframes = 0;
	(*queue).totalNumOfFrames = (*bm).numPages;
}

//emptying the queue
// RC emptyQueue() {return (queue->filledframes == 0);}

//creating the new list for page
// pageInfo* createNewList(const PageNumber pageNum) 
// {
// 	pageInfo* newpinfo = (pageInfo*) malloc(sizeof(pageInfo));
// 	char *c = (char*) calloc(PAGE_SIZE, sizeof(char));

// 	newpinfo->pageNum = pageNum;
// 	newpinfo->isDirty = 0;
// 	newpinfo->frameNum = 0;
// 	newpinfo->fixCount = 1;
// 	newpinfo->bufferData = c;
// 	newpinfo->prevPageInfo=NULL;
// 	newpinfo->nextPageInfo = NULL;
	
// 	return newpinfo;
// }
//dequeing the framelist
RC deQueue() 
{
	pageInfo *p = queue->head;
	int i = 0;
	while (i < queue->filledframes){
		if (i != (queue->filledframes-1))
		{
			p = p->nextPageInfo;
		} 
		else
		{
			queue->tail = p;
		}
		i++;
	}
	

	int tail_pnum; 
	int pageDelete=0;
	pageInfo *pinfo = queue->tail;
	i = 0;
	while(i < queue->totalNumOfFrames){
		if ((pinfo->fixCount) != 0)
		{

			tail_pnum=pinfo->pageNum;
			pinfo = pinfo->prevPageInfo;

		}
		else 
		{
			
				pageDelete = pinfo->pageNum;
			
			
			if (pinfo->pageNum != queue->tail->pageNum)
			{
				
				pinfo->prevPageInfo->nextPageInfo = pinfo->nextPageInfo;
				pinfo->nextPageInfo->prevPageInfo = pinfo->prevPageInfo;
				
			}
			else
			{
				
				queue->tail = (queue->tail->prevPageInfo);
				queue->tail->nextPageInfo = NULL;
			}
		}
		i++;
	}

	if (tail_pnum == queue->tail->pageNum)
	{	
		return 0;		
	}



	switch(pinfo->isDirty == 1){
		case true: 	writeBlock(pinfo->pageNum, filehandler, pinfo->bufferData);	
					writeIO++;
					break;
	}
		queue->filledframes--;
		return pageDelete;
}

//enqueing the page frame
RC Enqueue(BM_PageHandle * const page, const PageNumber pageNum,BM_BufferPool * const bm) 
{

	pageInfo* pinfo = (pageInfo*) malloc(sizeof(pageInfo));
	char *c = (char*) calloc(PAGE_SIZE, sizeof(char));
	int pageDelete=-1;
	if (queue->filledframes == queue->totalNumOfFrames ) { //If frames are full remove a page
		pageDelete=deQueue();
	}

		(*pinfo).pageNum = pageNum;
		(*pinfo).isDirty = 0;
		(*pinfo).frameNum = 0;
		(*pinfo).fixCount = 1;
		(*pinfo).bufferData = c;
		(*pinfo).prevPageInfo=NULL;
		(*pinfo).nextPageInfo = NULL;

	if ((*queue).filledframes == 0) {

			readBlock((*pinfo).pageNum,filehandler,(*pinfo).bufferData);
			(*page).data = (*pinfo).bufferData;
			readIO++;

			(*pinfo).frameNum = queue->head->frameNum;
			(*pinfo).nextPageInfo = queue->head;
			queue->head->prevPageInfo = pinfo;
			(*pinfo).pageNum = pageNum;
			(*page).pageNum= pageNum;
			(*queue).head = pinfo;
			

		} else {  
			readBlock(pageNum, filehandler, (*pinfo).bufferData);
			switch (pageDelete)
			{
			case -1:
				pinfo->frameNum = queue->head->frameNum+1;
				break;
			
			default:
				pinfo->frameNum=pageDelete;
				break;
			}
			
			(*page).data = (*pinfo).bufferData;
			readIO++;
			(*pinfo).nextPageInfo = queue->head;
			queue->head->prevPageInfo = pinfo;
			queue->head = pinfo;
			(*page).pageNum= pageNum;
			

		}
		(*queue).filledframes++;

		return RC_OK;
}

//pins the page with page number with LRU replacement strategy
RC pinPageLRU(BM_BufferPool * const bm, BM_PageHandle * const page,const PageNumber pageNum)
{

	pageInfo *pinfo = queue->head;
	int pageFound = 0;
	int i = 0;
	//for the provided page number here we are finding the node       
	while(i < bm->numPages){
		switch (pageFound)
		{
		case 0:
			if (pinfo->pageNum == pageNum)
			{
				pageFound = 1;
				break;
			}
			else
			{
				pinfo = pinfo->nextPageInfo;
			}
				
			break;
	
		}
		i++;
	}
	
	switch (pageFound)
	{
	case 0:
		Enqueue(page,pageNum,bm);
		break;
	
	case 1:
		
		pinfo->fixCount++;
		page->data = pinfo->bufferData;
		page->pageNum=pageNum;


		if(pinfo != queue->head){

			pinfo->prevPageInfo->nextPageInfo = pinfo->nextPageInfo;
		}else {
			pinfo->nextPageInfo = queue->head;
			queue->head->prevPageInfo = pinfo;
			queue->head = pinfo;
		}

		if(pinfo != queue->head && pinfo->nextPageInfo){
			pinfo->nextPageInfo->prevPageInfo = pinfo->prevPageInfo;
			if(pinfo != queue->tail){
				pinfo->nextPageInfo = queue->head;
				pinfo->prevPageInfo = NULL;
				pinfo->nextPageInfo->prevPageInfo = pinfo;
				queue->head = pinfo;
			}
			else{
				(*queue).tail = (*pinfo).prevPageInfo;
				queue->tail->nextPageInfo = NULL;
				(*pinfo).nextPageInfo = queue->head;
				(*pinfo).prevPageInfo = NULL;
				(*pinfo).nextPageInfo->prevPageInfo = pinfo;
				(*queue).head = pinfo;
			}
		
			
}
		break;
	}
	
	
	return RC_OK;
}

//pins the page with page number
RC pinPage(BM_BufferPool * const bm, BM_PageHandle * const page,const PageNumber pageNum)
{
	int res;

	res = (bm->strategy != RS_FIFO) ? pinPageLRU(bm,page,pageNum):pinPageFIFO(bm,page,pageNum);
	
	return res;
}

//
// void updateBM_BufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy)
// {

// 	 char* buffersize = (char *)calloc(numPages,sizeof(char)*PAGE_SIZE);

// 	   bm->pageFile = (char *)pageFileName;
// 	   bm->numPages = numPages;
// 	   bm->strategy = strategy;
// 	   bm->mgmtData = buffersize;
	
// }

//creates a new buffer pool with numPages page frames using the page replacement strategy
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
       readIO = writeIO = 0;

	   char* buffersize = (char *)calloc(numPages,sizeof(char)*PAGE_SIZE);

	   filehandler = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));
	   queue = (Queue *)malloc(sizeof(Queue));

	  bm->pageFile = (char *)pageFileName;
	  bm->numPages = numPages;
	  bm->strategy = strategy;
	  bm->mgmtData = buffersize;
	  openPageFile(bm->pageFile,filehandler);

	  createQueue(bm);

	   return RC_OK;

}

//destroys a buffer pool
RC shutdownBufferPool(BM_BufferPool *const bm)
{
	int i = 0, returncode = -1;
	pageInfo *pinfo=NULL,*temp=NULL;

	pinfo=queue->head;
	while (i< queue->filledframes)
	{

		if(pinfo->fixCount==0){

			switch(pinfo->isDirty==1){
				case true: writeBlock(pinfo->pageNum,filehandler,pinfo->bufferData);
				writeIO++;
				pinfo->isDirty=0;
				break;
			}

		}

		pinfo=pinfo->nextPageInfo;	
		i++;
	}
	
	closePageFile(filehandler);
	
	return RC_OK;
}

//causes all dirty pages (with fix count 0) from the buffer pool to be written to disk.
RC forceFlushPool(BM_BufferPool *const bm)
{
	int i = 0;
	pageInfo *temp1;
	temp1 = queue->head;
	while (i< queue->totalNumOfFrames)
	{

		if(temp1->fixCount==0){
				switch(temp1->isDirty==1){
					case true:  writeBlock(temp1->pageNum,filehandler,temp1->bufferData);
								writeIO++;
								temp1->isDirty=0;
								break;
				}
	
		}

		
		temp1=temp1->nextPageInfo;
		i++;
	}
	
	return RC_OK;
}

//unpins the page 
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	pageInfo *temp;
	int i=0;
	temp = queue->head;
	while (i < bm->numPages)
	{
		if(temp->pageNum==page->pageNum)
			break;
		temp=temp->nextPageInfo;
		i++;
	}
	
	
	if(i != bm->numPages)
		temp->fixCount=temp->fixCount-1;
	else
		return RC_READ_NON_EXISTING_PAGE;
			
	return RC_OK;
}

//write the current content of the page back to the page file on disk
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	pageInfo *temp;
	int i = 0;
	temp = queue->head;

	while (i < bm->numPages)
	{
		if(temp->pageNum==page->pageNum)
			break;
		temp=temp->nextPageInfo;
		i++;
	}
	
	

	if(i == bm->numPages)
		return 1; 
	int flag;

	if((flag=writeBlock(temp->pageNum,filehandler,temp->bufferData))!=0)
		return RC_WRITE_FAILED;
	else
		writeIO++;

	return RC_OK;
}

//marks a page as a dirtyx`
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	int i = 0;
	pageInfo *temp;
	
	temp = queue->head;

	while (i < bm->numPages)
	{
		if(temp->pageNum==page->pageNum)
			break;
		if(temp->nextPageInfo!=NULL)
		temp=temp->nextPageInfo;
	i++;
	}
	

	switch(i == bm->numPages){
		case true:
				return RC_READ_NON_EXISTING_PAGE;
	}
	
	temp->isDirty=1;
	return RC_OK;
}

//------------Statistics Functions-----------------

//returns an array of PageNumbers (of size numPages) where the ith element is the number of the page stored in the ith page frame
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	PageNumber (*pages)[bm->numPages];
	pages=calloc(bm->numPages,sizeof(PageNumber));
	pageInfo *temp;
	int i = 0;
	while (i< bm->numPages)
	{	
		temp=queue->head;
		while (temp!=NULL)
		{
			if(temp->frameNum ==i)
	           {
		       		(*pages)[i] = temp->pageNum;
			   		break;
				}
			temp=temp->nextPageInfo;
		}
		i++;
	}
	
	
	return *pages;
}

//returns an array of bools (of size numPages) where the ith element is TRUE if the page stored in the ith page frame is dirty.
bool *getDirtyFlags (BM_BufferPool *const bm)
{
	bool (*isDirty)[bm->numPages];
	int i;
	isDirty=calloc(bm->numPages,sizeof(PageNumber));
	pageInfo *temp;
	
	for(i=0; i< bm->numPages ;i++)
	{
		for(temp=queue->head ; temp!=NULL; temp=temp->nextPageInfo)
		{
           if(temp->frameNum ==i)
           {
				if(temp->isDirty==1)
					(*isDirty)[i]=TRUE;
				else
					(*isDirty)[i]=FALSE;
				break;
			}
		}
	}
	return *isDirty;

}

//returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame
int *getFixCounts (BM_BufferPool *const bm)
{
	int (*fixCounts)[bm->numPages];
	int i;
	fixCounts=calloc(bm->numPages,sizeof(PageNumber));
	pageInfo *temp;

	for(i=0; i< bm->numPages;i++){	
       for(temp=queue->head ; temp!=NULL; temp=temp->nextPageInfo){
	           if(temp->frameNum ==i){
		       (*fixCounts)[i] = temp->fixCount;
			   break;
				}
			}
		}
	return *fixCounts;
}

//returns the number of pages that have been read from disk since a buffer pool has been initialized
int getNumReadIO (BM_BufferPool *const bm)
{
	return readIO;
}

//returns the number of pages written to the page file since the buffer pool has been initialized
int getNumWriteIO (BM_BufferPool *const bm)
{
	return writeIO;
}

//pins the page with page number with FIFO replacement strategy
RC pinPageFIFO(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
		int numPages = bm->numPages;
		int pageFound = 0;
		
			pageInfo *list=NULL;
			pageInfo *temp=NULL;
			
			list = queue->head;

			//for a given page number finding a node
			int i = 0;
			while (i < numPages)
			{
				switch (pageFound)
				{
				case 0:
					if (list->pageNum != pageNum)
					{
						list = list->nextPageInfo;
												
					}
					else
					{
						pageFound = 1;
						break;
					}
					break;
				}
				i++;
			}
			
			
			switch (pageFound)
			{
			case 1:
				list->fixCount++;
				page->data = list->bufferData;
				page->pageNum = pageNum;

				
				return RC_OK;
				break;
			
			default:
				break;
			}
			
				temp = queue->head;
				int returncode = -1;
				
			//when compared with the total number of available frames, the frames available in the memory are less and from the head find out the first free frame.
			while (queue->filledframes < queue->totalNumOfFrames)
			{
				if(temp->pageNum != -1)
				{
					temp = temp->nextPageInfo;	
 				}
				else
				{
					(*temp).fixCount = 1;
					(*temp).isDirty = 0;
					(*temp).pageNum = pageNum;
					(*page).pageNum= pageNum;
					
					(*queue).filledframes = (*queue).filledframes + 1 ;
					
					readBlock((*temp).pageNum,filehandler,(*temp).bufferData);
					
					(*page).data = (*temp).bufferData;
					readIO++;
					returncode = 0;
					break;	
				}
			}
			
			switch (returncode)
			{
			case 0:
				return RC_OK;
				break;
			}
		
		
		    // creating new node
			pageInfo *addnode = (pageInfo *) malloc (sizeof(pageInfo));
			(*addnode).fixCount = 1;
			(*addnode).isDirty = 0;
			(*addnode).pageNum = pageNum;
			(*addnode).bufferData = NULL;
			(*addnode).nextPageInfo = NULL;
			(*page).pageNum= pageNum;
			(*addnode).prevPageInfo = queue->tail;
			temp = queue->head;

			i = 0;
			while (i<numPages)
			{
				if((temp->fixCount) != 0)
					temp = temp->nextPageInfo;
					
	        	else
					break;
					
			i++;
			}
			
			pageInfo *temp1=NULL;
			temp1=temp;
			if(i==numPages)
			{
				return RC_NO_FREE_BUFFER_ERROR;
			}

			
			if(temp!=queue->head && temp != queue->tail){
				temp->prevPageInfo->nextPageInfo = temp->nextPageInfo;
				temp->nextPageInfo->prevPageInfo=temp->prevPageInfo;
			}


			if(temp == queue->head)
			{

				queue->head = queue->head->nextPageInfo;
				queue->head->prevPageInfo = NULL;

			}
			if(temp == queue->tail)
			{
				queue->tail = temp->prevPageInfo;
				addnode->prevPageInfo=queue->tail;
			}
			
			//write back to the disk if the frame to be replaced is dirty.
			switch(temp1->isDirty == 1){
				case true: writeBlock(temp1->pageNum,filehandler,temp1->bufferData);
			 				writeIO++;
			 				break;
			}

			(*addnode).bufferData = (*temp1).bufferData;
			(*addnode).frameNum = (*temp1).frameNum;
			
			readBlock(pageNum,filehandler,(*addnode).bufferData);
			(*page).data = (*addnode).bufferData;
			readIO++;
			
			queue->tail->nextPageInfo = addnode;
			(*queue).tail=addnode;
            
            return RC_OK;

}