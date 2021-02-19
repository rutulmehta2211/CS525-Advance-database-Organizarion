#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<math.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<string.h>
#include "storage_mgr.h"


FILE *f;

void initStorageManager (void) {
	printf("\n<-----------Venkata Akshith Reddy Kasireddy-------------->\n");
	printf("\n<-----------Quick Savajiyani-------------->\n");
	printf("\n<-----------Rutul Mehta-------------->\n");
	printf("\n<---------------- Storage Manager Booting up------------->\n ");
	f = NULL; // file pointer initialized
}

// Function for constructing a page file
RC createPageFile (char *fileName) {
	f = fopen(fileName, "w+"); // Authorization to write is granted while opening the file

	// Condition to check whether the file is empty
	condition((f != NULL),{

		SM_PageHandle emptyPage;
		emptyPage = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));// empty page creation in thr memory

		// writing operation in the memory.
		condition((fwrite(emptyPage, sizeof(char), PAGE_SIZE,f) >= PAGE_SIZE),
		{
			printf("condition satisfied thus:");
			printf("write successful \n");
		})
		else{
			printf("condition isn't satisfied thus:");
			printf("write failure \n");
		}

		//closing a file
		fclose(f);
		// on the completion of the write function, memory is set free.
		free(emptyPage);
		verify();
	})
	else
		wrong();
}

//function definition for opening a page file
RC openPageFile (char *fileName, SM_FileHandle *fHandle) {
	// read permission granted while opening a file
	f = fopen(fileName, "r");
	condition(doEq(f,NULL),
	{
		wrong();
	})
	else
	{
			
	    //Assigning values to file handler.
	    (*fHandle).fileName = fileName;
	     struct stat fi;
	    (*fHandle).curPagePos = 0;
	    
	    //(*fHandle).totalNumPages = (ftell(f) + 1) / PAGE_SIZE;
			condition(lesser((fstat(fileno(f), &fi)),0),
				{
					unavailable();
				})

			(*fHandle).totalNumPages = (fi.st_size/ PAGE_SIZE);


	    fclose(f); //rewind function used to reset the pointer to its commencement.
	    verify();
	}
}

//function to terminate the page file
RC closePageFile (SM_FileHandle *fHandle) {
	condition((f != NULL),{f = NULL;})
	verify();
}

//destroying the generated page file
RC destroyPageFile (char *fileName) {
	f = fopen(fileName,"r");
	condition(doEq(f,NULL),
	{
		wrong();
	})

	remove(fileName); //delete the file after its done
	verify();
}

/*Reading Operation */
 //function for extracting the data from the block in reading mode.
 
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	int isSeek;
	f = fopen((*fHandle).fileName, "r");
	condition(doEq(f,NULL),
		{wrong();})
	condition(((pageNum < 0) || (pageNum > (*fHandle).totalNumPages)),
	{existence();})
	
	isSeek = fseek(f, (pageNum * PAGE_SIZE), SEEK_SET);
	condition(doEq(isSeek, 0),
	{
	// Reading the content
  // If else condition to check the block size
		condition((fread(memPage, sizeof(char), PAGE_SIZE, f) < PAGE_SIZE),{unavailable();})
	})
	else
	{
		existence();
	}
	//modifying the current page position
	(*fHandle).curPagePos = ftell(f);
	// Closing file.
	fclose(f);
    verify();
}

//function for extracting the position of the current block
int getBlockPos (SM_FileHandle *fHandle) {
	return (*fHandle).curPagePos;
}

//function from extracting the data from the first block
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	int i;
	f = fopen((*fHandle).fileName, "r");
	
	condition(doEq(f,NULL),{wrong();})
	i=0;
	while(i<PAGE_SIZE){
		char bit = fgetc(f);
		condition(feof(f),{break;})
		else
		{
			memPage[i] = bit;
		}
		i++;
	}

	
	(*fHandle).curPagePos = ftell(f);
	fclose(f);
	verify();
}

//function for extracting the data from the previous block
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	
	int currentPageNumber,startPosition,i=0;
	char bit;
	//if condition for checking the index
	condition(((*fHandle).curPagePos > PAGE_SIZE),{
		f = fopen(fHandle->fileName, "r");
		condition(doEq(f,NULL),{wrong();})
		
		currentPageNumber = ((*fHandle).curPagePos / PAGE_SIZE);
		startPosition = (PAGE_SIZE * (currentPageNumber - 2));

		// function to set the pointer to the previous block position.
		fseek(f, startPosition, SEEK_SET);
		while(i < PAGE_SIZE){
			bit = fgetc(f);
			memPage[i] = bit;
			i++;
		}
		
		

		
		((*fHandle).curPagePos) = ftell(f);
		
		fclose(f);
		verify();
	})
	else
	{
		printf("\n No previous block as this is the first block.");
		existence();
	}
}

//function to read the current block
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	int currentPageNumber,startPosition,i=0;
	
	f = fopen((*fHandle).fileName, "r");
	
	condition(doEq(f,NULL),{wrong();})

	
	currentPageNumber = ((*fHandle).curPagePos / PAGE_SIZE);
	startPosition = (PAGE_SIZE * (currentPageNumber - 2));


	
	fseek(f, startPosition, SEEK_SET);
	while(i < PAGE_SIZE){
		condition((feof(f)),{break;})
		memPage[i] = fgetc(f);
		i++;
	}

	//pointer set to the current page position
	(*fHandle).curPagePos = ftell(f);

	
	fclose(f);
	verify();
}

//Reading next block of the file
 RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
 	int currentPageNumber,startPosition,i=0;
 	
	f = (fopen((*fHandle).fileName, "r"));
	
	condition(((*fHandle).curPagePos != PAGE_SIZE),{
	
	//incrementing the index of pointer to the succeeding block
		currentPageNumber = ((*fHandle).curPagePos / PAGE_SIZE);
		startPosition = (PAGE_SIZE * (currentPageNumber - 2));
		
		fseek(f, startPosition, SEEK_SET);

		
		condition(doEq(f,NULL),{wrong();})


		while(i < PAGE_SIZE){
			condition((feof(f)),{break;})
			memPage[i] = fgetc(f);
			i++;
		}

		
		(*fHandle).curPagePos = ftell(f);

		
		fclose(f);
		verify();
	})
	else
	{
		printf("\n This is the final block. Thus next block can't be found.");
		existence();
	}
}

//function to execute the last block reading operation
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	int startPosition,i=0;
	startPosition = (((*fHandle).totalNumPages - 1) * PAGE_SIZE);
	
	f = (fopen((*fHandle).fileName, "r"));
	
	condition(doEq(f,NULL),{wrong();})
	
	fseek(f, startPosition, SEEK_SET);

	while(i < PAGE_SIZE){
		condition((feof(f)),{break;})
		memPage[i] = fgetc(f);
		i++;
	}

	//decrementing the index of pointer to the preceeding block
	(*fHandle).curPagePos = ftell(f);

	
	fclose(f);
	verify();
}

/*Writing Operation */
// writing block at a given page number
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	
	
	f = (fopen((*fHandle).fileName, "r+"));
	int startPosition;

	
	condition(doEq(f,NULL),{wrong();})
	
	condition(((pageNum < 0) || (pageNum > (*fHandle).totalNumPages)),{writing();})
	startPosition = (pageNum * PAGE_SIZE);

	condition((pageNum != 0),{
		// data written to the first page.
		(*fHandle).curPagePos = startPosition;
		fclose(f);
		writeCurrentBlock(fHandle, memPage);
	})
	else
	{
		int i = 0;
		//updating the current page location to the pagenum
		fseek(f, startPosition, SEEK_SET);
		while(i < PAGE_SIZE){
			if(feof(f) == "TRUE")
			{
				appendEmptyBlock(fHandle);
				fputc(memPage[i], f);
			}else{
				fputc(memPage[i], f);
			}
			i++;

		}

		
		(*fHandle).curPagePos = ftell(f);
		
		fclose(f);
	}
	verify();
}

//exteding the file by adding an empty block
RC appendEmptyBlock (SM_FileHandle *fHandle) {
	
	
	SM_PageHandle emptyBlock;
	int isSeek;
	isSeek = fseek(f, 0, SEEK_END);
	emptyBlock = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
	
	

	condition((isSeek != 0), {
		free(emptyBlock);
		writing();
	})
	else
	{
		
		fwrite(emptyBlock, sizeof(char), PAGE_SIZE, f);
	}
	
	free(emptyBlock);

	
	(*fHandle).totalNumPages += 1;
	verify();
}

//function to write to the current block
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    
	f = (fopen((*fHandle).fileName, "r+"));

	
	condition(doEq(f,NULL),{wrong();})
	
	appendEmptyBlock(fHandle);
	fseek(f, (*fHandle).curPagePos, SEEK_SET);

	
	fwrite(memPage, sizeof(char), strlen(memPage), f);
	(*fHandle).curPagePos = ftell(f);
	fclose(f);
	verify();
}

//ensuring the capacity of the file

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
	
	f = fopen((*fHandle).fileName, "a");
	condition(doEq(f,NULL),{wrong();})

	
	while(numberOfPages > (*fHandle).totalNumPages){
		appendEmptyBlock(fHandle);
	}
	
	fclose(f);
	verify();
}

RC verify(){
	return RC_OK;
}

RC wrong(){
	return RC_FILE_NOT_FOUND;
}

RC existence(){
	return RC_READ_NON_EXISTING_PAGE;
}

RC unavailable(){
	return RC_ERROR;
}

RC writing(){
	return RC_WRITE_FAILED;
}