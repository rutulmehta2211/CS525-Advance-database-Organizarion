#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "dberror.h"

FILE *file;
RC returnCode;

void initStorageManager(void)
{
    printf("Storage manager initialized....\n\n");                          //print "Storage manager initalized..." message.
}

RC createPageFile(char *fileName)
{
    char *memoryBlock = malloc(PAGE_SIZE * sizeof(char));                   //Allocate memory block
    file = fopen(fileName,"w+");                                            //If the file does not exist, it will be created. Open for both reading and writing.
    if(file == NULL)                                                        //Check whether file is created or not.
        returnCode = RC_FILE_NOT_FOUND;                                     //Error return code - 'File not found'.
    else
    {
        memset(memoryBlock, '\0', PAGE_SIZE);                               //If file exists then setting the allocated memory block by \0 using memset functions
		fwrite(memoryBlock, sizeof(char), PAGE_SIZE, file);                 //Writing memory block to the file.
		free(memoryBlock);                                                  //Memory block free, after writing.
		fclose(file);                                                       //Close the file, after creation.
		returnCode = RC_OK;                                                 //return code - Success.
    }
    return returnCode;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    file = fopen(fileName, "r+");                                           //Open file for reading
    if(file == NULL)                                                        //file variable should not be NULL.
        returnCode = RC_FILE_NOT_FOUND;                                     //Error return code - 'File not found'.
    else
    {
        fseek(file, 0, SEEK_END);                                           //Sets the position of a file pointer to the end of the file.
        (*fHandle).fileName = fileName;                                     //Set fileName - member variable of "SM_FileHandle" structure.
        (*fHandle).totalNumPages = (ftell(file)+1)/PAGE_SIZE;               //Set totalNumPages - member variable of "SM_FileHandle" structure - by deviding last location of the file by PAGE_SIZE.
        (*fHandle).curPagePos = 0;                                          //Set curPagePos - member variable of "SM_FileHandle" structure - as 0
        rewind(file);                                                       //File pointer again back to the start of the file.
        returnCode = RC_OK;                                                 //return code - Success.
    }
    return returnCode;
}

RC closePageFile(SM_FileHandle *fHandle)
{
    if(file == NULL)                                                        //file variable should not be NULL.
        returnCode = RC_FILE_NOT_FOUND;                                     //Error return code - 'File not found'.
    else
        returnCode = fclose(file)!=0 ? RC_FILE_NOT_FOUND : RC_OK;           //If file close successfully then return success otherwise error return code for 'File not found'.
    return returnCode;
}

RC destroyPageFile(char *fileName)
{
    returnCode = remove(fileName) != 0 ? RC_FILE_NOT_FOUND : RC_OK;         //If file destroy successfully then return success code otherwise return error code for 'File not found'.
    return returnCode;
}

/* reading blocks from disc */
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) 
{
    if(fHandle == NULL || file == NULL)                                     //Structure variable or file variable should not be NULL.
        returnCode = RC_FILE_NOT_FOUND;                                     //Error return code - 'File not found'.
    else
    {
        if (pageNum < 0 && pageNum > (*fHandle).totalNumPages)              //Page number should not be less than 0 or greater than total number of the page.
            returnCode = RC_READ_NON_EXISTING_PAGE;                         //Error return code - 'non-existing page'.
        else 
        {
            fseek(file, pageNum * PAGE_SIZE, SEEK_SET);                     //Set file pointer to the beginning of the page based on page number.
            int blockSize = fread(memPage, sizeof(char), PAGE_SIZE, file);  //Return block size after reading operation.
            if (blockSize < 0 || blockSize > PAGE_SIZE)                     //Block size should not be less than 0 or grater than PAGE_SIZE.
                returnCode = RC_READ_NON_EXISTING_PAGE;                     //Error return code - 'non-existing page'.
            else
            {
                (*fHandle).curPagePos = pageNum;                            //Set curPagePos - member variable of "SM_FileHandle" structure - as inserted pageNum.
                returnCode = RC_OK;                                         //return code - Success.
            }
        }
    }
    return returnCode;
}

int getBlockPos(SM_FileHandle *fHandle) 
{
    if(fHandle == NULL)                                                     //Structure variable should not be NULL.
    {
        THROW(RC_FILE_NOT_FOUND, "File not found!");                        //Throw error - 'File not found' 
        return 0;
    }
    else
        return ((*fHandle).curPagePos);                                     //Return currunt page position.
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) 
{
    return readOperations(fHandle,memPage,'F');                             //Call readOperations method with 'F' operationCode - try to read first block 
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) 
{
    return readOperations(fHandle,memPage,'P');                             //Call readOperations method with 'P' operationCode - try to read previous block
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) 
{
    return readOperations(fHandle,memPage,'C');                             //Call readOperations method with 'C' operationCode - try to read current block
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) 
{
    return readOperations(fHandle,memPage,'N');                             //Call readOperations method with 'N' operationCode - try to read next block
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) 
{
    return readOperations(fHandle,memPage,'L');                             //Call readOperations method with 'L' operationCode - try to read last block
}

RC readOperations(SM_FileHandle *fHandle, SM_PageHandle memPage, char operationCode)
{
    int pagePos;
    if (fHandle == NULL)                                                    //Variable of the structure should not be NULL.
        returnCode = RC_FILE_NOT_FOUND;                                     //Error return code - 'File not found'.
    else
    {
        pagePos = getBlockPos(fHandle);                                     //Call getBlockPos method to get current page position.
        switch(operationCode)
        {
            case 'F' :                                                      //operationCode 'F' - For reading first block
                if ((*fHandle).totalNumPages <= 0)                          //totalNumPages should not be less than or equal to 0.
                    returnCode = RC_READ_NON_EXISTING_PAGE;                 //Error return code - 'non-existing page'.
            break;
            case 'P' :                                                      //operationCode 'P' - For reading previous block
                pagePos--;                                                  //Decrease page position by 1.
		        if (pagePos < 0)                                            //Current page position should not be less than 0.
			        returnCode = RC_READ_NON_EXISTING_PAGE;                 //Error return code - 'non-existing page'.
            break;
            case 'C' :                                                      //operationCode 'C' - For reading current block
                if (pagePos < 0)                                            //Current page position should not be less than zero.
			        returnCode = RC_READ_NON_EXISTING_PAGE;                 //Error return code - 'non-existing page'.
            break;
            case 'N' :                                                      //operationCode 'N' - For reading next block
                pagePos++;                                                  //Increase page position by 1.
                if (pagePos > (*fHandle).totalNumPages)                     //Increased page position should not be greater than total number of the page.
			        returnCode = RC_READ_NON_EXISTING_PAGE;                 //Error return code - 'non-existing page'.
            break;
            case 'L' :                                                      //operationCode 'L' - For reading last block
                pagePos = (*fHandle).totalNumPages - 1;                     //Set page position to the last page.
            break;
            default:
                pagePos = getBlockPos(fHandle);                             //default - Call getBlockPos method to get current page position.                             
        }
        if(returnCode == RC_OK)                                             //Reading operation does not perform in case any error in above condition.
            returnCode = readBlock(pagePos, fHandle, memPage);              //Call readBlock method to read block.
    }
    return returnCode;                                                      
}
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) 
{
    if (fHandle == NULL || file == NULL)                                    //Structure variable or file variable should not be NULL.
        returnCode = RC_FILE_NOT_FOUND;                                     //Error return code - 'File not found'.
    else
    {
        if (pageNum < 0 || pageNum > (*fHandle).totalNumPages)              //Page number should not be less than 0 or grater than total number of the page.
            returnCode = RC_WRITE_FAILED;                                   //Error return code - 'Write failed'.
        else
        {
            if (fseek(file, (PAGE_SIZE * pageNum), SEEK_SET) != 0)          //Set file pointer to the starting of the page whose page number is pageNum.
                returnCode = RC_WRITE_FAILED;                               //Error return code - 'Write failed'.
            else
            {
                if(fwrite(memPage, sizeof(char), PAGE_SIZE, file) == 0)     //Write block at the current position of the page.
                    returnCode = RC_WRITE_FAILED;                           //Error return code - 'Write failed'.
                else
                {
                    (*fHandle).curPagePos = pageNum;                        //Set curPagePos - member variable of "SM_FileHandle" structure - as given pageNum 
                    fseek(file, 0, SEEK_END);                               //Set file pointer to the end of the file.
                    (*fHandle).totalNumPages = ftell(file) / PAGE_SIZE;     //Set totalNumPages - member variable of "SM_FileHandle" structure - by dividing last location by PAGE_SIZE.
                    returnCode = RC_OK;                                     //return code - Success.
                }
            }
        }
    }
	return returnCode;
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) 
{
    returnCode = writeBlock((*fHandle).curPagePos,fHandle,memPage);         //Call writeBlock method and use current page position for writing current block. 
	return returnCode;
}

RC appendEmptyBlock(SM_FileHandle *fHandle) 
{
    if (fHandle == NULL || file == NULL)                                    //Structure variable or file variable should not be NULL.                                
        returnCode = RC_FILE_NOT_FOUND;                                     //Error return code - 'File not found'.
    else
    {
            char *emptyBlock;
            emptyBlock = (char *) calloc(PAGE_SIZE, sizeof(char));          //Allocate memory block.
            
            fseek(file, 0, SEEK_END);
            if(fwrite(emptyBlock, 1, PAGE_SIZE, file) == 0)                 //Try to write and it should not return 0.
                returnCode = RC_WRITE_FAILED;                               //Error return code - 'Write failed'.
            else
            {
                (*fHandle).totalNumPages = ftell(file) / PAGE_SIZE;         //Set totalNumPages - member variable of "SM_FileHandle" structure - by dividing last location by PAGE_SIZE.
                (*fHandle).curPagePos = (*fHandle).totalNumPages - 1;       //Set curPagePos - member variable of "SM_FileHandle" structure - as the last page position.
                free(emptyBlock);                                           //Memory block free, after writing.
                returnCode = RC_OK;                                         //return code - Success
            }
    }
	return returnCode;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) 
{
	int pages = numberOfPages - (*fHandle).totalNumPages;                   //Calculate additional pages.
	if(pages > 0)                                                           //Additional pages should be greater than 0.
	{
		for (int i=0; i < pages; i++)
			appendEmptyBlock(fHandle);                                      //Call appendEmptyBlock method to just write empty blocks.
		returnCode = RC_OK;                                                 //return code - Success
	}
	else
		returnCode = RC_WRITE_FAILED;                                       //Error return code - 'Write failed'.
    
    return returnCode;
}
