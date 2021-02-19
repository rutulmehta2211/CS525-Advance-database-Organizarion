CS 525 -  Assignment 2 - Buffer Manager 


--> Group Members:

Rutul Rajeshkumar Mehta - A20476293 (rmehta29@hawk.iit.edu)
Venkata Akshith Reddy Kasireddy - A20455209 (vkasireddy@hawk.iit.edu)
Quick Savajiyani - A20451378 (qsavajiyani@hawk.iit.edu)


--> Problem Description:

The main motive of this assignment is to implement a buffer manager which manages a fixed number of pages in the menory that represent pages from the page file managed by the storage manager implemented in the previous assignment. The bummer manager should be capable of handeling more than one open buffer pool at the same time. There should be the implementation of at lease 2 replacement stratigies FIFO and LRU.


--> Instructions to Run the Code:

Run the following commands on a linux System. 
Go to the assignment directory in the terminal and then run the following commands. 


$ make
$ ./assign2 

Here we have created an additional test file, 'test_assign2_2.c' so to run that you will need to modify the make file and replace the text 'test_assign2_1' with 'test_assign2_2' everywhere in the file whether its a .c file , object file or any other and then execute the above commands. 

To remove the object files, run following command: 
$make clean


--> Buffer Manager functions description:
--> Functions for Buffer Manager Interface Pool Handling:

initBufferPool - Here we use this function to create and initialize a new buffer pool using pageframes(numPages) and page replacement strategy.

shutdownBufferPool - Here we use this function to destroy the Buffer pool. 

forceFlushPool - This function is used to write all dirty pages from the buffer pool to the disk with fix count 0. 

--> Functions for Buffer Manager Interface Access Pages

markDirty - This function is used to mark the page as dirty. It sets the dirtybit of the page to 1. 

unpinPage - This function is used to remove the page from the buffer pool.

forcePage - This function is used to write the current content of the page to the page file on the disk. 

pinPage - This function pins a page with a page number(pageNum). 

--> Function for Statistics Interface

*getFrameContents - This function creats an array of pagenumber (of size pageNum) where the ith element is the number of page stored in the ith page frame. 

*getDirtyFlags - This function returns an array of booleans (of size pageNum) where the ith element is TRUE if the page stored in the ith page frame is dirty.

*getFixCounts - This function returns an array of int (of size pageNum) where the ith element is the fix count of the page stored in the ith page frame.

getNumReadIO - This function returns the number of pages that have been read to page file since the initialization of buffer pool.

getNumWriteIO - This function returns the number of pages that have been written to page file since the initialization of buffer pool.


--> Test Cases:

testCreatingAndReadingDummyPages - Here in this function a dummy page is created and read and once this is done the page is destroyed. 
  
createDummyPages - Here in this functhon dummy pages are created. 
   
checkDummyPages - Here this function is used to read dummy pages page by page and then unpinned and also the function checks the dummy pages. 
   
testReadPage - Here this function pins the page that is to be read and then after reading it unpins the page and destroys it. 
    
testFIFO - Here this function pins the page and the contents of the page are checked and then unpinned. Then a force flush is done after the last page is unpinned. It also checks the number of read and write inputs. At last when everyting is dont the buffer pool is shut down and the testing is complete. 
    
testLRU - Here this function reads the first 5 pages and then directly unpins them assuming no modifications are made. The pages are read to change the order of the LRU. Then the pages are replaced and checks that it happens in LRU order. 

test_of_LRU_K - Here this function reads the first 5 pages and then directly unpins them assuming no modifications are made. The pages are read to change the order of the LRU_K. Then the pages are replaced and checks that it happens in the function order. 

testError - Here this function requests an additional file after the pinpage of bufferpools is done and later on destroys the buffer pool then it try to pinpage with negative pagenumber before destroying the buffer pool. For the uninitialized buffer pool the shutdown buffer pool, flush buffer pool and pinpage is done for the unopened files. Lastly it unpins, force flush and make dirty that is not in the pool and then removes the page file.
 

