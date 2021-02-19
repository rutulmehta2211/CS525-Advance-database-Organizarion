CS 525 -  Assignment 1 - Storage Manager 

--> Group Members:

Rutul Rajeshkumar Mehta - A20476293 (rmehta29@hawk.iit.edu)
Venkata Akshith Reddy Kasireddy - A20455209 (vkasireddy@hawk.iit.edu)
Quick Savajiyani - A20451378 (qsavajiyani@hawk.iit.edu)

--> Problem Description:
 
The main motive of this assignment is to implement a simple storage manager which can read blocks from a file on disk into memory and also write blocks from memory to a file on disk. It should also have methods for creating, opening and closing the files. It should also keep track of the information of the open file i.e: The total number of pages in a file, current page position, the file name, and the file pointer. 


--> Instructions to Run the Code:

Run the following commands on a linux System. 
Go to the assignment directory and then run the following commands. 

$ make
$ ./assign1 


--> Storage Manager functions description:

--> File manipulation functions: 

createPageFile - This method creates a page file with an initial file size to be 1 page. It then fills this one page with ‘\0’ bytes. 

openPageFile - This method is used to open an already existing file. If the file is found it will open the file else it will give an error message “RC_FILE_NOT_FOUND”. 

closePageFile - This method is used to close an open page file. 

destroyPageFile - This method is used to destroy/delete an already created page file. 


--> Read Functions:

readBlock - This method is used to read the block of the existing file. It reads the block that is at the position “pageNum” from a file and stores the contents of the file to the memory. 

getBlockPos - This method is used to give the current page position in the file. 

readFirstBlock - This method is used to read the pages from the first block. If the file is empty or does not exist it gives an error message. 

readPreviousBlock - This method is used to read pages from the previous block using the curPagePos of the file. If the curPagePos is the first page then it will give an error message. 

readCurrentBlock - This method is used to read the current page using the curPagePos. If the file does not exist it gives an error message. 

readNextBlock - This method is used to read the page from the next block of the file and this is done by the help of curPagePos. If the current block is the last block then it will give an error message. 

readLastBlock - This method is used to read the pages from the last block. If the file is empty or does not exist it gives an error message. 


--> Write Functions:

writeBlock - This method is used to write a page to the disk using the absolute position i.e. pageNum. If the page exists it will write the page to the disk else it will give an error message. 

writeCurrentBlock - This method is used to write a page to the disk using the current position. 

appendEmptyBlock - This method is used to add a page in the file at the end. The page that is added should be filled with 0 bytes. 

ensureCapacity - This method is used to make sure that the number of pages in the file is not less than the number of pages specified in “numberOfPages”. If the pages are less than the specified number then the pages are added to the file using the “appendEmptyBlock” method and those pages are filled with 0 bytes. 


--> Test Cases: 

We made use of readNotThere() and pageTest() as two additional test cases in test_assign1_1.c which are described as “Test reads that aren’t over there” and “Testing content of the second page.” The logic of these test cases are described as follows:

readNotThere() : 
In the first step it creates and opens a new file.
After opening the file, it attempts to read the entire file unit until it reaches the end page of the file.
After reaching the last page, it destroys the entire file that was created in this method.
After destroying the file, opening a destroyed file should cause an error.
Thus it verifies if something is there or not.

pageTest() :
In the first step it creates and opens a new file.
After opening a new file, the first block and second block are written.
Every page is read until it encounters the last page of the file.
When performing a reading operation, it first reads and then verifies whether the content is what we have written.
The read and write methods are executed in the current block if the content is per the rules.
Appending an empty block.
Ensuring the capacity of the file in terms of total number of pages.
Thereafter the file is destroyed.
After destroying the file, opening a destroyed file should cause an error.
Thus it verifies the content of the second page.




 





