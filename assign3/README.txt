CS-525 Assignment 3 - Record Manager 

--> Group Members:

Rutul Rajeshkumar Mehta - A20476293 (rmehta29@hawk.iit.edu)
Venkata Akshith Reddy Kasireddy - A20455209 (vkasireddy@hawk.iit.edu)
Quick Savajiyani - A20451378 (qsavajiyani@hawk.iit.edu)

-->Instruction to Run :-

Run the following commands on a linux System. 
Go to assignment 3 directory and run the following commands:

1. Execute the make file 1 as follows: This is for the Test case 1(test_assign3_1)
		make -f makefile1

2. Execute the make file 2 as follows: This is for the Test case 2(test_assign3_2)
		make -f makefile2

3. Execute the make file 3 as follows: This is for the Test case 3(test_expr)
		make -f makefile3
		

--> FUNCTION DESCRIPTIONS:

Table and manager:
initRecordManager:
	- This function is used to initialize a record manager.

shutdownRecordManager:
	- This function is used to shutdown the record manager.

createTable:	
	- This function is used to create a new table. The table name, the schema and free space information is included.

openTable:
	- This function is used to open the table's information. After initializing the buffer pool it pins the page and reads schema from the file and then unpins it. 

closeTable:
	- This function is used to close a table. 
	- It unpins the page and then the buffer pool is shutdown, before the buffer pool is shutdown it writes all the outstanding changes to the table.

deleteTable:
	- This function is used to delete the table. 

getNumTuples:
	- This function returns the number of tuples in the table.

--> Handling the records in a table:

insertRecord:
	- We use this function to insert a new record in a table, This record is inserted in an available space after a page is pinned and then the page is marked as dirty and then written           back to memory. At last the page is unpinned. 

deleteRecord:
	- We use this function to delete a record from the page and slot mentioned. The page is pinned and then the record is deleted. Once the record has been deleted the number of 	  	  records in  a page is reduced by 1 and then the page is marked dirty and then unpinned.  	
	

updateRecord:
	- We use this function to update an existing record from the page and slot mentioned then the page is pinned and the recode is updated and marked dirty. At last the page is 	  unpinned.	

getRecord:
	- We use this function to retrieve a record from a page and slot mentioned.

--> Scans:
startScan:
	- This function is used to scan the tuples based on a certain criteria.

next:
	- The start scan function calls the next method. The next method gives the next tuple from the record which satisfies the criteria given in the start scan. 
	
closeScan:
	- We use this function is indicate to the record manager that all associated resources can be cleaned up.

--> Dealing with schemas:

getRecordSize: 
	- First it checks if the schema is created or not if the schema already exists it returns the size of the records for the given schema in bytes. If the schema does not exists it 		 gives an error message. 
	 
createSchema:
	- This function is used to create a new schema

freeSchema:
	- This function is used to free the space associated with that particular schema in the memory.

--> Dealing with records and attribute values:
createRecord:
	- We use this function to create a new record, Page and slots are set to -1 initially. 	
	
freeRecord:
	- We use this function to free the record by removing the data from the specified record. we check if the record is free or not if the record is not free we remove the data from 	  that record and then make the record free. 
	
getAttr:
        - We use this function to get the attribute value of a particular record 
	

setAttr:
	- We use this function to set the attribute value.


--> ADDITIONAL IMPLEMENTATION and TEST CASES (Conditional Updates using Scans):

It takes a condition(expression) based on which the tuples to be updated are selected and a pointer to a function which takes a record as input and returns the updated version of the record.

To test the additional function for conditional updates using scans, We have added test cases in test_assign3_2.c. 

