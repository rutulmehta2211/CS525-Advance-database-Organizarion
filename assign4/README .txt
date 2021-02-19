CS 525 - Assignment 4 - B+ Tree Index

--> Group Members:

Rutul Rajeshkumar Mehta - A20476293 (rmehta29@hawk.iit.edu)
Venkata Akshith Reddy Kasireddy - A20455209 (vkasireddy@hawk.iit.edu)
Quick Savajiyani - A20451378 (qsavajiyani@hawk.iit.edu)

--> Problem Description:
The main motive behind this assignment is to implement a B+ tree index.  

--> Instructions to Run the Code:

Run the following commands on a linux System. 

In the terminal go to the assignment 4 directory and run the following commands.

$ make
$ ./test_assign4_1
$ ./test_assign4_2
$ ./test_expr

$ make clean         //To clean/delete all the object files.

--> Function Description:

initIndexManager() - Here we use this function to initialize index manager.

shutdownIndexManager() - Here this function is used to close index manager and free up resources.

createBtree() - Here this function is used to create a B+ tree index.

openBtree() - Here this function is used to open a created B+ tree and traversing all of its elements.

closeBtree() - Here this function is used to close the opened B+ tree.

deleteBtree() - Here this function is used to delete/remove a key and the corresponding record pointer from the index. It is also used to delete an existing b+ tree.

getNumNodes() - Here this function is used to get the total number of nodes in the B+ tree.
 
getNumEntries() - Here this function is used to calculate the total number of entries present in the tree by taking its pointer object result and tree as arguments.

getKeyType() - Here this function is used to get the Key Type of a B+ tree.

findKey() - Here this function is used to find a key in the B+ tree and return its RID result.

insertKey() - Here this function is used to insert a key into a B+ tree and takes in a key and its RID as arguments.

deleteKey() - Here this function is used to delete a key and its corresponding record pointer (RID) from the B+ tree.

openTreeScan() - Here this function is used to open the tree and scan all of the B+ tree entries.

nextEntry() - Here this function is used to read the next entry in the B+ tree.

closeTreeScan() - Here this function is used to close the tree after scanning all the elements of the B+ tree.

printTree() - Here this method is used to print the B+ tree. This method is mainly used for debugging purpose. 

--> Test Cases:

testInsertAndFind_Float()
- Here first this funhecks checks for the number of nodes and the number of enteries in the B+ tree
- The it Iterates in a loop and checks if the RID was found.
- This itteration is done to search for the keys.

testDelete_Float()
- Here this function first checks for the value to be deleted.
- Then checks for the value using its RID.
- The value is deleted once its found. 
- Once the value is deleted it performs the search operation again to make sure that the deleted item is not found.
 

testDelete_String()
- Here this function checks for the entry against the corresponding record pointer even among the inserted enteries.
- The corresponding record pointer of the entery that was deleted should not be found and for the once that are not deleted we find the right entry and check it against its corresponding     record pointer for confirmation.
