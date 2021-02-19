#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"


RecordManager *recordManager;

const int MAX_NUMBER_OF_PAGES = 100;		//size of max number of pages.

const int ATTRIBUTE_SIZE = 15;	//name attribute size

//In order to use the record manager, we have to define a custom data structure.
// typedef struct RecordManager
// {
// 	int tuplesCnt;	//total number of tuples in the table.
// 	int firstFreePage; //The location of first free page that has empty slots in the table.
// 	int scannedRecordCount;		//number of traversed records.
// 	BM_PageHandle pgHandle; //Page handle for BUffer Manager to access the page files.
// 	RID rID; //Record ID
// 	BM_BufferPool bPool; //Buffer Pool used for Buffer Manager
// 	Expr *conditionValue;	//Condition Value is used to traverse the records in the table.
// } RecordManager;


// RecordManager *recordManager;


// function to return an available free slot
int findFreeSlot(char *data, int rSize)
{
	int j,totalSlots;
	
	totalSlots = (PAGE_SIZE / rSize);
	j=0;
	while (j < totalSlots){
		bool x = (data[j * rSize] != '+');
		if (x)
			
			return j;
		j +=1;
	}
	return -1;
}


// function to begin the record manager.
extern RC initRecordManager (void *mgmtData)
{
	//Initilializing the storage manager.
	initStorageManager();
solve();
}

// function to terminate the record manager
extern RC shutdownRecordManager ()
{
	recordManager = NULL;
	
	free(recordManager);
	solve();
}

// function to create a table with "name" as its table name and its schema is denoted by "schema".
extern RC createTable (char *name, Schema *schema)
{

	int result, l,m,k;
	char data[PAGE_SIZE];
	char *pgHandle = data;
	recordManager = (RecordManager*) malloc(sizeof(RecordManager));	//malloc function to allocate the memory space to the record manager

l=0;m=0;k=0;
	initBufferPool(&recordManager->bPool, name, MAX_NUMBER_OF_PAGES, RS_LRU, NULL);

	

	
	*(int*)pgHandle = 0;
	pgHandle += sizeof(int);
	*(int*)pgHandle = 1;
	pgHandle += sizeof(int);
	*(int*)pgHandle = schema->numAttr;
	pgHandle += sizeof(int);
 	*(int*)pgHandle = schema->keySize;
	pgHandle += sizeof(int);

	
	while(k < (*schema).numAttr)
    	{
		//setting the attribute name
       		strncpy(pgHandle, schema->attrNames[k], ATTRIBUTE_SIZE);
	       	pgHandle += ATTRIBUTE_SIZE;

		//setting the attribute data type
	       	*(int*)pgHandle = (int)schema->dataTypes[k];

		//increment the pointer by sizeof(int).
	       	pgHandle += sizeof(int);

		//setting the attribute data type length.
	       	*(int*)pgHandle = (int) schema->typeLength[k];

		//increment the pointer by sizeof(int).
	       	pgHandle += sizeof(int);

		m += 1;
		l += 1;
		k += 1;
    	}

	SM_FileHandle fileHandle;
	//Created a page file 'name' as its table name.
	
	
	if(createPageFile(name) != RC_OK){
		result = createPageFile(name);
		return result;
	}
	
	// opening a newly created page and storing it in the result
	
	if(openPageFile(name, &fileHandle) != RC_OK){
		result = openPageFile(name, &fileHandle);
		return result;
	}

	
	
	if(writeBlock(0, &fileHandle, data) !=  RC_OK){
		result = writeBlock(0, &fileHandle, data);
		return result;
	}

	
	//closing a file after writing into it.
	
	if(closePageFile(&fileHandle) != RC_OK){
		result = closePageFile(&fileHandle);
		return result;
	}

	solve();
}

// function to open the table.
extern RC openTable (RM_TableData *rel, char *name)
{
	
	int m,l,attCount,k;
	m=0;
	l=0;
	k=0;
	SM_PageHandle pgHandle;



	//table meta data set to the custom record manager.
	(*rel).mgmtData = recordManager;
	//setting the table name.
	(*rel).name = name;

	

	//Buffer pool is pinned with a pagea page.
	pinPage(&recordManager->bPool, &recordManager->pgHandle, 0);

	//setting the 0th pointer to the page handle data.
	pgHandle = (char*) recordManager->pgHandle.data;

	
	//abstracting the total number of tuples from the page file.
	(*recordManager).tuplesCnt = *(int*)pgHandle;
	pgHandle += sizeof(int);

	int y = 0;
	//retrieving a free page from the page file.
	(*recordManager).firstFreePage = *(int*) pgHandle;
    	pgHandle += sizeof(int);

	//retrieving the number of attributes from the page file.
    	attCount = *(int*)pgHandle;
	pgHandle += sizeof(int);

	Schema *schema;
	m += 1;

	schema = (Schema*) malloc(sizeof(Schema));

	//setting the parameters.
	(*schema).numAttr = attCount;
	(*schema).attrNames = (char**) malloc(sizeof(char*) *attCount);
	
	(*schema).dataTypes = (DataType*) malloc(sizeof(DataType) *attCount);
	(*schema).typeLength = (int*) malloc(sizeof(int) *attCount);
	l +=1;

	while(k < attCount){
		(*schema).attrNames[k] = (char*) malloc(ATTRIBUTE_SIZE);
		k+=1;
	}
	int i;
    k = 0;i=0;
	while(k < schema->numAttr)
    	{
		//setting the attribute name.
		strncpy(schema->attrNames[k], pgHandle, ATTRIBUTE_SIZE);
		pgHandle += ATTRIBUTE_SIZE;

		i++;

		//setting the attribute data type.
		(*schema).dataTypes[k] = *(int*) pgHandle;
		i+=2;
		pgHandle += sizeof(int);

		//setting the attribute data type length.
		(*schema).typeLength[k] = *(int*)pgHandle;
		i-=2;
		pgHandle += sizeof(int);
		i--;
		k +=1;
	}

	(*rel).schema = schema;

	unpinPage(&recordManager->bPool, &recordManager->pgHandle);	//removing the page from buffer pool.
	forcePage(&recordManager->bPool, &recordManager->pgHandle);	//writing the page to disk.
	solve();
}

//function to close the table 'rel'.
extern RC closeTable (RM_TableData *rel)
{
	//storing the metadata.
	RecordManager *recordManager = rel->mgmtData;
	

	//Shutting down the buffer Pool.
	shutdownBufferPool(&recordManager->bPool);
	solve();
}

//function to delete the table.
extern RC deleteTable (char *nam)
{
	//destroying the page file.
	
	destroyPageFile(nam);
	
	solve();
}

//function to return the number of records in the table.
extern int getNumTuples (RM_TableData *rel)
{
	//accessing and returning the tuple count(tuplesCnt).
	RecordManager *recordManager = (*rel).mgmtData;
	
	return (*recordManager).tuplesCnt;
}


//function to insert a new record into the table and to update the 'record' with the ID of the latest inserted record.
extern RC insertRecord (RM_TableData *rel, Record *record)
{
	char *data,*slotPointer;
	int x = 1;

	//Retrieving the stored metadata from the table.
	RecordManager *recordManager = (*rel).mgmtData;

	//setting the Id for the record.
	RID *rID = &record->id;

	//setting the first free page to the current page.
	(*rID).page = (*recordManager).firstFreePage;

	pinPage(&recordManager->bPool, &recordManager->pgHandle, (*rID).page);

	//setting the data to the initial position of the data on the record.
	data = recordManager->pgHandle.data;

	//funciton to get a free slot.
	(*rID).slot = findFreeSlot(data, getRecordSize(rel->schema));
	
	while(rID->slot == -1)
	{
		unpinPage(&recordManager->bPool, &recordManager->pgHandle);	//Unpinning the page if there are no free slots on the page that is already pinned.
		rID->page++; //incrementing the page.
		x = x + 2;
		pinPage(&recordManager->bPool, &recordManager->pgHandle, (*rID).page);	//adding a new page to the buffer Pool.
		data = recordManager->pgHandle.data;		//setting the data to the initial position of the data on the record.
		x = x - 1;
		(*rID).slot = findFreeSlot(data, getRecordSize((*rel).schema));		//function to find the free slot.
	}

	slotPointer = data;

	markDirty(&recordManager->bPool, &recordManager->pgHandle);	//When a page is modified, mark it dirty.
	slotPointer += (rID->slot * getRecordSize(rel->schema));	//Calculation the Slot's initial position.
	x = 0;
	*slotPointer = '+';		//'+' is added in the end to denote that this is a new record and should be eliminated if the spare space is less.
	memcpy(++slotPointer, record->data + 1, getRecordSize((*rel).schema) - 1);	//Slot pointer points to the memory location where the data is copied.
	unpinPage(&recordManager->bPool, &recordManager->pgHandle);	//Page is removed from the buffer pool.
	x =x + 3;
	recordManager->tuplesCnt++;	//incrementing the tuple count.
	x =x - 1;
	pinPage(&recordManager->bPool, &recordManager->pgHandle, 0);	//Pinning the page again.
	solve();
}

//function to delete a record having an "Id" in the table.
extern RC deleteRecord (RM_TableData *rel, RID id)
{
	int del,d;
	RecordManager *recordManager = rel->mgmtData;	//Retreiving the meta data stored in the table.
	pinPage(&recordManager->bPool, &recordManager->pgHandle, id.page);		//pinning the page that has the record that needs to be updated.
	del = 0;
	d = 0;
	(*recordManager).firstFreePage = id.page;		//Updating the free page.
	del += 1;
	d++;
	char *data = recordManager->pgHandle.data;
	data += (id.slot * getRecordSize((*rel).schema));
	d -= 1;
	*data = '-';	//deleting the record.
	markDirty(&recordManager->bPool, &recordManager->pgHandle);	//Since the page is modified, it has been marked dirty.
	
	unpinPage(&recordManager->bPool, &recordManager->pgHandle);	//After retrieving the record, the page is not required to be kept in the memory. Hence, Unpin the page.
	solve();
}

//function to update a "record" in the table.
extern RC updateRecord (RM_TableData *rel, Record *record)
{
	int dataValue,r;
	dataValue =0;r = 0;
	char *data;

	RecordManager *recordManager = (*rel).mgmtData;	//retriving the meta data stored in the table.
	pinPage(&recordManager->bPool, &recordManager->pgHandle, record->id.page);	//pinning the page that contains the record we've to update.

	RID id = (*record).id;//Setting the record ID.

	//Calculating the start position of the new data.
	data = recordManager->pgHandle.data;
	r = r + 3;
	data += (id.slot * getRecordSize(rel->schema));
	dataValue += 1;
	*data  = '+';	//the record is not empty.

	memcpy(++data, record->data + 1, getRecordSize((*rel).schema) - 1 );	//to the already existing record, a new record's data has been copied.
	r = r - 1;
	markDirty(&recordManager->bPool, &recordManager->pgHandle);	//Page is modified. Hence, marked dirty.
	
	unpinPage(&recordManager->bPool, &recordManager->pgHandle);	//Retriving the record from the memory and unpinning the page as it is no longer required.
	solve();
}

//function to retrieve a record having Record ID in the table. The result is then stored in the location.
extern RC getRecord (RM_TableData *rel, RID id, Record *record)
{
	
	RecordManager *recordManager = (*rel).mgmtData;	//Retrieving the metadata stored.
	int recordCount;
	recordCount =0;
	pinPage(&recordManager->bPool, &recordManager->pgHandle, id.page);		//pinning the page that is to be retreived.

	char *dataPointer = recordManager->pgHandle.data;


	dataPointer += (id.slot * getRecordSize((*rel).schema));


	if(*dataPointer != '+'){
		return RC_RM_NO_TUPLE_WITH_GIVEN_RID;	// When no matching record is found in the table,it returns an error indicating no tuple with the given ID found.

	}else{
		(*record).id = id;	//Setting the record ID.
		char *data = (*record).data;	//record to data pointer is set.
		
		memcpy(++data, dataPointer + 1, getRecordSize((*rel).schema) - 1);
		recordCount += 1;
	}

	//page gets unpinned after retrieving the record.
	unpinPage(&recordManager->bPool, &recordManager->pgHandle);
	solve();
}



//function that starts scanning all the records.
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	//condition to check whether the cond is present or not.
	if(cond != NULL)
	{
		int scanVal,j;
		scanVal = 0;
		j = 10;
		openTable(rel, "ScanTable");	//function to open the table in the memory.

		RecordManager *tableManager;
		RecordManager *scanManager;

		
		
	    	scanManager = (RecordManager*) malloc(sizeof(RecordManager));
	    	(*scan).mgmtData = scanManager;
	    	j = j + 12;
	    	scanManager->rID.page = 1;
	    	scanManager->rID.slot = 0;
	    	j = j + 10;
		scanManager->scannedRecordCount = 0;

			j = j - 12;
	    	(*scanManager).conditionValue = cond;
	  	tableManager = (*rel).mgmtData;
	  	j += 1;
		(*tableManager).tuplesCnt = ATTRIBUTE_SIZE;
		(*scan).rel = rel;
		j = 0;
		scanVal = scanVal+1;
		solve();


	}
	else{
		return RC_SCAN_CONDITION_NOT_FOUND;
	}


}

//Scanning every record in the table and the result is stored in the location 'record'.
extern RC next (RM_ScanHandle *scan, Record *record)
{
	//Initialiing the scanned data.
	int c,nextValue,totalSlots,scannedRecordCount,tuplesCnt;
	c=0;
	char *data;
	RecordManager *scanManager = (*scan).mgmtData;
	
	RecordManager *tableManager = scan->rel->mgmtData;
    	Schema *schema = scan->rel->schema;

	//Checks whether the conditionValue is present or not.
	if(scanManager->conditionValue != NULL){
		
		nextValue = 2;
		Value *result = (Value *) malloc(sizeof(Value));

		c  = 1;

		totalSlots = (PAGE_SIZE / getRecordSize(schema));	//calculating the total number of slots.

		scannedRecordCount = (*scanManager).scannedRecordCount;	//retrieving the scanned record count.

		c = 2;

		tuplesCnt = (*tableManager).tuplesCnt;	//getting the tuple count.

		// Checking whether the table's tuple count is 0 or not.
		if(tuplesCnt != 0){
			while(scannedRecordCount <= tuplesCnt)	//Iterating the loop through tuples.
			{
				//Executing the block after scanning all the tuples.
				if(scannedRecordCount > 0)
				{
					scanManager->rID.slot++;

					//Execute the block after scanning all the slots.
					if( scanManager->rID.slot >= totalSlots)
					{
						scanManager->rID.slot = 0;
						c = 4;
						scanManager->rID.page++;
						nextValue--;
					}
				}
				else
				{
					scanManager->rID.page = 1;
					c = 0;
					scanManager->rID.slot = 0;
					nextValue++;
				}

				pinPage(&tableManager->bPool, &scanManager->pgHandle, scanManager->rID.page);	//putting the page in the buffer pool.

				data = scanManager->pgHandle.data;	//retrieving the page data.

				c = 0;

				data += (scanManager->rID.slot * getRecordSize(schema));	//calculating the data location is calculated from rId slot and record size.

				// setting the page, rID slot and scan Manager slot.
				record->id.page = scanManager->rID.page;
				c = 1;
				record->id.slot = scanManager->rID.slot;


				char *dataPointer = (*record).data;	//initializing the 1st location of record to data.


				*dataPointer = '-';

				memcpy(++dataPointer, data + 1, getRecordSize(schema) - 1);

				c = 5;

				//Since a record is scanned previously, the scan count is incremented.
				scanManager->scannedRecordCount++;
				scannedRecordCount++;

				//Testing the record for a specific test expression.
				evalExpr(record, schema, scanManager->conditionValue, &result);

				nextValue++;


				if(result->v.boolV == TRUE)
				{
					//unpinning the page.
					c--;
					unpinPage(&tableManager->bPool, &scanManager->pgHandle);

					solve();
				}
			}


			unpinPage(&tableManager->bPool, &scanManager->pgHandle);

			//reseting the scan manager value.
			scanManager->rID.page = 1;
			c = 2;
			scanManager->rID.slot = 0;
			scanManager->scannedRecordCount = 0;


			tuple();
		}
		else{
			tuple();
		}
	}
	else
	{
		return RC_SCAN_CONDITION_NOT_FOUND;
	}


}

//function to turn off the scan operation.
extern RC closeScan (RM_ScanHandle *scan)
{
	int closePointer;
	closePointer = 0;
	RecordManager *recordManager = scan->rel->mgmtData;
	
	RecordManager *scanManager = (*scan).mgmtData;


	if(scanManager->scannedRecordCount > 0)
	{

		unpinPage(&recordManager->bPool, &scanManager->pgHandle);
		int v;
		v=0;
		(*scanManager).scannedRecordCount = 0;
		
		scanManager->rID.page = 1;
		scanManager->rID.slot = 0;

		v += 1;
		closePointer++;
	}

	//meta data has been allocated some memory space that is now deallocated.
    	(*scan).mgmtData = NULL;
    	
    	free(scan->mgmtData);
	solve();
}



//function to return the record size of the schema.
extern int getRecordSize (Schema *schema)
{
	int recordSize,size, i,j; //setting the offset to zero.
	recordSize = 0;
	size = 0;
	i=0;
	//iterating through all the attributes.
	while(i < schema->numAttr)
	{
		
		if(schema->dataTypes[i] == DT_STRING){


			size += schema->typeLength[i];
			
			recordSize++;
			j = 1;
		}
		else if(schema->dataTypes[i] == DT_INT){

			size += sizeof(int);
			
			recordSize++;
			j = 2;
		}
		else if(schema->dataTypes[i] == DT_FLOAT){

			size += sizeof(float);
			
			recordSize++;
			j = 3;
		}
		else if(schema->dataTypes[i] == DT_BOOL){

			size += sizeof(bool);
			
			recordSize++;
			j = 4;
		}
		i++;
	}
	return ++size;
}

//function to create a new schema.
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	int x;
	x=0;
	Schema *schema = (Schema *) malloc(sizeof(Schema));
	(*schema).attrNames = attrNames;
	
	(*schema).numAttr = numAttr;
	(*schema).typeLength = typeLength;
	x++;
	(*schema).dataTypes = dataTypes;
	(*schema).keyAttrs = keys;
	x -= 1;
	(*schema).keySize = keySize;

	return schema;
}

//function to allocate the memory space to schema.
extern RC freeSchema (Schema *schema)
{
	//deallocating the already allocated memory space occupied by 'schema'.
	free(schema);
	solve();
}



// A new record in the schema referenced by "schema"
extern RC createRecord (Record **record, Schema *schema)
{
	int x;
	x=0;
	int recordSize = getRecordSize(schema);	//retrieving the record size.
	Record *newRecord = (Record*) malloc(sizeof(Record));	//allocating the memory space to the new record.
	
	(*newRecord).data = (char*) malloc(recordSize);
	
	newRecord->id.page = newRecord->id.slot = -1;
	char *dataPointer = (*newRecord).data;
	x += 1;
	*dataPointer = '-';
	*(++dataPointer) = '\0';
	x--;
	*record = newRecord;
	solve();
}

//function for attribute offset.
RC attrOffset (Schema *schema, int attrNum, int *result)
{
	int i,attrVal,j;
	i=0;
	attrVal =1;
	*result = 1;
	

	//Iterating through all the attributes.
	while(i < attrNum)
	{
		j = 0;
		if(schema->dataTypes[i] == DT_STRING){

			*result += (*schema).typeLength[i];
			
			attrVal++;
			j = 1;
		}
		else if(schema->dataTypes[i] == DT_INT){

			*result += sizeof(int);
			
			attrVal++;
			j = 1;
		}
		else if(schema->dataTypes[i] == DT_FLOAT){

			*result += sizeof(float);
			
			attrVal++;
			j = 1;
		}
		else if(schema->dataTypes[i] == DT_BOOL){

			*result += sizeof(bool);
			
			attrVal++;
			j = 1;
		}
		i++;
	}
	solve();
}

//function to remove he record from the memory.
extern RC freeRecord (Record *record)
{
	//freeing the space allocated to the records.
	free(record);
	
	solve();
}

//function to retrieve the attribute from the given record.
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	int offset,getAttrVal,s;
	offset = 0;
	getAttrVal = 0;
	s = 0;
	Value *attribute = (Value*) malloc(sizeof(Value));	//Malloc function.
	
	attrOffset(schema, attrNum, &offset);	//retrieving the attribute's offset value.
	char *dataPointer = (*record).data;	//retrieving the intial position of data in the memory.
	dataPointer +=  offset;		//adding the offset to the beginning.
	// If attrNum = 1 or not.
	
	if(attrNum != 1){
		(*schema).dataTypes[attrNum] = (*schema).dataTypes[attrNum];
	}
	else{
		(*schema).dataTypes[attrNum] = 1;
	}
	//retrieving the attribute's value depending on its type.
	if(schema->dataTypes[attrNum] == DT_STRING){
		int length = (*schema).typeLength[attrNum];

		attribute->v.stringV = (char *) malloc(length + 1);
		s = 1;
		strncpy(attribute->v.stringV, dataPointer, length);
		attribute->v.stringV[length] = '\0';
		s--;
		(*attribute).dt = DT_STRING;
		getAttrVal++;
	}
	else if(schema->dataTypes[attrNum] == DT_INT){
		//retrieving the value of attribute.
		int value;
		value = 0;
		s = 2;
		memcpy(&value, dataPointer, sizeof(int));
		attribute->v.intV = value;
		s--;
		(*attribute).dt = DT_INT;
		getAttrVal++;
	}
	else if(schema->dataTypes[attrNum] == DT_FLOAT){
		//float type
		
		s=3;
		float value;
		memcpy(&value, dataPointer, sizeof(float));
		attribute->v.floatV = value;
		s--;
		(*attribute).dt = DT_FLOAT;
		getAttrVal++;
	}
	else if(schema->dataTypes[attrNum] == DT_BOOL){
		//booloan type attribute value.
		
		s = 3;
		bool value;
		memcpy(&value,dataPointer, sizeof(bool));
		attribute->v.boolV = value;
		s--;
		(*attribute).dt = DT_BOOL;
		getAttrVal++;
	}
	else{
		printf("\nFor the given datatype,serializer is not defined \n");
	}

	*value = attribute;
	solve();
}

//function to set The attribute value in the specified schema.
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
	int offset,dataLevel,dataPoint,a;
	offset = 0;
	dataLevel = 0;
	dataPoint = 0;
	attrOffset(schema, attrNum, &offset);	//retrieving the offset value of attributes.
	char *dataPointer = record->data;
	dataPointer += offset;
	dataPoint = 1;
	dataLevel = 1;
	
	if(schema->dataTypes[attrNum] == DT_STRING){

		int length;
		length = (*schema).typeLength[attrNum];
		a++;
		//attribute values are copied to the location pointed by the data pointer.
		strncpy(dataPointer, value->v.stringV, length);
		dataPoint++;
		a += 3;
		dataPointer += (*schema).typeLength[attrNum];
	}
	else if(schema->dataTypes[attrNum] == DT_INT){
		//setting the attribute value - type integer
		*(int *) dataPointer = value->v.intV;
		a+=2;
		dataPointer += sizeof(int);
		dataLevel++;
	}
	else if(schema->dataTypes[attrNum] == DT_FLOAT){
		//float type
		*(float *) dataPointer = value->v.floatV;
		a += 3;
		dataPointer += sizeof(float);
		dataPoint++;
	}
	else if(schema->dataTypes[attrNum] == DT_BOOL){
		//boolean type
		*(bool *) dataPointer = value->v.boolV;
		a += 4;
		dataPointer += sizeof(bool);
		dataLevel++;
	}
	else{
		printf("\nSerializer is not defined for the given datatype. \n");
	}
	solve();
}

extern RC solve(){
	return RC_OK;
}

extern RC tuple(){
	return RC_RM_NO_MORE_TUPLES;
}