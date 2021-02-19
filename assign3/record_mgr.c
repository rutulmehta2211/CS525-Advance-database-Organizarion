#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

SM_FileHandle filehandler;
SM_PageHandle pagehandler;



// typedef struct TableData_information{
//     int sizeOfRecord; //size of the record
//     int totalNumerOfRecordsInTable; //total number of records in a table
//     int blkFctr;
//     RID firstFreeSpaceLocation; //first free location in the page where space is availabl
//     RM_TableData *rm_tbl_data;
//     BM_PageHandle pageHandle;//pagehandle of buffermanager
//     BM_BufferPool bufferPool;//bufferpool of buffermanager
// }TableData_information;

// typedef struct RM_SCANDATA_MANAGEMENT{
//     RID recID; //the RID of the record that scanned now
//     Expr *cond; //select condition of the record
//     int count;  // number of total records be scanned
//     RM_TableData *rm_tbl_data;
//     BM_PageHandle rm_pageHandle;
//     BM_BufferPool rm_bufferPool;
// }RM_SCANDATA_MANAGEMENT;


TableData_information tabledata_info;
RM_SCANDATA_MANAGEMENT rmscandata_mgmt;

void readSchemaFromPageFile(RM_TableData *, BM_PageHandle *);

int readTotalNumberOfKeyAttribute(char *);
int extractDataType(char *);
int extractTypeLength(char *data);
int * getDatatypeOfAttribute(char *, int );
int * getSizeOfAttribute(char *schemaData, int numAtr);
int * extractKeyData(char *data,int keyNum);
int * extractFirstFreePageSlot(char *);
int getAttributeOffsetInRecord(Schema *, int );
char * readNameOfSchema(char *);
char * readMetadataOfAttribute(char *);
char * readDataOfAttributeKey(char *);
char * getDataOfSingleAttribute(char *, int );
// char ** getNamesOfAttributes(char *, int );
char * extractName(char *);
char * readDataOfFreePageSlot(char *);
char ** getNamesOfAttributes(char *, int );
RC rc;

/* -----------Table and Record manager functions begins -------------------------*/

//Initializing a record manager
RC initRecordManager (void *mgmtData){
    initStorageManager(); //calling a function from storage manager which is used in manipulating page files
    return RC_OK;
}

//Shutting down the record manager
RC shutdownRecordManager (){

    if(pagehandler == ((char *)0)){
        return RC_OK;
    }
    else{
        free(pagehandler); //free the memory associated with buffer manager
        return RC_OK;
    }
}

//creating a table
RC createTable (char *name, Schema *schema){
	//passing name of the table. it will create a table with this file name
	//schema contains other information which needed to create a table
    

    if(createPageFile(name) == RC_OK){
    pagehandler = (SM_PageHandle) malloc(PAGE_SIZE);

    //All Metadata about the table will be written to page 0;
    char metadataOfTabel[PAGE_SIZE];
    int sizeOfRecord;
    int i = 0;
    memset(metadataOfTabel,'\0',PAGE_SIZE);//If file exists then Setting the allocated memory block by \0 using memset functions

    sprintf(metadataOfTabel,"%s|",name);

    sizeOfRecord = getRecordSize(schema);
    sprintf(metadataOfTabel+ strlen(metadataOfTabel),"%d[",(*schema).numAttr);
    
   while(i<(*schema).numAttr){
        sprintf(metadataOfTabel+ strlen(metadataOfTabel),"(%s:%d~%d)",(*schema).attrNames[i],(*schema).dataTypes[i],(*schema).typeLength[i]);
        i++;
   } 

    sprintf(metadataOfTabel+ strlen(metadataOfTabel),"]%d{",schema->keySize);
    i=0;
    while(i<schema->keySize){
        sprintf(metadataOfTabel+ strlen(metadataOfTabel),"%d",schema->keyAttrs[i]);
        if(i<(schema->keySize-1))
            strcat(metadataOfTabel,":");
        i++;
    }

    
    strcat(metadataOfTabel,"}");

    tabledata_info.firstFreeSpaceLocation.page =1;
    tabledata_info.firstFreeSpaceLocation.slot =0;

    sprintf(metadataOfTabel+ strlen(metadataOfTabel),"$%d:%d$",tabledata_info.firstFreeSpaceLocation.page,tabledata_info.firstFreeSpaceLocation.slot);

    // sprintf(metadataOfTabel+ strlen(metadataOfTabel),"?%d?",tabledata_info.totalNumerOfRecordsInTable);

    tabledata_info.totalNumerOfRecordsInTable =0;

    sprintf(metadataOfTabel+ strlen(metadataOfTabel),"?%d?",tabledata_info.totalNumerOfRecordsInTable);

    // sprintf(metadataOfTabel+ strlen(metadataOfTabel),"$%d:%d$",tabledata_info.firstFreeSpaceLocation.page,tabledata_info.firstFreeSpaceLocation.slot);

    // sprintf(metadataOfTabel+ strlen(metadataOfTabel),"?%d?",tabledata_info.totalNumerOfRecordsInTable);

    memmove(pagehandler,metadataOfTabel,PAGE_SIZE);//copy a block of memory from a metadata of tabel to page 

    if((openPageFile(name, &filehandler) != RC_OK) || (writeBlock(0, &filehandler, pagehandler)!= RC_OK)){
        return 1;
    }


    free(pagehandler);
    return RC_OK;
    }else{
        return 1;
    }
}

//opening a table before it can execute any other operations on a table such as scanning or inserting records
RC openTable (RM_TableData *rel, char *name){
    pagehandler = (SM_PageHandle) malloc(PAGE_SIZE);

    BM_PageHandle *h = &tabledata_info.pageHandle;

    printf("By employing buffermanager we pin page 0 and read data from page 0");

    BM_BufferPool *bm = &tabledata_info.bufferPool;

    //we pin page 0 and read data from page 0 using buffer manager 
    initBufferPool(bm, name, 3, RS_FIFO, NULL);
    bool val_1= (pinPage(bm, h, 0) == RC_OK);

    if(val_1){
        //Data is parsed by readSchemaFromPageFile method
        readSchemaFromPageFile(rel,h);//value initialied in rel after table read from page 0
        if(unpinPage(bm,h) == RC_OK){
            return RC_OK;
        }else {
            RC_message = "Unpin page failed "; //unpin page
            return RC_UNPIN_PAGE_FAILED;
        }
    }else{
        RC_message = "Pin page failed "; //if page is not pinned then show error
        return RC_PIN_PAGE_FAILED;
    }

}

//closing a table will cause all outstanding changes to the table to be written to the page 0.
RC closeTable (RM_TableData *rel) //input parameter contais all imformation related to schema
{
    char metadataOfTabel[PAGE_SIZE];
    char *pageData;   //used to hangle page data ,
    BM_PageHandle *page = &tabledata_info.pageHandle;
    BM_BufferPool *bm = &tabledata_info.bufferPool;
    int sizeOfRecord;
    memset(metadataOfTabel,'\0',PAGE_SIZE); //Setting the allocated memory block by \0 using memset functions

    sprintf(metadataOfTabel,"%s|",(*rel).name);

    sizeOfRecord = tabledata_info.sizeOfRecord; //getting size of the record
    sprintf(metadataOfTabel+ strlen(metadataOfTabel),"%d[",rel->schema->numAttr);

    int i =0;
    while(i<rel->schema->numAttr){
              sprintf(metadataOfTabel+ strlen(metadataOfTabel),"(%s:%d~%d)",rel->schema->attrNames[i],rel->schema->dataTypes[i],rel->schema->typeLength[i]);
                i++;
    }
        sprintf(metadataOfTabel+ strlen(metadataOfTabel),"]%d{",rel->schema->keySize);
    i=0;
    while(i<rel->schema->keySize){
        sprintf(metadataOfTabel+ strlen(metadataOfTabel),"%d",rel->schema->keyAttrs[i]);
        bool x = (i<(rel->schema->keySize-1));
        if(x){
            strcat(metadataOfTabel,":");
        }
        i++;
    }

    //appending all attributes of schema using strcat
    strcat(metadataOfTabel,"}"); 

    sprintf(metadataOfTabel+ strlen(metadataOfTabel),"$%d:%d$",tabledata_info.firstFreeSpaceLocation.page,tabledata_info.firstFreeSpaceLocation.slot);

    sprintf(metadataOfTabel+ strlen(metadataOfTabel),"?%d?",tabledata_info.totalNumerOfRecordsInTable);

    if(pinPage(bm,page,0) == RC_OK)
    {
         memmove(page->data,metadataOfTabel,PAGE_SIZE); //copy a block of memory from a metadata of tabel to page data
         if( markDirty(bm,page)==RC_OK){
            if(unpinPage(bm,page)==RC_OK){
                if(shutdownBufferPool(bm) == RC_OK){
                    return RC_OK;
                }else{
                     RC_message = "Shutdown Buffer Pool Failed";
                    return RC_BUFFER_SHUTDOWN_FAILED;
                }

            }else{
                RC_message = "Unpin Page 0 failed Failed";
                return RC_UNPIN_PAGE_FAILED;
            }
         }else{
            RC_message = "Page 0 Mark Dirty Failed";
            return RC_MARK_DIRTY_FAILED;
         }

    }else{
         RC_message = "Pin page failed  ";
            return RC_PIN_PAGE_FAILED;
    }

}

//delete table functions will delete the table and all the data associated with it
RC deleteTable (char *name)//takes the name of to be deleted page as an argument
{

    BM_BufferPool *bm = &tabledata_info.bufferPool;

    if(name != ((char *)0)){
        if(destroyPageFile(name) == RC_OK){
            return RC_OK;
        }else{
            RC_message = "Destroyt Page File Failed";
        return RC_FILE_DESTROY_FAILED;
        }
    }
    else{
        RC_message = "Table name can not be null ";
        return RC_NULL_IP_PARAM;
    }

}

//returns the total numbers of records in table
int getNumTuples (RM_TableData *rel)
{
    int x = tabledata_info.totalNumerOfRecordsInTable;
    return x;
}
/* -----------Table and Record manager functions ends -------------------------*/


/* -----------Record functions begins -------------------------*/

//inserts the record passed in input parameter at avialable page and slot
RC insertRecord (RM_TableData *rel, Record *record)
{
    int sizeOfRecord,freePageNum,freeSlotNum,blockfactor,offset;
    char *pageData;   //used to handle page data
    
    BM_PageHandle *page = &tabledata_info.pageHandle;
    BM_BufferPool *bm = &tabledata_info.bufferPool;
    sizeOfRecord = tabledata_info.sizeOfRecord;
    freePageNum = tabledata_info.firstFreeSpaceLocation.page;  // record will be inserted at this page number
    freeSlotNum = tabledata_info.firstFreeSpaceLocation.slot;  // record will be inserted at this slot
    blockfactor = tabledata_info.blkFctr;
    bool verify = (freePageNum < 1 || freeSlotNum < 0);

    if(!(freePageNum < 1 || freeSlotNum < 0))
    {
        if(pinPage(bm,page,freePageNum) == RC_OK)
        {
        pageData = (*page).data;  // assigning pointer from h to pagedata for convineint

        offset =  freeSlotNum * sizeOfRecord; //staring postion of record; slot Start from 0 position

        (*record).data[sizeOfRecord-1]='$';
        memcpy(pageData+offset, record->data, sizeOfRecord);//copy to be inserted record data to paga data 


        if(markDirty(bm,page)==RC_OK){
            if(unpinPage(bm,page)==RC_OK){
                (*record).id.page = freePageNum;  // storing page number for record
                (*record).id.slot = freeSlotNum;  // storing slot number for record

                //updating total number of records in a table
                tabledata_info.totalNumerOfRecordsInTable = tabledata_info.totalNumerOfRecordsInTable +1;

            //updating next available page and slot after record inserted into file

            freeSlotNum ==(blockfactor-1) ? (tabledata_info.firstFreeSpaceLocation.slot =0 ): (tabledata_info.firstFreeSpaceLocation.slot = (freeSlotNum +1));
            if(freeSlotNum ==(blockfactor-1)){tabledata_info.firstFreeSpaceLocation.page=freePageNum+1;}
                 return RC_OK;
            }else{
                RC_message = "Unpin Page failed Failed";
                return RC_UNPIN_PAGE_FAILED;
            }
        }else{
            RC_message = "Page Mark Dirty Failed";
            return RC_MARK_DIRTY_FAILED;
        }

        }else{
            RC_message = "Pin page failed  ";
        return RC_PIN_PAGE_FAILED;
        }
    }else if(verify){
        RC_message = "Invalid page|Slot number ";
        return RC_IVALID_PAGE_SLOT_NUM;
    }
}

//it delete the record of passed input parameter id
RC deleteRecord (RM_TableData *rel, RID id) //input arguments rel contains all information of schema and Id contains page number and slot number
{
    int sizeOfRecord,recordPageNumber,recordSlotNumber,blockfactor,recordOffet;
    sizeOfRecord = tabledata_info.sizeOfRecord;
    blockfactor = tabledata_info.blkFctr;
    BM_PageHandle *page = &tabledata_info.pageHandle;
    BM_BufferPool *bm = &tabledata_info.bufferPool;
    recordPageNumber = id.page;  // record will be searched at this page number
    recordSlotNumber = id.slot;  // record will be searched at this slot

    if(pinPage(bm,page,recordPageNumber) == RC_OK){
        recordOffet = recordSlotNumber * sizeOfRecord;
        memset((*page).data+recordOffet, '\0', sizeOfRecord);  // setting data of page values to null by \0 using memset functions

        tabledata_info.totalNumerOfRecordsInTable = tabledata_info.totalNumerOfRecordsInTable -1;  // updating total number of record by after deleting record

        if(markDirty(bm,page)==RC_OK){
            if(unpinPage(bm,page)==RC_OK){
                 return RC_OK;
            }else{
                RC_message = "Unpin Page failed Failed";
                return RC_UNPIN_PAGE_FAILED;
            }
        }else{
            RC_message = "Page Mark Dirty Failed";
            return RC_MARK_DIRTY_FAILED;
        }
    }else{
        RC_message = "Pin page failed  ";
        return RC_PIN_PAGE_FAILED;
    }

}

//it will update the particular record at page and slot metioned in record
RC updateRecord (RM_TableData *rel, Record *record)//input parameters contains all information of schema and new record to be updated into file
{
    int sizeOfRecord,recordPageNumber,recordSlotNumber,blockfactor,recordOffet;
    sizeOfRecord = tabledata_info.sizeOfRecord;
    blockfactor = tabledata_info.blkFctr;
    BM_PageHandle *page = &tabledata_info.pageHandle;
    BM_BufferPool *bm = &tabledata_info.bufferPool;
    recordPageNumber = (*record).id.page;  // record will be searched at this page number
    recordSlotNumber = (*record).id.slot;  // record will be searched at this slot
    // // sizeOfRecord-1 bacause last value in record which is set by us is $

    if(pinPage(bm,page,recordPageNumber) == RC_OK)
    {
        recordOffet = recordSlotNumber * sizeOfRecord;

         memcpy(page->data+recordOffet, record->data, sizeOfRecord-1); //copy to be updated record data to paga data 
        // sizeOfRecord-1 bacause last value in record which is set by us is $
        if(markDirty(bm,page)==RC_OK){
            if(unpinPage(bm,page)==RC_OK){
                 return RC_OK;
            }else{
                 RC_message = "Unpin Page failed Failed";
                    return RC_UNPIN_PAGE_FAILED;
                }
            }else{
              RC_message = "Page Mark Dirty Failed";
             return RC_MARK_DIRTY_FAILED;
        }

    }else{
        RC_message = "Pin page failed  ";
        return RC_PIN_PAGE_FAILED;
    }
}


//it will return the record of particular page number and slot number mentioned in input parameter id
RC getRecord (RM_TableData *rel, RID id, Record *record)//input parameters contains all information of schema and page number and slot id to be read from file and a pointer to record data
{
    int sizeOfRecord,recordPageNumber,recordSlotNumber,blockfactor,recordOffet;
    sizeOfRecord = tabledata_info.sizeOfRecord;
    blockfactor = tabledata_info.blkFctr;
    BM_PageHandle *page = &tabledata_info.pageHandle;
    BM_BufferPool *bm = &tabledata_info.bufferPool;
    recordPageNumber = id.page;  // record will be searched at this page number
    recordSlotNumber = id.slot;  // record will be searched at this slot


if(pinPage(bm,page,recordPageNumber) == RC_OK){
    recordOffet = recordSlotNumber * sizeOfRecord;  // it gives starting point of record
    memcpy((*record).data, (*page).data+recordOffet, sizeOfRecord); //copy data from page file to record data. also checks boundry condition for reccord->data size
    
    (*record).data[sizeOfRecord-1]='\0';
    (*record).id.page = recordPageNumber;
    (*record).id.slot = recordSlotNumber;
    if(unpinPage(bm,page)==RC_OK){
        return RC_OK;
    }else{
        RC_message = "Unpin Page failed Failed";
        return RC_UNPIN_PAGE_FAILED;
    }
}else{
    RC_message = "Pin page failed  ";
        return RC_PIN_PAGE_FAILED;
}

}

/* -----------Record functions ends -------------------------*/

/* -----------scan functions begins -------------------------*/
// scan functions are used to retrieve all tuples from a table that fulfill a certain condition

//initializes the RM_ScanHandle data structure passed as an argument to startScan
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)//input parameters contains all the information related to schema and scan, and condition to scan the data
{
	//initializes the RM_ScanHandle data structure passed as an argument to startScan
    BM_BufferPool *bm = &tabledata_info.bufferPool;

    (*scan).rel = rel;
    rmscandata_mgmt.cond=cond;
    rmscandata_mgmt.recID.page=1; // records starts from page 1
    rmscandata_mgmt.recID.slot=0; // slot starts from 0
    rmscandata_mgmt.count = 0;

    (*scan).mgmtData = &rmscandata_mgmt;

    return RC_OK;
}

//next method should return the next tuple that fulfills the scan condition.
RC next (RM_ScanHandle *scan, Record *record)
{
	//check condition for no more tuple available in table

    if(!(tabledata_info.totalNumerOfRecordsInTable < 1 || rmscandata_mgmt.count==tabledata_info.totalNumerOfRecordsInTable))
    {    
        int blockfactor,totalTuple,currentTotalRecordScan,currentPageScan,currentSlotScan;
        blockfactor = tabledata_info.blkFctr;
        totalTuple = tabledata_info.totalNumerOfRecordsInTable;
        BM_PageHandle *page = &tabledata_info.pageHandle;
        BM_BufferPool *bm = &tabledata_info.bufferPool;

        currentTotalRecordScan = rmscandata_mgmt.count; //scanning start from current count to total no of records
        currentPageScan = rmscandata_mgmt.recID.page;  // scanning will start from current page till record from last page encountered
        currentSlotScan = rmscandata_mgmt.recID.slot;  // scanning will start from current slot till record encountered
    
        rmscandata_mgmt.count = rmscandata_mgmt.count +1 ;
        Value *queryExpResult = (Value *) malloc(sizeof(Value));
        // rmscandata_mgmt.count = rmscandata_mgmt.count +1 ;

        //This will will scan every record. after reading record it checks the condition and if condition is not satisfied then again the scan passed as parameter until the total record scan is less than the total number of tuple
        //next should return RC_RM_NO_MORE_TUPLES once the scan is completed and RC_OK otherwise (unless an error occurs of course)

        while(currentTotalRecordScan<totalTuple)
        {

            rmscandata_mgmt.recID.page= currentPageScan;
            rmscandata_mgmt.recID.slot= currentSlotScan;
            bool val_2 = (getRecord(scan->rel,rmscandata_mgmt.recID,record) != RC_OK);

            if(val_2){
                RC_message="Record reading failed";
            }
            currentTotalRecordScan = currentTotalRecordScan+1;   // increment current total recoed scan counter by 1


            if(rmscandata_mgmt.cond == NULL){
                  queryExpResult->v.boolV = TRUE; // if no condition is mentioned then it will return all records
            }else{
                evalExpr(record, (scan->rel)->schema, rmscandata_mgmt.cond, &queryExpResult);
                if((*queryExpResult).v.boolV ==1)
                {
                    (*record).id.page=currentPageScan;
                    (*record).id.slot=currentSlotScan;
                     currentSlotScan ==(blockfactor-1) ? (currentSlotScan = 0) : (currentSlotScan = currentSlotScan +1);
                    if(currentSlotScan ==(blockfactor-1)){currentPageScan =currentPageScan +1  ;}
                    rmscandata_mgmt.recID.page= currentPageScan;
                    rmscandata_mgmt.recID.slot= currentSlotScan;
                    return RC_OK;
                }
            }
            

            if(currentSlotScan ==(blockfactor-1)){currentPageScan =currentPageScan +1  ;}
            currentSlotScan ==(blockfactor-1) ? (currentSlotScan = 0) : (currentSlotScan = currentSlotScan +1);
            
        }

        (*queryExpResult).v.boolV = TRUE;
        rmscandata_mgmt.recID.page=1; // records starts from page 1
        rmscandata_mgmt.recID.slot=rmscandata_mgmt.count=0; // slot starts from 0
        // rmscandata_mgmt.count = 0;
        return  RC_RM_NO_MORE_TUPLES;
        }else{
            return  RC_RM_NO_MORE_TUPLES;
        } 
}

//cloese scan will close the scan and reset all the information in RM_SCANDATA_MANAGEMENT
RC closeScan (RM_ScanHandle *scan)
{
    rmscandata_mgmt.recID.page=1; // records starts from page 1
    rmscandata_mgmt.recID.slot=rmscandata_mgmt.count= 0; // slot starts from 0
    // rmscandata_mgmt.count = 0;
    return RC_OK;
}

/* -----------scan functions ends -------------------------*/


/* -----------schema functions begins -------------------------*/

//this function will return the size of record based on length of each field and data type
int getRecordSize (Schema *schema)
{
    
   
    if(schema !=((Schema *)0))
    {
    int sizeOfRecord = 0;

    int i=0;
    while(i<schema->numAttr){
        if((*schema).dataTypes[i] == DT_FLOAT){
             sizeOfRecord = sizeOfRecord + sizeof(float);
        }else if((*schema).dataTypes[i] == DT_BOOL){
             sizeOfRecord = sizeOfRecord + sizeof(bool);
        }else if((*schema).dataTypes[i] == DT_INT){
            sizeOfRecord = sizeOfRecord + sizeof(int);
        }else if((*schema).dataTypes[i] == DT_STRING){
            sizeOfRecord = sizeOfRecord + (sizeof(char) * schema->typeLength[i]);
        }
        i++;
    }
    return sizeOfRecord; // it will return the size of record
}else{//if schema is not intialized then it will show that you need to create schema
    RC_message = "schema is not initialized.first you need to create schema";
    return RC_SCHEMA_NOT_INIT;
}
    
}


//this function will create schema with given input parameter such as attribute number,name,data type,length of attribute, number of keys and value of keys
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){

    Schema *schema = (Schema * ) malloc(sizeof(Schema));//allocate memory to schema

   

    if(schema !=((Schema *)0)){
        //set the parameter of new schema based on the input parameters
        int sizeOfRecord;
        (*schema).numAttr = numAttr;
        (*schema).attrNames = attrNames;
        (*schema).dataTypes = dataTypes;
        (*schema).typeLength = typeLength;
        (*schema).keySize = keySize;
        (*schema).keyAttrs = keys;
        sizeOfRecord = getRecordSize(schema);
        tabledata_info.sizeOfRecord = sizeOfRecord;
        tabledata_info.totalNumerOfRecordsInTable = 0;

        return schema;//returns the schema
    }else{
        RC_message = "dynamic memory allocation failed | schema";
        return RC_MELLOC_MEM_ALLOC_FAILED;
    }
}

//this function will free the memory which was allocated to schema
RC freeSchema (Schema *schema){
    printf("-------------------------------freeSchema in process -------------------------------");
    free(schema);
    return RC_OK;
}

/* -----------schema functions begins -------------------------*/

/* -----------Attribute functions begins -------------------------*/

//createRecord function is used to create a new record with all the null values
RC createRecord (Record **record, Schema *schema) //input parameters are pointer to newly created record and all attribues of schema
{

    Record *newRecord = (Record *) malloc (sizeof(Record));//allocating memory for new record
    
    if(newRecord != ((Record *)0))
    {
        (*newRecord).data = (char *)malloc(sizeof(char) * tabledata_info.sizeOfRecord);
        memset((*newRecord).data,'\0',sizeof(char) * tabledata_info.sizeOfRecord);

        (*newRecord).id.page =-1;           //set to -1 bcz it has not inserted into table/page/slot
        (*newRecord).id.page =-1;           //set to -1 bcz it has not inserted into table/page/slot

        *record = newRecord;//assignning a new record

        return RC_OK;
    }else{
        RC_message = "dynamic memory allocation failed | Record";
        return RC_MELLOC_MEM_ALLOC_FAILED;
    }
}

//freeRecord function will free the memory related to record
RC freeRecord (Record *record)
{
   
    if(record != ((Record *)0)){
        if((*record).data != ((char *)0))
            free((*record).data);//free the record data
        free(record);//free the space of record
        return RC_OK;
    }else{
        RC_message = " Record is  null";
        return RC_NULL_IP_PARAM;
    }
}

//getAttr function returns the value of attribute pointed by atttrnum
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)//input parameter contains record data,schema attributes,attribute number which is to be retrived and values to be return
{
    int offset,integer_attribute,attribute_size;
    float float_attribute;
    char *sub_string ;
    offset = getAttributeOffsetInRecord(schema,attrNum);//getAttributeOffsetInRecord function returns offset of particular attribute number
    Value *l_rec;     
    attribute_size =0;

    if((*schema).dataTypes[attrNum] == DT_FLOAT){
            attribute_size = sizeof(float);
            sub_string= malloc(attribute_size+1);     // one extra byte to store '\0' char
            memcpy(sub_string, (*record).data+offset, attribute_size);//copy data from record data to sub_string
            sub_string[attribute_size]='\0';          // set last byet to '\0'
            float_attribute =  atof(sub_string);//converts the string argument sub_string to a float
            MAKE_VALUE(*value, DT_FLOAT, float_attribute);
            free(sub_string);
    }else if((*schema).dataTypes[attrNum] == DT_BOOL){
            attribute_size = sizeof(bool);
            sub_string= malloc(attribute_size+1);     // one extra byte to store '\0' char
            memcpy(sub_string, (*record).data+offset, attribute_size);//copy data from record data to sub_string
            sub_string[attribute_size]='\0';          // set last byet to '\0'
            integer_attribute =  atoi(sub_string);//converts the string argument sub_string to an integer
            MAKE_VALUE(*value, DT_BOOL, integer_attribute);
            free(sub_string);
    }else if((*schema).dataTypes[attrNum] == DT_INT){
            attribute_size = sizeof(int);
            sub_string= malloc(attribute_size+1);     // one extra byte to store '\0' char
            memcpy(sub_string, (*record).data+offset, attribute_size);//copy data from record data to sub_string
            sub_string[attribute_size]='\0';          // set last byet to '\0'
            integer_attribute =  atoi(sub_string);//converts the string argument sub_string to an integer
            MAKE_VALUE(*value, DT_INT, integer_attribute);
            free(sub_string);
    }else if((*schema).dataTypes[attrNum] == DT_STRING){
            attribute_size =sizeof(char)*schema->typeLength[attrNum];
            sub_string= malloc(attribute_size+1);    // one extra byte to store '\0' char
            memcpy(sub_string, (*record).data+offset, attribute_size);//copy data from record data to sub_string
            sub_string[attribute_size]='\0';       // set last byet to '\0'
            MAKE_STRING_VALUE(*value, sub_string);
            free(sub_string);
            }

    return RC_OK;

    return RC_OK;
}

void strRepInt(int j,int val,  char *intStr){
    int i,q,last;
    q = val;
    last = j;
    i = 0;
    while (q > 0 && j >= 0) {
        i = q % 10;
        intStr[j] += i;
        q = q / 10;
        // intStr[j] = intStr[j] + i;
        j--;
    }
    int comp = (last+1);
    intStr[comp] = '\0';

}


//setAttr functions will set value of particular attribute given in attrNum
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)//input parameters are pointers to the record data,schema attributes, attribute number whose value needs to be changed and new value of attribute
{

    int offset,remainder,quotient,k,j,number;
    offset = getAttributeOffsetInRecord(schema,attrNum);//getAttributeOffsetInRecord function returns offset of particular attribute number
    remainder = 0;
    quotient = 0;
    k=0;
    bool q,r;
    char intStr[sizeof(int)+1];
    char intStrTemp[sizeof(int)+1];
    memset(intStr,'0',sizeof(char)*4);
    char *hexValue ="0001";
    number = (int)strtol(hexValue, NULL, 16);

    if((*schema).dataTypes[attrNum] == DT_FLOAT){
            sprintf((*record).data + offset,"%f" ,(*value).v.floatV);
    }else if((*schema).dataTypes[attrNum] == DT_BOOL){
            strRepInt(1,(*value).v.boolV,intStr);
            sprintf((*record).data + offset,"%s" ,intStr);
    }else if((*schema).dataTypes[attrNum] == DT_INT){
            strRepInt(3,(*value).v.intV,intStr);
            sprintf((*record).data + offset, "%s", intStr);
    }else if((*schema).dataTypes[attrNum] == DT_STRING){
            int strLength =(*schema).typeLength[attrNum];
            sprintf((*record).data + offset, "%s", (*value).v.stringV);
    }


    return RC_OK;
}


//function to extract total number of records from page
int extractTotalRecordsTab(char *schemaData){
    char *attributeData = (char *) malloc(sizeof(char)*10);
    int i=0,j;
    memset(attributeData,'\0',sizeof(char)*10);
    // int i=0;
    while(schemaData[i] != '?'){
        i++;
    }
    i++;
    j=0;
    while(schemaData[i] != '?'){
        attributeData[j] = schemaData[i++];
        j++;
    }
    
    attributeData[j]='\0';
    return atoi(attributeData);//returns the total number of record from page
}


int readTotalAttributes(char *schemaData)
{
    char *strNumberOfAttribute = (char *) malloc(sizeof(int)*2);
    int i = 0,j=0;
    memset(strNumberOfAttribute,'\0',sizeof(int)*2);
    
    while(schemaData[i] != '|'){
        i++;
    }
   
    i++;
    while(schemaData[i] != '['){
        int cv = j++;
        strNumberOfAttribute[cv]=schemaData[i++];
    }

    strNumberOfAttribute[j]='\0';
    printf("Return string to integer converter of total attributes");
    return atoi(strNumberOfAttribute);//return string to integer converter of total attributes
}

//function to read total number of key attributes from page file

int readTotalNumberOfKeyAttribute(char *schemaData)
{
    char *strNumberOfAttribute = (char *) malloc(sizeof(int)*2);
    int i=0,j=0;
    memset(strNumberOfAttribute,'\0',sizeof(int)*2);
   
    while(schemaData[i] != ']'){
        i++;
    }
    
    i++;
    while(schemaData[i] != '{'){
        int cm = j++;
        strNumberOfAttribute[cm]=schemaData[i++];
    }
    strNumberOfAttribute[j]='\0';
    return atoi(strNumberOfAttribute);//return string to integer converter of total number of key attributes
}

//function to read data type of attribute from page file
int * getDatatypeOfAttribute(char *schemaData, int numAtr)
{
    int i = 0;
    int *data_type=(int *) malloc(sizeof(int) *numAtr);
    while(i<numAtr){
        char *atrDt =getDataOfSingleAttribute(schemaData,i);
        data_type[i]  = extractDataType(atrDt);

        free(atrDt);
        i++;
    }

    
    return data_type;//return data type of attribute which was read from page file
}



/* -----------Attribute functions ends -------------------------*/

//updateScan function will update the record based on the scan condition
RC updateScan (RM_TableData *rel, Record *record, Record *updaterecord, RM_ScanHandle *scan) //input parameters are scheme details, record data to be returned, pointer to the updated record,other informatoin related to scan
{

    while((rc = next(scan, record)) == RC_OK)
    {
        (*updaterecord).id.page=record->id.page;
        (*updaterecord).id.slot=record->id.slot;
        printf("Based on the scan condition this method will update the record");
        updateRecord(rel,updaterecord);

    }
    return RC_OK;
}

//readSchemaFromPageFile method parse the data calles to further method to read ans parse next record
void readSchemaFromPageFile(RM_TableData *rel, BM_PageHandle *h) //input parameters are schema details and pointer to page data
{
   char metadata[PAGE_SIZE];
    strcpy(metadata,(*h).data);//copy page handle data to metadata

    char *schema_name=readNameOfSchema(metadata); //calling readNameOfSchema function to get schema name

    int totalAtribute = readTotalAttributes(metadata);//calling readTotalAttributes function to get total Atribute

    char *atrMetadata =readMetadataOfAttribute(metadata);//calling readMetadataOfAttribute function to get attribute Metadata

    DataType *dt =   (DataType *)getDatatypeOfAttribute(atrMetadata,totalAtribute);//calling getDatatypeOfAttribute function to get data type

    char **names=getNamesOfAttributes(atrMetadata,totalAtribute);//calling getNamesOfAttributes functions to get names

    // DataType *dt =   (DataType *)getDatatypeOfAttribute(atrMetadata,totalAtribute);//calling getDatatypeOfAttribute function to get data type

    int *sizes = getSizeOfAttribute(atrMetadata,totalAtribute);//calling getSizeOfAttribute function to get size

    char **cpNames = (char **) malloc(sizeof(char*) * totalAtribute);
    int *cpSizes = (int *) malloc(sizeof(int) * totalAtribute);
    DataType *cpDt = (DataType *) malloc(sizeof(DataType) * totalAtribute);
    // int *cpSizes = (int *) malloc(sizeof(int) * totalAtribute);

    int totalKeyAtr = readTotalNumberOfKeyAttribute(metadata);//calling readTotalNumberOfKeyAttribute function to get total Key Attribute

    char *atrKeydt = readDataOfAttributeKey(metadata);//calling readDataOfAttributeKey function to get attribute Key data

    int *keys =   extractKeyData(atrKeydt,totalKeyAtr);//calling extractKeyData function to get keys

    int *cpKeys = (int *) malloc(sizeof(int)*totalKeyAtr);

    char *freeVacSlot = readDataOfFreePageSlot(metadata);//calling readDataOfFreePageSlot function to get free Slot

    int totaltuples = extractTotalRecordsTab(metadata);//calling extractTotalRecordsTab to get total tuples

    // char **names=getNamesOfAttributes(atrMetadata,totalAtribute);//calling getNamesOfAttributes functions to get names

    // DataType *dt =   (DataType *)getDatatypeOfAttribute(atrMetadata,totalAtribute);//calling getDatatypeOfAttribute function to get data type

    // int *sizes = getSizeOfAttribute(atrMetadata,totalAtribute);//calling getSizeOfAttribute function to get size

    // int *keys =   extractKeyData(atrKeydt,totalKeyAtr);//calling extractKeyData function to get keys

    int *pageSlot = extractFirstFreePageSlot(freeVacSlot); //calling extractFirstFreePageSlot to get page slot number

    // int totaltuples = extractTotalRecordsTab(metadata);//calling extractTotalRecordsTab to get total tuples

    // char **cpNames = (char **) malloc(sizeof(char*) * totalAtribute);
    // DataType *cpDt = (DataType *) malloc(sizeof(DataType) * totalAtribute);
    // int *cpSizes = (int *) malloc(sizeof(int) * totalAtribute);
    // int *cpKeys = (int *) malloc(sizeof(int)*totalKeyAtr);
    char *cpSchemaName = (char *) malloc(sizeof(char)*20);
    int i=0;



    memset(cpSchemaName,'\0',sizeof(char)*20); //allocated memory block by \0 to cpSchemaName using memset functions

    while( i < totalAtribute){
        cpNames[i] = (char *) malloc(sizeof(char) * 10);
        strcpy(cpNames[i], names[i]);
        i++;
    }
    
    //copy data from variable which was read from page files to schema
    memcpy(cpDt, dt, sizeof(DataType) * totalAtribute);
    memcpy(cpSizes, sizes, sizeof(int) * totalAtribute);
    memcpy(cpKeys, keys, sizeof(int) * totalKeyAtr);
    memcpy(cpSchemaName,schema_name,strlen(schema_name));

    //creating schema from data which is read from files
    Schema *schema = createSchema(totalAtribute, cpNames, cpDt, cpSizes, totalKeyAtr, cpKeys);

    //free variable which was read from page file
    free(names);
    free(dt);
    free(sizes);
    free(keys);
    free(schema_name);

    // //creating schema from data which is read from files
    // Schema *schema = createSchema(totalAtribute, cpNames, cpDt, cpSizes, totalKeyAtr, cpKeys);
    (*rel).schema=schema;
    (*rel).name =cpSchemaName;


    tabledata_info.rm_tbl_data = rel;
    tabledata_info.sizeOfRecord =  getRecordSize(rel->schema) + 1;   //
    tabledata_info.blkFctr = (PAGE_SIZE / tabledata_info.sizeOfRecord);
    tabledata_info.firstFreeSpaceLocation.page =pageSlot[0];
    tabledata_info.firstFreeSpaceLocation.slot =pageSlot[1];
    tabledata_info.totalNumerOfRecordsInTable = totaltuples;

}

//function to read schema name from page file
char * readNameOfSchema(char *schemaData)
{
     int i=0;
    char *tableName = (char *) malloc(sizeof(char)*20);
    memset(tableName,'\0',sizeof(char)*20);
    while(schemaData[i] != '|'){
        tableName[i]=schemaData[i];
        i++;
    }
   
    tableName[i]='\0';
    return tableName;
}
//function to read total attributes from page file

//function to read metadata of attribute from page file
char * readMetadataOfAttribute(char *schemaData)
{
    int i=0,j=0;
    char *attributeData = (char *) malloc(sizeof(char)*100);
    memset(attributeData,'\0',sizeof(char)*100);
    
    while(schemaData[i] != '['){
        i++;
    }
    i++;
    
    while(schemaData[i] != ']'){
        attributeData[j] = schemaData[i++];
        j++;
    }
    
    attributeData[j]='\0';

    return attributeData;//return metadata of attribute which was read from page file
}

//function to read data of attribute key from page file
char * readDataOfAttributeKey(char *schemaData)
{
    int i=0,j=0;
    char *attributeData = (char *) malloc(sizeof(char)*50);
    memset(attributeData,'\0',sizeof(char)*50);
    
    while(schemaData[i] != '{'){
        i++;
    }
    i++;
   
    while(schemaData[i] != '}'){
        attributeData[j] = schemaData[i++];
        j++;
    }
    
    attributeData[j]='\0';

    return attributeData; //return data of attribute key which was read from page file
}

//function to read data of free page slot from page file
char * readDataOfFreePageSlot(char *schemaData){
    char *attributeData = (char *) malloc(sizeof(char)*50);
    int i=0,j=0;
    memset(attributeData,'\0',sizeof(char)*50);
  
    while(schemaData[i] != '$'){
        i++;
    }
    i++;
    
    while(schemaData[i] != '$'){
        attributeData[j] = schemaData[i++];
        j++;
    }
    
    attributeData[j]='\0';

    return attributeData;//return the data of free page slot which was read from page file
}

//function to read names of attributes from page file
char ** getNamesOfAttributes(char *schemaData, int numAtr){

    int i=0;
    char ** attributesName = (char **) malloc(sizeof(char)*numAtr);
    while(i<numAtr){
        char *atrDt =getDataOfSingleAttribute(schemaData,i);
        char *name = extractName(atrDt);
        printf("Read names of attributes from page file");
        attributesName[i] = malloc(sizeof(char) * strlen(name));
        strcpy(attributesName[i],name);
        i++;
        free(name);
        free(atrDt);
        // i++;
    }

    
    return attributesName; //return the names of attributes which was read from page file
}


//function to read size of attribute from page file
int * getSizeOfAttribute(char *schemaData, int numAtr)
{
    int i = 0;
    int *data_size= (int *) malloc(sizeof(int) *numAtr);
    while(i<numAtr){
        char *atrDt =getDataOfSingleAttribute(schemaData,i);
        data_size[i]  = extractTypeLength(atrDt);
     
        free(atrDt);
        i++;
    }


    return data_size;//return data size of attribute which was read from page file
}
//function to get data of single attribute from page file
char * getDataOfSingleAttribute(char *schemaData, int atrNum)
{
    char *attributeData = (char *) malloc(sizeof(char)*30);
    int count=0,i=0,j=0;
    
    while(count<=atrNum){
        if(schemaData[i++] == '(')
            count++;
    }
    
    
    while(schemaData[i] != ')'){
         attributeData[j] = schemaData[i++];
         j++;
    }
    
    attributeData[j]='\0';
    return attributeData; ////return data of single attribute which was read from page file
}

//function to extract name 
char * extractName(char *data)
{
    char *name = (char *) malloc(sizeof(char)*10);
    int i=0;
    memset(name,'\0',sizeof(char)*10);
    
    while(data[i]!=':'){
        name[i] = data[i];
        i++;
    }
    
    name[i]='\0';
    return  name; //return the extracted name
}

//function to extract data type
int extractDataType(char *data)
{
    char *dtTp = (char *) malloc(sizeof(int)*2);
    int i=0,j=0,dt;
    memset(dtTp,'\0',sizeof(char)*10);

    while(data[i]!=':'){
        i++;
    }
    i++;
   
    while(data[i]!='~'){
        dtTp[j]=data[i++];
        j++;
    }
   
    dtTp[j]='\0';
    dt =atoi(dtTp);
    free(dtTp);
    return  dt; //return data type
}

//function to extract length of data type
int extractTypeLength(char *data)
{
    char *datatypeLength = (char *) malloc(sizeof(int)*2);
    int i=0,j=0,dt;
    memset(datatypeLength,'\0',sizeof(char)*10);

    while(data[i]!='~'){
        i++;
    }

    i++;
    while(data[i]!='\0'){
        datatypeLength[j]=data[i++];
        j++;
    }
 
    datatypeLength[j]='\0';
    dt =atoi(datatypeLength);
    free(datatypeLength);
    return  dt; // retuns the length of data type
}
//function to extract key data
int * extractKeyData(char *data,int keyNum)
{
    int i=0,j=0,k=0;
    char *val = (char *) malloc(sizeof(int)*2);
    memset(val,'\0',sizeof(int)*2);
    int * values=(int *) malloc(sizeof(int) *keyNum);
    
   while(data[k]!='\0'){
     if(data[k]!=':'){
        val[i++] = data[k];
    }else{
        values[j]=atoi(val);
            memset(val,'\0',sizeof(int)*2);
            j++;
            i=0;
    }
    k++;
   }

    values[keyNum-1] =atoi(val);
    return  values; //return the value of key data
}

//function to extract first free page slot
int * extractFirstFreePageSlot(char *data)
{
    int i=0,j=0,k=0;
    char *val = (char *) malloc(sizeof(int)*2);
    memset(val,'\0',sizeof(int)*2);
    int * values=(int *) malloc(sizeof(int) *2);
    while(data[k]!='\0'){
        if(data[k]!=':'){
            val[i++] = data[k];
        }else{
            values[j]=atoi(val);
            memset(val,'\0',sizeof(int)*2);
            j++;
            i=0;
        }
        k++;
    }
    
    values[1] =atoi(val);
    printf("\n Slot %d",values[1]);
    return  values;//returns the value of first free page slot
}



//getAttributeOffsetInRecord function returns offset of particular attribute
int getAttributeOffsetInRecord(Schema *schema, int atrnum){
    int offset,pos;
    offset=0;
    pos=0;
    while(pos<atrnum){
        if((*schema).dataTypes[pos] == DT_INT){
                offset = offset + sizeof(int);
        }else if((*schema).dataTypes[pos] == DT_STRING){
            offset = offset + (sizeof(char) *  schema->typeLength[pos]);
        }else if((*schema).dataTypes[pos] == DT_FLOAT){
            offset = offset + sizeof(float);
        }else if((*schema).dataTypes[pos] == DT_BOOL){
            offset = offset  + sizeof(bool);
        }
        pos++;
    }
    return offset;
}

//function to print record
void printRecord(char *record, int recordLength){
    int i=0;
    while(i<recordLength){
        printf("%c",record[i]);
        i++;
    }
}

//function to print Record deatils 
void printTableInfoDetails(TableData_information *tab_info)
{
    printf(" \n Printing record details ");
    printf("<--------------------- Information of Table------------------------> ");
    printf(" \n table name [%s]",tab_info->rm_tbl_data->name);
    printf(" \n Size of record [%d]",(*tab_info).sizeOfRecord);
    printf(" \n total Records in page (blkftr) [%d]",(*tab_info).blkFctr);
    printf(" \n total Attributes in table [%d]",tab_info->rm_tbl_data->schema->numAttr);
    printf(" \n total Records in table [%d]",(*tab_info).totalNumerOfRecordsInTable);
    printf(" \n next available page and slot [%d:%d]",tab_info->firstFreeSpaceLocation.page,tab_info->firstFreeSpaceLocation.slot);
}

//function to print data of page
void printPageData(char *pageData)
{
    printf("\n Prining page Data ==>");
    int i=0;
    while(i<PAGE_SIZE){
        printf("%c",pageData[i]);
        i++;
    }
    printf("\n exiting ");
}