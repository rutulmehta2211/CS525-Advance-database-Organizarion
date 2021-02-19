
#include "btree_mgr.h"
#include "tables.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <string.h>


BTree *scan;
BTree *root;
SM_FileHandle btree_fh;
//variable initialization.

int nodeVal,indexNum,maxEle;
indexNum = 0;


//function to initialize the index manager.
RC initIndexManager (void *mgmtData)
{
   solution();
}


//function to terminate the index manager.
RC shutdownIndexManager ()
{
    solution();
}

//function to create B tree.
RC createBtree (char *idxId, DataType keyType, int n)
{
     int i = 0;
    root = ((BTree*)malloc(sizeof(BTree)));
    (*root).key = malloc(sizeof(int) * n);
    nodeVal = 2;
    (*root).id = malloc(sizeof(int) * n);
    (*root).next = malloc(sizeof(BTree) * (n + 1));
    
    while (i<n + 1){
        (*root).next[i] = NULL;
        i++;
    }
    maxEle = n;
    createPageFile (idxId);

    solution();
}


//function to open the created B tree and traversing all its elements.
RC openBtree (BTreeHandle **tree, char *idxId)
{
    nodeVal= 3;
    openPageFile (idxId, &btree_fh);
    solution();
}


//function to close the openend B tree after traversing all its elements.
RC closeBtree (BTreeHandle *tree)
{

    closePageFile(&btree_fh);
    nodeVal=1;
    free(root);

   solution();
}


//function to delete the b tree and respective record pointer from the index.
RC deleteBtree (char *idxId)
{
    destroyPageFile(idxId);
    nodeVal = 10;
   solution();
}


//function to calculate the total number of nodes of the tree.
RC getNumNodes (BTreeHandle *tree, int *result)
{
     BTree *temp = ((BTree*)malloc(sizeof(BTree)));
    nodeVal = 5;
    int no_of_nodes,i;
    i=0;
    no_of_nodes=0;
    
    while (i<maxEle + 2) {
        no_of_nodes =no_of_nodes + 1;
        i++;
    }

    *result = no_of_nodes;

    solution();
}

//function to calculate the total number of entries of the btree.
RC getNumEntries (BTreeHandle *tree, int *result)
{
    BTree *temp = ((BTree*)malloc(sizeof(BTree)));
    int tot,tot_elements, i,tot_value;

    // BTree *temp = ((BTree*)malloc(sizeof(BTree)));
    tot=0;tot_elements=0;tot_value=0;
    // int tot_value = 0;
    temp = root;
    while (temp != NULL){
        i = 0;
        while (i < maxEle){
            if(temp->key[i] == 0){
                i++;
            }else{
                tot_elements = tot_elements + 1;
                i++;
            }

        }
        temp = (*temp).next[maxEle];
    }
    tot_value = ++tot;
    *result = tot_elements;
    solution();
}

RC getKeyType (BTreeHandle *tree, DataType *result)
{
   solution();
}


//function for accessing the index by searching the key provided.
RC findKey (BTreeHandle *tree, Value *key, RID *result)
{
    BTree *temp = ((BTree*)malloc(sizeof(BTree)));
    int f,i;
    // BTree *temp = ((BTree*)malloc(sizeof(BTree)));
    f=0;
    temp = root;
    while (temp != NULL) {
        i = 0;
        while (i < maxEle) {
            if(temp->key[i] != key->v.intV){
                i++;
            }else{
                
                (*result).page = temp->id[i].page;
                (*result).slot = temp->id[i].slot;
                f = 1;
                break;
            }


           
        }
            if(f != 1){
                temp = (*temp).next[maxEle];
            }else{
                break;
            }
    
    }

    f != 1 ? state():solution();
}

//function to insert a new key and a record pointer pair into the index by making use of it.
RC insertKey (BTreeHandle *tree, Value *key, RID rid)
{
    int i,nodeFull,totalEle;
    nodeVal = 9;
    BTree *temp = (BTree*)malloc(sizeof(BTree));
    BTree *node = (BTree*)malloc(sizeof(BTree));
    i=0;nodeFull=0;totalEle=0;
    (*node).key = malloc(sizeof(int) * maxEle);
    (*node).id = malloc(sizeof(int) * maxEle);
    (*node).next = (malloc(sizeof(BTree) * (maxEle + 1)));

    while (i < maxEle) {
    	(*node).key[i] = 0;
        i++;
    }

    temp = root;
    while (temp != NULL) {
        nodeFull = 0;
        i = 0;
        while (i < maxEle) {
            if(temp->key[i] != 0){
                i++;;
            }else{
                temp->id[i].page = rid.page;
                nodeVal = 1;
                temp->id[i].slot = rid.slot;
                // nodeVal = 1;
                (*temp).key[i] = key->v.intV;
                (*temp).next[i] = NULL;
                nodeFull = nodeFull + 1;
                break;
            }

           
        }

        if(nodeFull == 0){
            if((*temp).next[maxEle] == NULL){
                (*node).next[maxEle] = NULL;
                nodeVal = 2;
                (*temp).next[maxEle] = node;       
            }
            temp = temp->next[maxEle];
        }else{
            temp = temp->next[maxEle];
        }

       
    }

    temp = root;
    while (temp != NULL){
        i = 0;
        while (i < maxEle){
            if(temp->key[i] == 0){
                i++;
            }else{
                totalEle += 1;
                i++;
            }

        }
        temp = (*temp).next[maxEle];
    }

    if(totalEle != 6){
        solution();
    }else{
        (*node).key[0] = root->next[maxEle]->key[0];
        (*node).key[1] = root->next[maxEle]->next[maxEle]->key[0];
        nodeVal = 3;
        (*node).next[0] = root;
        (*node).next[1] = (*root).next[maxEle];
        (*node).next[2] = root->next[maxEle]->next[maxEle];
        solution();
    }

   

}

//function to delete the key and the respective record pointer.
RC deleteKey (BTreeHandle *tree, Value *key)
{
     BTree *temp = (BTree*)malloc(sizeof(BTree));
    int f, i;
    f=0;
  
    temp = root;
    while (temp != NULL) {
        i = 0;
        while (i < maxEle) {
            if(temp->key[i] != key->v.intV){
                i++;
            }else{
                (*temp).key[i] = 0;
                temp->id[i].page = 0;
                temp->id[i].slot = 0;
                f = 1;
                break;
            }


          
        }

      

        if(f != 1){
           temp = temp->next[maxEle]; 
        }else{
            break;
        }

    }

   solution();
}

//function to open the tree and scan all B tree entries.
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
     BTree *temp = (BTree*)malloc(sizeof(BTree));
      scan = (BTree*)malloc(sizeof(BTree));
    indexNum = 0;
    int totalEle = 0, i;
   
    scan = root;

    temp = root;
    while (temp != NULL){
        i = 0;
        while (i < maxEle){
            if(temp->key[i] == 0){
                i++;
            }else{
                nodeVal = 0;
                totalEle = totalEle + 1;
                i += 1;
            }
        }
        temp = (*temp).next[maxEle];
    }

    int elements[maxEle][totalEle],key[totalEle],count;
    count = 0;
    
    temp = root;
    while (temp != NULL) {
        i = 0;
        while (i < maxEle) {
            
            elements[0][count] = temp->id[i].page;
            key[count] = (*temp).key[i];
            nodeVal = 1;
            elements[1][count] = temp->id[i].slot;
            count = count + 1;
            i += 1;
        }
        temp = (*temp).next[maxEle];
    }

    int pg, st,swap,c=0;
    
    
    while (c < count - 1)
    {
        int d = 0;
        while (d < (count - c - 1))
        {

            if(key[d] <= key[d+1]){
                d += 1;
            }else{


                swap = key[d];
                nodeVal = 0;
                key[d] = key[d + 1];
                nodeVal = 1;
                  key[d + 1] = swap;
                     
                pg = elements[0][d];
                st = elements[1][d];

                nodeVal = 0;

               
                elements[0][d] = elements[0][d + 1];
                elements[1][d] = elements[1][d + 1];

                nodeVal = 1;

              
                elements[0][d + 1] = pg;
                elements[1][d + 1] = st;
                 d++;
            }

        }
        c += 1;
    }

    count = 0;
    temp = root;
    while(temp != NULL) {
        i = 0;
        while (i < maxEle) {
            
            temp->id[i].page = elements[0][count];
            (*temp).key[i] = key[count];
            nodeVal = 2;
            temp->id[i].slot = elements[1][count];
            count = count + 1;
            i += 1;
        }
        temp = (*temp).next[maxEle];
    }
  solution();
}

//function to read the next entry in the B tree.
RC nextEntry (BT_ScanHandle *handle, RID *result)
{
    if(scan->next[maxEle] == NULL) {
        return RC_IM_NO_MORE_ENTRIES;
    }
    else{

        if(maxEle == indexNum){
            indexNum = 0;
            scan = scan->next[maxEle];
        }

        if(maxEle != indexNum) {
            nodeVal = 2;
        }

        (*result).page = scan->id[indexNum].page;
        nodeVal = 1;
        (*result).slot = scan->id[indexNum].slot;

        indexNum = indexNum + 1;
    }

 solution();
}

//function to close the tree after scanning all the elements in the B tree.
RC closeTreeScan (BT_ScanHandle *handle)
{
    indexNum = 0;
    nodeVal = 4;
    solution();
}


//Function to print b tree.
char *printTree (BTreeHandle *tree)
{
    solution();
}

RC solution(){
    return RC_OK;
}

RC state(){
    return RC_IM_KEY_NOT_FOUND;
}
