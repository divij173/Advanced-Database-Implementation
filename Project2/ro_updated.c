#include <stdio.h>
#include <stdlib.h>
#include "ro.h"
#include "db.h"
#include <assert.h>

// #define MAXID 4
// one buffer
struct buffer {
	int   pin;
    int usage;
    UINT64  id;
};

// // collection of buffers + stats
struct bufPool {
	int   nbufs;         // how many buffers
    int nvb;
    char  strategy;
	struct buffer *bufs;
};

BufPool initBufPool(int nbufs, char strategy)
{
	BufPool newPool;
	struct buffer *bufs;

	newPool = malloc(sizeof(struct bufPool));
	assert(newPool != NULL);
	newPool->nbufs = nbufs;
	newPool->strategy = strategy;
    newPool->nvb = 0;
	newPool->bufs = malloc(nbufs * sizeof(struct buffer));

	int i;
	for (i = 0; i < nbufs; i++) {
		newPool->bufs[i].id = 0;
		newPool->bufs[i].pin = 0;
        newPool->bufs[i].usage = 0;
	}
	return newPool;
}

// static unsigned int clock = 0;


int pageInPool(BufPool pool, UINT64 page)
{
	int i;  
    // char id[50];
	// sprintf(id,"%s%d",table_name,page);
    // char *id2 = (char*)malloc(sizeof(table_name)+sizeof(pageId)+1);
    // char *id3 = (char*)malloc(sizeof(table_name)+12);
    // sprintf(id2,"%s%d",table_name,pageId);
    // printf("FuncSlot = %s\n", id2);
	UINT64 id;
	// sprintf(id,"%d",page);
	for (i = 0; i < pool->nbufs; i++) {
		if (page==pool->bufs[i].id) {
			return i;
		}
	}
	return -1;
}

int request_page(BufPool pool, UINT64 page)
{
	int slot;
	// printf("Request %ld\n", page);
	slot = pageInPool(pool,page);
	if (slot >= 0)
    {
        // pool->nhits++;
        pool->bufs[page].usage+=1;
        pool->bufs[page].pin=1;
        return page;
    }
    while (slot>=-9999) {
        if (pool->bufs[pool->nvb].pin==0 && pool->bufs[pool->nvb].usage == 0)
        {
            // read page from disk to buffer[nvb]
            pool->bufs[pool->nvb].pin=1;
            pool->bufs[pool->nvb].usage=1;
            pool->nvb = (pool->nvb+1) % pool->nbufs;
            log_read_page(page);
            return page;
        }
        else
        {
            if (pool->bufs[pool->nvb].usage > 0)
            {
                pool->bufs[pool->nvb].usage-=1;
            }
            pool->nvb = (pool->nvb+1) % pool->nbufs;
        }
    }
	return slot;
}

void release_page(BufPool pool, UINT64 page)
{
    pool->bufs[pool->nvb].pin =0;
}
// Conf* global_conf;
// Database* global_db;
// Conf* cf;
// Database* db;
void init(){
    // do some initialization here.

    // example to get the Conf pointer
    Conf* cf = get_conf();

    // example to get the Database pointer
    Database* db = get_db();

    // // Get the Conf pointer
    // global_conf = get_conf();

    // // Get the Database pointer
    // global_db = get_db();

    // printf("%s",db->tables->name);
    printf("init() is invoked.\n");
}

void release(){
    // optional
    // do some end tasks here.
    // free space to avoid memory leak

    // free(cf);
    // free(db);
    printf("release() is invoked.\n");
}

_Table* sel(const UINT idx, const INT cond_val, const char* table_name){
    // init();
    printf("sel() is invoked.\n");

    // invoke log_read_page() every time a page is read from the hard drive.
    // invoke log_release_page() every time a page is released from the memory.

    // invoke log_open_file() every time a page is read from the hard drive.
    // invoke log_close_file() every time a page is released from the memory.

    // testing
    // the following code constructs a synthetic _Table with 10 tuples and each tuple contains 4 attributes
    // examine log.txt to see the example outputs
    // replace all code with your implementation
    Database* db = get_db();
    Conf* cf = get_conf();
    // printf("%s",db->path);
    char table_path[200];

    // Table t;
    // UINT arr_size = sizeof(db->tables)/sizeof(db->tables[0]);
    // printf("%u    , %s   ,%s ",db->tables[1].oid, table_name, t.name);
    int length=0,height=0;
    Table *tab=db->tables;
    // printf("%i",sizeof(tab));
    UINT oid_sel=0;
    for (UINT i = 0; i<sizeof(tab);i++)
    {
        // printf("%s,%s|",db->tables[i].name, table_name );
        // printf("%s,%s||", a,b);
        if (strcmp(db->tables[i].name,table_name) == 0)
        {
            // printf("|%s, %s|", db->tables[i].name, table_name);
            length=db->tables[i].nattrs;
            height=db->tables[i].ntuples;
            sprintf(table_path,"%s/%u",db->path,db->tables[i].oid);
            oid_sel= db->tables[i].oid;
            break;    
        }
    }
    // sprintf(table_path,"%s/%u",db->path,t.oid);
    // printf("|%s|", table_path);

    // int table[height][length];
    FILE* file = NULL;
    file = fopen(table_path,"r");
    log_open_file(oid_sel);
    if (file == NULL)
    {
        printf("Failed to open the file.\n");
        return 1;
    }

    BufPool pool = initBufPool(cf->buf_slots,cf->buf_policy);
    
    UINT64 pageId;
    INT attribute;

    // Read all the pages from the file
    int arr1[height][length];
    _Table* result = malloc(sizeof(_Table)+height*length*4);
    int ind=0;
    int row1=0;
    int inc=0;
    int pageSize=cf->page_size;
    int ntuples_inpage=    (pageSize-8)/(length*4);
    int ntuples_inlastpage=height - height/ntuples_inpage*ntuples_inpage;// 7/3=2,2*3=6. 7-6=1
    int numb_of_full_page=height/ntuples_inpage;
    for (UINT i=0; i<numb_of_full_page;i++)
    {
        fseek(file,i*pageSize ,SEEK_SET);
        fread(&pageId, sizeof(UINT64), 1, file); // Read pageId as INT64
        // printf("\nPage ID: %ld\n", pageId);
        int slot = request_page(pool,pageId);
        if (slot>-4)
        {
            for (int j = 0; j < ntuples_inpage; j++) { // tuple
                int count=0;
                for (int k = 0; k < length; k++) { //attr
                    fread(&attribute, sizeof(INT), 1, file); // Read attribute as INT32
                    arr1[ind][k] = attribute;
                    if (k==idx && arr1[ind][k] == cond_val)
                    {
                        count++;
                    }
                    // printf("|%d|", attribute);
                }
                if (count>0) {
                    Tuple t = malloc(sizeof(INT)*length);
                    result->tuples[inc] = t;
                    inc++;
                    for (UINT m = 0; m < length; m++){
                        t[m] = arr1[ind][m];
                    row1++;
                    }
                }
                ind++;
                // printf("\n");
            }
        }
        release_page(pool,pageId);
        log_release_page(pageId);
        
        
        // fseek(file, (length - 1) * sizeof(INT8), SEEK_CUR);
    }
    log_read_page(pageId);
    if(ntuples_inlastpage>0){
        fseek(file,numb_of_full_page*pageSize,SEEK_SET);

        fread(&pageId, sizeof(UINT64), 1, file); // Read pageId as INT64
        // printf("\nLast Page ID: %ld\n", pageId);

        int slot = request_page(pool,pageId);
        if (slot>-4)
        {
            for (int j = 0; j < ntuples_inlastpage; j++) { // tuple
                int count=0;
                for (int k = 0; k < length; k++) { //attr
                    fread(&attribute, sizeof(INT), 1, file); // Read attribute as INT32
                    arr1[ind][k] = attribute;
                    if (k==idx && arr1[ind][k] == cond_val)
                    {
                        count++;
                    }
                    // printf("|%d|", attribute);
                }
                if (count>0) {
                    Tuple t = malloc(sizeof(INT)*length);
                    result->tuples[inc] = t;
                    inc++;
                    for (UINT m = 0; m < length; m++){
                        t[m] = arr1[ind][m];
                    row1++;
                    }
                }
                ind++;
                // printf("\n");
            }
        }
        release_page(pool,pageId);
        log_release_page(pageId);
    }

    // log_read_page(UINT64 pid);

//    for (int i=0; i<ind;i++)
//    {
//     for (int j=0;j<length;j++)
//     {
//         printf("%d ",arr1[i][j]);
//     }
//     printf("\n");
//    }
    log_release_page(pageId);
    fclose(file);
    log_close_file(oid_sel);
    free(pool->bufs);
    free(pool);
    result->nattrs = length;
    result->ntuples = inc;
    
    return result;
}

_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name){

    printf("join() is invoked.\n");
    // write your code to join two tables
    // invoke log_read_page() every time a page is read from the hard drive.
    // invoke log_release_page() every time a page is released from the memory.

    // invoke log_open_file() every time a page is read from the hard drive.
    // invoke log_close_file() every time a page is released from the memory.
    Database* db = get_db();
    Conf* cf = get_conf();

    char table_path1[200];
    char table_path2[200];

    int length1=0,height1=0,length2=0,height2=0;
    Table *tab=db->tables;
    // printf("%i",sizeof(tab));
    int file_count=0;
    UINT oid_join1=0;
    UINT oid_join2=0;
    for (UINT i = 0; i<sizeof(tab);i++)
    {
        // printf("%s,%s|",db->tables[i].name, table_name );
        // printf("%s,%s||", a,b);
        if (strcmp(db->tables[i].name,table1_name) == 0)
        {
            // printf("|%s, %s|", db->tables[i].name, table_name);
            length1=db->tables[i].nattrs;
            height1=db->tables[i].ntuples;
            sprintf(table_path1,"%s/%u",db->path,db->tables[i].oid);    
            oid_join1=db->tables[i].oid;
            // break;    
            file_count++;
        }

        if (strcmp(db->tables[i].name,table2_name) == 0)
        {
            // printf("|%s, %s|", db->tables[i].name, table_name);
            length2=db->tables[i].nattrs;
            height2=db->tables[i].ntuples;
            sprintf(table_path2,"%s/%u",db->path,db->tables[i].oid); 
            oid_join2=db->tables[i].oid;   
            // break;   
            file_count++; 
        }

        if (file_count > 1)
        {
            break;
        }
    }
    // printf("%s||%s|", table_path1,table_path2);

    FILE* file1 = NULL;
    
    file1 = fopen(table_path1,"r");
    log_open_file(oid_join1);

    if (file1 == NULL)
    {
        printf("Failed to open the file.\n");
        return 1;
    }

    BufPool pool = initBufPool(cf->buf_slots,cf->buf_policy);

    UINT64 pageId1;
    INT attribute1;

    // table1
    int arr1[height1][length1];
    // _Table* result1 = malloc(sizeof(_Table)+height1*length1*4);
    int ind1=0;
    int pageSize1=cf->page_size;
    int ntuples_inpage1=    (pageSize1-8)/(length1*4);
    int ntuples_inlastpage1=height1 - height1/ntuples_inpage1*ntuples_inpage1;// 7/3=2,2*3=6. 7-6=1
    int numb_of_full_page1=height1/ntuples_inpage1;
    for (UINT i=0; i<numb_of_full_page1;i++)
    {
        fseek(file1,i*pageSize1 ,SEEK_SET);
        fread(&pageId1, sizeof(UINT64), 1, file1); // Read pageId as INT64
        // printf("\nPage1 ID: %ld\n", pageId1);

        int slot = request_page(pool,pageId1);
        if (slot>-4)
        {
            for (int j = 0; j < ntuples_inpage1; j++) { // tuple
                for (int k = 0; k < length1; k++) { //attr
                    fread(&attribute1, sizeof(INT), 1, file1); // Read attribute as INT32
                    arr1[ind1][k] = attribute1;
                }
                ind1++;
                // printf("\n");
            }
        }
        release_page(pool,pageId1);
        log_release_page(pageId1);
        // fseek(file, (length - 1) * sizeof(INT8), SEEK_CUR);
    }
    if(ntuples_inlastpage1>0){
        fseek(file1,numb_of_full_page1*pageSize1,SEEK_SET);

        fread(&pageId1, sizeof(UINT64), 1, file1); // Read pageId as INT64
        // printf("\nLast Page1 ID: %ld\n", pageId1);

        int slot = request_page(pool,pageId1);
        if (slot>-4)
        {
            for (int j = 0; j < ntuples_inlastpage1; j++) { // tuple
                for (int k = 0; k < length1; k++) { //attr
                    fread(&attribute1, sizeof(INT), 1, file1); // Read attribute as INT32
                    arr1[ind1][k] = attribute1;
                }
                ind1++;
                // printf("\n");
            }
        }
        release_page(pool,pageId1);
        log_release_page(pageId1);
    }

    // log_read_page(UINT64 pid);

    // for (int i=0; i<height1;i++)
    // {
    //  for (int j=0;j<length1;j++)
    //  {
    //     printf("%d ",arr1[i][j]);
    //  }
    //  printf("\n");
    // }
    fclose(file1);
    log_close_file(oid_join1);


    /////////////////////////////////////////////////////////////////////

    FILE* file2 = NULL;
    file2 = fopen(table_path2,"r");
    log_open_file(oid_join2);
    if (file2 == NULL)
    {
        printf("Failed to open the file.\n");
        return 1;
    }

    UINT64 pageId2;
    INT attribute2;

    // table2
    int arr2[height2][length2];
    // _Table* result2 = malloc(sizeof(_Table)+height2*length2*4);
    int ind2=0;
    int pageSize2=cf->page_size;
    int ntuples_inpage2=    (pageSize2-8)/(length2*4);
    int ntuples_inlastpage2=height2 - height2/ntuples_inpage2*ntuples_inpage2;// 7/3=2,2*3=6. 7-6=1
    int numb_of_full_page2=height2/ntuples_inpage2;
    log_read_page(pageId2);
    for (UINT i=0; i<numb_of_full_page2;i++)
    {
        fseek(file2,i*pageSize2 ,SEEK_SET);
        fread(&pageId2, sizeof(UINT64), 1, file2); // Read pageId as INT64
        // printf("\nPage1 ID: %ld\n", pageId1);

        int slot = request_page(pool,pageId2);
        if (slot>-4)
        {
            for (int j = 0; j < ntuples_inpage2; j++) { // tuple
                for (int k = 0; k < length2; k++) { //attr
                    fread(&attribute2, sizeof(INT), 1, file2); // Read attribute as INT32
                    arr2[ind2][k] = attribute2;
                    // printf("|%d|", attribute2);
                }
                ind2++;
                // printf("\n");
            }
        }
        release_page(pool,pageId2);
        log_release_page(pageId2);
        // fseek(file, (length - 1) * sizeof(INT8), SEEK_CUR);
    }
    if(ntuples_inlastpage2>0){
        fseek(file2,numb_of_full_page2*pageSize2,SEEK_SET);

        fread(&pageId2, sizeof(UINT64), 1, file2); // Read pageId as INT64
        // printf("\nLast Page1 ID: %ld\n", pageId1);

        int slot = request_page(pool,pageId2);
        if (slot>-4)
        {
            for (int j = 0; j < ntuples_inlastpage2; j++) { // tuple
                for (int k = 0; k < length2; k++) { //attr
                    fread(&attribute2, sizeof(INT), 1, file2); // Read attribute as INT32
                    arr2[ind2][k] = attribute2;
                    // printf("|%d|", attribute2);
                }
                ind2++;
                // printf("\n");
            }
        }
        release_page(pool,pageId2);
        log_release_page(pageId2);
    }

    // log_read_page(UINT64 pid);
    // printf("\n||TABLE2||\n");
    // for (int i=0; i<height2;i++)
    // {
    //  for (int j=0;j<length2;j++)
    //  {
    //     printf("%d ",arr2[i][j]);
    //  }
    //  printf("\n");
    // }
    log_release_page(pageId2);
    fclose(file2);
    log_close_file(oid_join2);
    free(pool->bufs);
    free(pool);

    int county1=-1;
    int arr3[height1+height2][length1+length2];
    for (UINT i1=0;i1<height1;i1++) {
        for (UINT i2=0;i2<height2;i2++) {
            if(arr1[i1][idx1]==arr2[i2][idx2]){
                // printf("||||%d|||", arr1[i1][idx1], arr2[i2][idx2]);
                int inc=0;
                county1++;
                for (UINT k=0;k<length1;k++) {
                    arr3[county1][k]=arr1[i1][k];
                    // printf("|||[%d][%d] = %d|||",county1,k,arr3[county1][k]);
                    inc=k;
                }    
                inc+=1;
                // printf("||%d||", county1);
                for (UINT k=0;k<length2;k++) {
                    // inc++;
                    arr3[county1][inc+k]=arr2[i2][k];
                    // printf("|||[%d][%d] = %d|||",county1,inc+k,arr3[county1][inc+k]);
                    // printf("|||%d|||",arr2[i2][k]);
                }
            }
        }
    }

    // for (UINT i=0; i<county1+1;i++)
    // {
    //  for (UINT j=0;j<length1+length2;j++)
    //  {
    //     printf("%d ",arr3[i][j]);
    //  }
    //  printf("\n");
    // }

    _Table* result = malloc(sizeof(_Table)+(county1+1)*(length1+length2)*4);
    // printf("%d", cf->buf_slots);
    result->nattrs = length1+length2;
    result->ntuples = county1+1;
    for (UINT i=0;i<county1+1;i++) {
        Tuple t = malloc(sizeof(INT)*(length1+length2));
        result->tuples[i] = t;
        // inc++;
        for (UINT m = 0; m < length1+length2; m++){
            t[m] = arr3[i][m];
        // row1++;
        }
    }
    


    return result;
}