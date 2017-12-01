/*
+ locey
+ allowing a map to exist for indefinite time with controlled timeout for the task to mimic MPI processes
+ consider different runtime types e.g. magnet and irons. 
+ user code depicts reduction between magnet and iron (to form a magnet) but not between irons or between magnets. 
+ if magnet unique, no shuffle needed; one thread reduces all (order not important between irons, but need local state e.g. for counting)
+ consider a script for the order of reduction: nodes x outgoing edges -> nodes; nodes x incoming edges -> nodes
+ consider special dataset containing devices as the date items. used for process distribution
+ if a data item's key is unique, it does not move (therefore the prcess attached to it stays there); 
+ or alternertively explicitly says data item should not move (during reduce)
*/

class PeksComm;

extern int mpi_pid;
extern int freshid();
extern PeksComm comm;

#define EXIT2(info)  do {char buf[256];   const char* msg = strerror_r(errno, buf, sizeof(buf)); const char* mss = (info); mss = mss ? mss : ""; \
        printf("[FAIL] %s:%d(errno=%d): %s (%s)", __FILE__, __LINE__, errno, msg, mss); \
        MPI_Finalize(); exit(-1); } while (0)
#define EXIT() EXIT2(NULL)                              // in future EXIT should destroy the current task and restart it
#define MAX_PATH 256
#define BLOCK_SIZE (64*1024*1024)

typedef void* (thread_func)(void *);
//-----------------------------------------------------------------------------------------------------------------
class PeksItem {        // items collected in event broadcast
public:
    int tid;            // to which thread this item belongs
    int d1, d2, d3;     // to be assigned for computation, communication, and IO capabilities
public:
    void init(int tid, int d1, int d2, int d3) {this->tid=tid; this->d1=d1; this->d2=d2; this->d3=d3; }
};

// msg content for a new task format: header, name, lpids
class PeksContentTask {
public:
    int namelen;            // len of dataset's name as chars
    int taskid;             // the taskid for the new task ought to be consistent with receivers' counting
    int nprocs;             // assuming one thread per proc
    int nblocks;
    int nblksets;
    int tgt_taskid;         // target taskid of inter-task communication 
    int codeid;
    int size;
public:
    static PeksContentTask* create(int taskid, int nprocs, int nblocks, int nblksets, int tgt_taskid, int codeid, char* name) {
        int s = sizeof(PeksContentTask) + (strlen(name)+3)/4*4 + nprocs*sizeof(int);  PeksContentTask* r = (PeksContentTask*)malloc(s);
        r->namelen=strlen(name); r->taskid=taskid; r->nprocs=nprocs; r->nblocks=nblocks; r->nblksets=nblksets; r->codeid=codeid; r->size=s;
        r->tgt_taskid=tgt_taskid; strcpy(r->get_name(), name);  int* l=r->get_lpids(); for (int i=0; i<nprocs; i++) l[i]=i;
	    
	return r;
    }
    char* get_name() {return ((char*)this)+sizeof(PeksContentTask);}
    int get_len() {return (namelen+3)/4*4;}
    int* get_lpids() {return (int*)(get_name() + get_len());}                   // aligned to 4 bytes for the following lpids ints
    void print() {if(0)printf("content: pid=%d, taskid=%d, nprocs=%d, tgt=%d, code=%d, name=%s\n", 
        mpi_pid, taskid, nprocs, tgt_taskid, codeid, get_name());}
};

#define EVENT_TYPE_WAITCOMM 0
#define EVENT_TYPE_RESHAPE  1
#define EVENT_TYPE_SURVEY   2
#define EVENT_TYPE_NEWTASK  3

class PeksMsg {
public:   
    int taskid;             // the target task of the message, !!! should be double-checked in case the supertask is started from diff files
};
class PeksMsgBlock: public PeksMsg {  // a data block in the form of a message
public:
    int spid, staskid, stid, sbsid, sbid;    int tpid, ttid, tbsid, tbid;
public:
    void set_src(int pid, int taskid, int tid, int bsid, int bid) {spid = pid; staskid = taskid; stid = tid; sbsid = bsid; sbid = bid; }
    void set_tgt(int pid, int taskid, int tid, int bsid, int bid) {tpid = pid; this->taskid = taskid; ttid = tid; tbsid = bsid; tbid = bid; }
    char* get_data() {return (char*)this+sizeof(PeksMsgBlock);}
    inline int get_data_size() {return BLOCK_SIZE-sizeof(PeksMsgBlock);}
};
// messgae format: 8int=header, payload[len=braodcast content, nitems*4int=replies]
class PeksMsgEvent: public PeksMsg {  // an event to be broadcast among processes of a task (8 ints before payload)
public:
    int root;       // the root logical pid of broadcst tree
    int slpid;      // source logical pid
    int tpid;       // target pid of this event for relaying
    int msgid;      // which msg from the root
    int type;       // type=EVENT_TYPE_RESHAPE
    int len;        // length of the content (aligned to 4 bytes to ensure alignment of the items) 
    int nitems;     // num of items currently in the payload, each item with fixed size
public:
    PeksMsgEvent() {root=0; nitems=0; len=0;}
    static PeksMsgEvent* create(int taskid, int root, int slpid, int tpid, int msgid, int type, int len, void* content) {
        PeksMsgEvent* r = (PeksMsgEvent*)malloc(sizeof(PeksMsgEvent)+len);          // with content but without items
        r->taskid=taskid; r->root=root; r->slpid=slpid; r->tpid=tpid; r->msgid=msgid; r->type=type; r->len=len; r->nitems=0;
        if (len>0) memcpy(r->payload(), content, len); return (PeksMsgEvent*)r;
    }
    char* payload() { return ((char*)this)+sizeof(PeksMsgEvent);}
    int size() {return sizeof(PeksMsgEvent) + len + nitems*sizeof(PeksItem);}       // accumulatied n replied items
    PeksItem* get_items() {return (PeksItem*)(payload()+len);} 
    void add_item(int tid, int d1, int d2, int d3) {get_items()[nitems++].init(tid,d1,d2,d3);}
    PeksMsgEvent* dup() {int l=size(); void* d=malloc(l); memcpy(d,this,l); return (PeksMsgEvent*)d; }
    void items_linedup(PeksItem* items) {PeksItem* pitems=get_items(); for (int i=0; i<nitems; i++) items[pitems[i].tid]=pitems[i];}
    void occur();
    void relay_events();
    void print() {
        printf("pid=%d, msg=%d, len=%d [%d %d %d], nitems=%d, ", mpi_pid, msgid, len, payload()[0],payload()[1], payload()[2], nitems);
        for (int i=0; i<nitems; i++) printf("(%d:lpid=%d,mpi=%d,%d,%d), ", i,
            get_items()[i].tid, get_items()[i].d1, get_items()[i].d2, get_items()[i].d3);
        printf("\n");
    }
};

//===============================================================================
class PeksTask;
class PeksThread;
class PeksBlockset;
class PeksObject {
public:
    sem_t monitor;          // if not yet init, wait and post this sem
public:
    PeksObject() {sem_init(&monitor, 0, 0);}    
    void done_init() {sem_post(&monitor); }        // after init, this is called.
};
//---------------------------------
class PeksBlock: PeksObject {   
public:
    PeksBlockset* homeset;	
    int lbid; PeksMsgBlock* data;    
    sem_t exist;            // initially 0, whether data ready for read, may be automatically removed 
public:
    void init() { sem_init(&exist, 0, 0); done_init();}
    PeksBlock() : data(NULL) {init();}
    PeksBlock(PeksBlockset* hset, int lbid): homeset(hset), lbid(lbid), data(NULL) {init();}
	PeksBlock(int e, int k) : data(NULL) {init();}
    ~PeksBlock() {delete data; sem_destroy(&exist); sem_destroy(&monitor);}
    void post() {sem_post(&exist);}  // when available post the semaphore
    void alloc() {if (data==NULL) data=(PeksMsgBlock*)malloc(BLOCK_SIZE); memset(data->get_data(), rand()%26+'A', data->get_data_size());}
	void store();
    void load();
    void store(int ltid, int bsid, int bid);
    void load(int ltid, int bsid, int bid);
    void overwrite(PeksMsgBlock* d) {alloc(); memcpy(data, d, BLOCK_SIZE); post();}  // this data overwritten by d
    void send();
    void sweep() {}
};
//---------------------------------
class PeksBlockset: PeksObject {
public:
    PeksThread* homethread; int nblocks; PeksBlock** blocks;  int bsid; // nblocks: num of local blocks in the blockset
public:
	PeksBlockset():nblocks(0),blocks(NULL) {}
	void init(PeksThread* hthread,int bsid,int nlb) {
        homethread=hthread; blocks=(PeksBlock**)calloc(nlb,sizeof(void*)); this->bsid=bsid; nblocks=nlb; done_init();}
    PeksBlock* get(int i) {sem_wait(&monitor); if (blocks[i]==NULL) blocks[i]=new PeksBlock(this,i); 
        sem_post(&monitor); return blocks[i];}
    ~PeksBlockset() {for (int i=0; i<nblocks; i++) delete blocks[i]; delete blocks; sem_destroy(&monitor);}
    void sweep() {for (int i=0; i<nblocks; i++) if (blocks[i]!=NULL) blocks[i]->sweep();}
};
//---------------------------------
class PeksThread: PeksObject {
public:
    PeksTask* hometask; int nblksets; PeksBlockset** blocksets;  // num of blocksets on the thread
	int tid; pthread_t thread;    
public:
	PeksThread():nblksets(0),blocksets(NULL),thread(NULL) {}
    ~PeksThread() {for (int i=0; i<nblksets; i++) delete blocksets[i]; delete blocksets; sem_destroy(&monitor);}
	void init(PeksTask* htask, int tid, int nbs) {hometask=htask; blocksets=(PeksBlockset**)calloc(nbs,sizeof(void*));
		this->tid=tid; nblksets=nbs; done_init();}
    PeksBlockset* get(int i);
    void sweep() {for (int i=0; i<nblksets; i++) if (blocksets[i]!=NULL) blocksets[i]->sweep();}
};
//---------------------------------
#define NRELAYS 3
#define THREAD_EXIT_WAIT_SEC 1

class PeksTask: PeksObject {    // also class of a dataset    
public:                         
    int     taskid;             // id (index) of the task in the division, this id never decreases after software installation
    int     lpid;               // the logical pid of this process in the task, -1 if not a member
    char    *name;              // name of the dataset and the task, also the main part of the names of the files
    int     nthreads;           // num of threads
    PeksThread** threads;       
    int     nlprocs;            // num of logical procs of this task
    int*    lpids;              // logical pid on which each thread is located
    int*    pids;               // the mpi pid of every logical pid
	int     nblksets;           // nblksets: num of all blockset of the task, same for all threads
	int*    tabnblocks;         // nblocks per thread int[nthreads][nblocksets]  super's is NULL

    int     source_taskid;      // to allow bob to find its creator
    int     target_taskid;      // taskid of target of comm overwrite
    sem_t   init_complete;      // posted task begins
    int*    succ;               // from a root lpid,  all successive lpid of this process, succ=root for leaf procs, need %n
    int     current_eventid;    // id of the event being broadcast, the var set after obtaining broadcast lock at root
    int     nevents_replied;    // number of events received from the leaf procs in the broadcast tree at root
    int     max_nevents_replied;// expected number of events received from the leaf procs
    sem_t   echo_complete;      // triggered at initiator when all msgs return from leaves
    PeksItem* items;            // payload collected from broadcast (num=nitems)
public:
	PeksTask(char* name): nthreads(0),threads(NULL), current_eventid(0), nevents_replied(0) {
        this->name=name; sem_init(&echo_complete,0,0); sem_init(&init_complete,0,0); items=NULL; }
    ~PeksTask() {for (int i=0; i<nthreads; i++) if (threads[i]!=NULL) delete threads[i];
        delete threads; delete succ; sem_destroy(&monitor);}
	void init(int i, int nthreads, int nblksets, int* tabnblocks, int lpid, int *lpids, int npt, int* pids);
    void init(PeksTask* super, int taskid, int nthds, int* superlpids, int nblocks, int nblksets);
    void init(PeksTask* super, int taskid, int lpid_min, int lpid_max, int nblocks, int nblksets);
    PeksThread* get(int i) {sem_wait(&monitor); 
        if (threads[i]==NULL) {threads[i]=new PeksThread(); threads[i]->init(this,i,nblksets);}
        sem_post(&monitor); return threads[i];
    }
    void begin(thread_func code);
    void end();
    void sweep_dir();       // remove the entire directory of the dataset
    void sweep_thread();    // called by communicator to create a new thread for sweeping while freeing the caller
    void sweep();           // need to prevent IO before starting file sweeping; need to sweep pending send
    void broadcast_update_event(PeksMsgEvent* e) {if (lpid<0) return;  // all threads on this proc added, called by comm
        for (int i=0; i<nthreads; i++) if (lpids[i]==lpid && get(i)!=NULL) e->add_item(i, lpid, mpi_pid, 3);    }
    void broadcast_echo(PeksMsgEvent* e) {                       // one echo may contain several replies
        e->items_linedup(items); // called by comm on root thread, no re-entry as broadcast is atomic
        if (current_eventid==e->msgid) nevents_replied++;
        if (current_eventid==e->msgid && nevents_replied==max_nevents_replied) sem_post(&echo_complete); 
    }
    void broadcast_event(int type, int len, void* content);             // only lpid==0 performs broadcasting
    // !!!  not used yet
    void create_subtask(int lpid_min, int lpid_max) {                   // performed by the root of a super task
        broadcast_event(EVENT_TYPE_SURVEY, 0, NULL);                    // Survey num of free data blocks (not occupied or booked)
    }
    void print_echo(PeksMsgEvent* e) {
//        for (int i=0; i<nthreads; i++) printf("echo:  eventid=%d, itemtid[%d]=%d, type=%d\n", e->msgid, i, items[i].tid, e->type);
    }
};
//---------------------------------
class PeksDivision: PeksObject {                    // a division has a super task (taskid=0) for creating other tasks
public:                                 // creating super task is the same as fault recovery
	int ntask; int taskid; PeksTask** tasks;
public:
	PeksDivision():ntask(0),taskid(0),tasks(NULL) {}
    ~PeksDivision() {sem_destroy(&monitor);}
	void init(int nt) {tasks=(PeksTask**)calloc(nt,sizeof(void*)); ntask=nt; taskid=0; done_init();}
    PeksTask* get(int i) {return tasks[i]; }
    PeksTask* create_task(int nprocs);
    PeksTask* create_task(char* name, int if_sweep, int lpid_min, int lpid_max, int nblocks, int nblksets, int tgt_taskid, int codeid);
    PeksTask* create_task(PeksContentTask* content); 
};

//===============================================================================
#define TAG_BASE 1000
#define MAX_NMSGS 10
enum MSGTYPE {MSG_BLOCK, MSG_EVENT};
class PeksComm {
public:
    void* r[MAX_NMSGS];                 // pending messages to send
    MPI_Request* reqs[MAX_NMSGS];       // pending requests, NULL if pending msg!=NULL
    enum MSGTYPE type[MAX_NMSGS];       // data block or broadcast event
    sem_t* sent[MAX_NMSGS];             // posted when the send request is completed; NULL if no wait
    int n;                              // num of messages pending
    sem_t npos;                         // how many more requests are allowed
    void *msgbuf;                       // message just received, not sure if block or event
public:
    PeksComm() {n=0; sem_init(&npos, 0, MAX_NMSGS); 
        msgbuf = (void*)malloc(BLOCK_SIZE); for(int i=0; i<MAX_NMSGS; i++) reqs[i]=NULL;}
    void remove0(int k) {n--; r[k]=r[n]; reqs[k]=reqs[n]; type[k]=type[n]; reqs[n]=NULL; sem_post(&npos);
        if(0)if (n!=0) printf("mpi_pid=%d, k=%d, n=%d\n", mpi_pid, k, n);}
    void remove(int k) {delete reqs[k]; if (type[k]==MSG_EVENT) delete (PeksMsgEvent*)r[k]; remove0(k);}
    void remove_reqs_of_task(int t);
    void send0(void* msg, enum MSGTYPE t, sem_t* s) {r[n]=msg; sent[n]=s; type[n++]=t; }
    void send(void* msg, enum MSGTYPE t, sem_t* s);
    void communicator();
    MPI_Request* isend(int msgt, void* msg) {MPI_Request* req = new MPI_Request;
        if (msgt==MSG_BLOCK) {PeksMsgBlock* block=(PeksMsgBlock*)msg;
            MPI_Isend(msg, BLOCK_SIZE, MPI_CHAR, block->tpid, TAG_BASE + MSG_BLOCK,  MPI_COMM_WORLD, req);}
        if (msgt==MSG_EVENT) {PeksMsgEvent* e=(PeksMsgEvent*)msg; //printf("isend: "); e->print();
            MPI_Isend(msg, e->size(), MPI_CHAR, e->tpid, TAG_BASE + MSG_EVENT,  MPI_COMM_WORLD, req);}
        return req;
    }
    void after_irecv(int msgt, void* msg);
};
