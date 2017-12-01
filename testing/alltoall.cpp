#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/sysinfo.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <fcntl.h>
#include <pthread.h>
#include <dirent.h>
#include <mpi.h>
#include <semaphore.h>
#define _PA_LOCALMT  1023 
#define _PA_THREADMT  130048 
#define _PA_ANYMT   131071 
//#include "pa.h"
#define _pa_dims__PA0_main_ta(_pam,_pat) (1)
#define _pa_dstp__PA0_main_ta(_pam,_pat) (_pa_dims__PA0_main_ta(,_pat)*1)
#define _pa_dstp__PA0_main_ta_0(_pam,_pat) (_pa_dims__PA0_main_ta_0(,_pat)*_pa_dstp__PA0_main_ta(_pam,_pat))
#define _pa_dim__PA0_main_ta_0(_pam,_pat) ((2048&_pat)==0?1:1)
#define _pa_gofs__PA0_main_ta_0(_pai,_pam,_pat) ((2048&_pat)==0? 0 : (_pam?(_pai)%_pa_dim__PA0_main_ta_0(,_pat):(_pai)))
#define _pa_fofs__PA0_main_ta_0(_pai,_pam,_pat) 0
#define _pa_stp__PA0_main_ta_0(_pam,_pat) _pa_dstp__PA0_main_ta_0(_pam,_pat)
#define _pa_dim__PA0_main_ta(_pam,_pat) (_pa_dim__PA0_main_ta_0(,_pat))
#define _pa_dims__PA0_main_ta_0(_pam,_pat) (1)
#define _pa_gofs__PA0_main_ta(_pai,_pam,_pat) ((2048&_pat)==0? 0 : (_pam?(_pai)%_pa_dim__PA0_main_ta(,_pat):(_pai)))
#define _pa_fofs__PA0_main_ta(_pai,_pam,_pat) 0
#define _pa_stp__PA0_main_ta(_pam,_pat) _pa_dstp__PA0_main_ta(_pam,_pat)
#define _pa_rofs__PA0_main_ta(_pai,_pam,_pat) _pa_ofs(__PA0_main_ta,(_pai,_pam,_pat))
#define _pa_sofs__PA0_main_ta(_pai,_pam,_pat) _pa_ofs(__PA0_main_ta,(_pai,_pam,_pat))
#define _pa_rofs__PA0_main_ta_0(_pai,_pam,_pat) ((_pa_gofs__PA0_main_ta_0(_pai,_pam,_pat)*_pa_dims__PA0_main_ta_0(,_pat))+_pa_fofs__PA0_main_ta_0(_pai,_pam,_pat))
#define _pa_sofs__PA0_main_ta_0(_pai,_pam,_pat) (((_pai)*_pa_dims__PA0_main_ta_0(,_pat))+0)
#define _pa_idx__PA0_main_ta(_pai,_pat ) (_pai)
#define _pa_dims__PA0_swprocs_ta(_pam,_pat) (1)
#define _pa_dstp__PA0_swprocs_ta(_pam,_pat) (_pa_dims__PA0_swprocs_ta(,_pat)*1)
#define _pa_dstp__PA0_swprocs_ta_0(_pam,_pat) (_pa_dims__PA0_swprocs_ta_0(,_pat)*_pa_dstp__PA0_swprocs_ta(_pam,_pat))
#define _pa_dim__PA0_swprocs_ta_0(_pam,_pat) ((8192&_pat)==0?1:CCC_PES)
#define _pa_gofs__PA0_swprocs_ta_0(_pai,_pam,_pat) ((8192&_pat)==0? 0 : (_pam?(_pai)%_pa_dim__PA0_swprocs_ta_0(,_pat):(_pai)))
#define _pa_fofs__PA0_swprocs_ta_0(_pai,_pam,_pat) 0
#define _pa_stp__PA0_swprocs_ta_0(_pam,_pat) _pa_dstp__PA0_swprocs_ta_0(_pam,_pat)
#define _pa_dim__PA0_swprocs_ta(_pam,_pat) (_pa_dim__PA0_swprocs_ta_0(,_pat))
#define _pa_dims__PA0_swprocs_ta_0(_pam,_pat) (1)
#define _pa_gofs__PA0_swprocs_ta(_pai,_pam,_pat) ((8192&_pat)==0? 0 : (_pam?(_pai)%_pa_dim__PA0_swprocs_ta(,_pat):(_pai)))
#define _pa_fofs__PA0_swprocs_ta(_pai,_pam,_pat) 0
#define _pa_stp__PA0_swprocs_ta(_pam,_pat) _pa_dstp__PA0_swprocs_ta(_pam,_pat)
#define _pa_rofs__PA0_swprocs_ta(_pai,_pam,_pat) _pa_ofs(__PA0_swprocs_ta,(_pai,_pam,_pat))
#define _pa_sofs__PA0_swprocs_ta(_pai,_pam,_pat) _pa_ofs(__PA0_swprocs_ta,(_pai,_pam,_pat))
#define _pa_rofs__PA0_swprocs_ta_0(_pai,_pam,_pat) ((_pa_gofs__PA0_swprocs_ta_0(_pai,_pam,_pat)*_pa_dims__PA0_swprocs_ta_0(,_pat))+_pa_fofs__PA0_swprocs_ta_0(_pai,_pam,_pat))
#define _pa_sofs__PA0_swprocs_ta_0(_pai,_pam,_pat) (((_pai)*_pa_dims__PA0_swprocs_ta_0(,_pat))+0)
#define _pa_idx__PA0_swprocs_ta(_pai,_pat ) (_pai)
#ifdef _USE_MIC
__attribute__((target(mic))) int _pa_dummy_mic;
#endif
#define _PA_SWPROC_TAG 10000
#include "peks.h"
int mpi_pid; int nprocs; int process_exit=0;         
PeksDivision division;  PeksComm comm;
PeksTask *alice=NULL, *bob=NULL, *alice2=NULL;
pthread_t attached_thread(thread_func f, void* in) {pthread_t t; pthread_create(&t,NULL,f,in); return t;}    
//=============================================

void wait_thread(pthread_t t) { if (pthread_join(t, NULL)) EXIT();}
//=============================================

void detached_thread(thread_func f, void* in) {pthread_t t; if(pthread_create(&t,NULL,f,in)==0)pthread_detach(t);}
//=============================================

void* relay_detached_thread(void* in) {PeksMsgEvent* e=(PeksMsgEvent*)in; e->relay_events(); return NULL;}
//=============================================

double get_timer(){struct timeval tp; gettimeofday(&tp,0); return ((double)(tp.tv_sec)+(double) (tp.tv_usec) * 1.e-6);}
//=============================================

int freshid() {static int i=5; return __sync_add_and_fetch(&i, 1); }
//=============================================

void sem_waitpost(sem_t* s) {sem_wait(s); sem_post(s);}
//=============================================

void sem_wait(sem_t* s, int n) {for (int i=0; i<n; i++) sem_wait(s);}
//=============================================

void sem_post(sem_t* s, int n) {for (int i=0; i<n; i++) sem_post(s);}
//=============================================

int sem_waitall(sem_t* s) {int m=0; while (sem_trywait(s)==0) m++; return m;}  
//=============================================

const char* root_path = "./datasets";
FILE* block_path(const char *ds_name, int if_r, int ltid, int bsid, int bid) {
    char path[MAX_PATH];
    sprintf(path,"%s/%s/%d/%d/%d.pex", root_path,ds_name,ltid,bsid,bid); 
    if (if_r) {FILE * f = fopen(path, "rb"); if(f==NULL) EXIT2(path); return f;}
    FILE * f = fopen(path, "wb");
    if (f==NULL) {int mode = 0777;
        sprintf(path,"%s", root_path); mkdir(path, mode);
        sprintf(path,"%s/%s", root_path,ds_name); mkdir(path, mode);
        sprintf(path,"%s/%s/%d", root_path,ds_name, ltid); mkdir(path, mode);
        sprintf(path,"%s/%s/%d/%d",root_path,ds_name,ltid,bsid);mkdir(path,mode);
        sprintf(path,"%s/%s/%d/%d/%d.pex", root_path,ds_name, ltid, bsid, bid);
        f = fopen(path, "wb"); if (f==NULL) EXIT2(path); }
    return f;
    }
//=============================================

int bdcast_succ0(int pid) {int n0 = 0, n1 = NRELAYS;  
    while (pid > n1) {int nt = n1; n1 = (n1 - n0)*NRELAYS + n1; n0 = nt;}
    return (pid-n0-1)*NRELAYS + n1 + 1;    
    }
//=============================================

int bdcast_succ(int pid, int root, int k, int n) {  
    int tpid = (pid-root+n)%n;  int succ = bdcast_succ0(tpid)+k;
    if (succ>=n) return root; return (succ+root)%n;
    }
//=============================================

int bdcast_succ_nroot(int n) {int pid; for (pid=n/3; bdcast_succ0(pid)<=n; pid++); return n - pid + 1;}
//=============================================

void* communicator(void* dummy) {comm.communicator(); return NULL;}
//=============================================

void* sweep_task(void* task) { ((PeksTask*)task)->sweep(); return NULL;}
//=============================================

sem_t comm_csec;    
void PeksComm::send(void* msg, enum MSGTYPE t, sem_t* s) {
    sem_wait(&npos); sem_wait(&comm_csec); send0(msg,t,s); sem_post(&comm_csec); 
    }
//=============================================

void PeksComm::remove_reqs_of_task(int t) {
    sem_wait(&comm_csec); for (int i=n-1; i>=0; i--) if(((PeksMsg*)r[i])->taskid==t) remove0(i); sem_post(&comm_csec);
    }
//=============================================

void PeksMsgEvent::occur(){comm.send(this, MSG_EVENT, NULL);}
//=============================================

sem_t relay_events_csec;        
void PeksMsgEvent::relay_events() {
    sem_wait(&relay_events_csec); PeksTask* t = division.get(taskid); 
    for (int k=0; k<NRELAYS; k++) {int tlpid = t->succ[root*NRELAYS+k]; tpid = t->pids[tlpid]; 
        comm.send(dup(), MSG_EVENT, NULL); if (tlpid==root) break;              
        }  
    switch (type) {
        case EVENT_TYPE_WAITCOMM:   sem_wait(&comm.npos, MAX_NMSGS); break; 
        case EVENT_TYPE_RESHAPE:    sem_wait(&comm.npos, NRELAYS); break;   
        case EVENT_TYPE_NEWTASK:
        PeksContentTask* content = (PeksContentTask*)payload();
        content->print();
        break;
        }
    sem_post(&relay_events_csec);
    }
//=============================================

void PeksBlock::store() { store(homeset->homethread->tid, homeset->bsid, lbid); }
//=============================================

void PeksBlock::load() { if (data==NULL)  load(homeset->homethread->tid, homeset->bsid, lbid); }
//=============================================

void PeksBlock::send() {sem_wait(&exist); comm.send(data, MSG_BLOCK, &exist); }
//=============================================

void PeksBlock::store(int ltid, int bsid, int bid) {sem_wait(&exist); sem_wait(&monitor);
    FILE* f = block_path(homeset->homethread->hometask->name, false, ltid, bsid, bid);
    int ret = fwrite(data, 1, BLOCK_SIZE, f); if(ret!=BLOCK_SIZE) EXIT(); fclose(f); 
    sem_post(&monitor); sem_post(&exist); 
    }
//=============================================

void PeksBlock::load(int ltid, int bsid, int bid) {
    FILE * f = block_path(homeset->homethread->hometask->name, true, ltid, bsid, bid); alloc();
    int ret = fread(data, 1, BLOCK_SIZE, f);  if (ret!=BLOCK_SIZE) EXIT();  sem_post(&exist); fclose(f);  
    }
//=============================================

PeksBlockset* PeksThread::get(int i) {sem_wait(&monitor); 
    if (blocksets[i]==NULL) {blocksets[i]=new PeksBlockset(); blocksets[i]->init(this,i,hometask->tabnblocks[tid*nblksets+i]);}
    sem_post(&monitor); return blocksets[i];
    }
//=============================================

sem_t broadcast_event_csec;
void PeksTask::broadcast_event(int type, int len, void* content) {
    if (lpid!=0) return;
    sem_wait(&broadcast_event_csec);            
    current_eventid = freshid(); nevents_replied=0;                 
    PeksMsgEvent *e = PeksMsgEvent::create(taskid, 0, 0, mpi_pid, current_eventid, type, len, content);  
    e->occur(); sem_wait(&echo_complete);       
    print_echo(e);
    sem_post(&broadcast_event_csec);                                
    }
//=============================================

void PeksComm::after_irecv(int msgt, void* msgbuf) {
    if (msgt==MSG_BLOCK) {PeksMsgBlock* data = (PeksMsgBlock*)msgbuf;
        division.tasks[data->taskid]->get(data->ttid)->get(data->tbsid)->get(data->tbid)->overwrite(data); }
    if (msgt==MSG_EVENT) {PeksMsgEvent* e=(PeksMsgEvent*)msgbuf; PeksTask* t=division.get(e->taskid);
        if (t->lpid!=e->root || e->slpid==t->lpid) {                
            e->slpid=t->lpid; t->broadcast_update_event(e);         
            detached_thread(relay_detached_thread, e->dup());    }
        else t->broadcast_echo(e);                                  
        }
    }
//=============================================

void PeksComm::communicator() {MPI_Status status; int k=0;  
    while(1) {MPI_Request req;  int flag=0; int msgt;
        MPI_Irecv(msgbuf, BLOCK_SIZE, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG,  MPI_COMM_WORLD, &req);
        while (1) {if (process_exit && n==0) return; 
            MPI_Test(&req, &flag, &status); if (flag) {msgt = status.MPI_TAG-TAG_BASE;  break;} 
            sem_wait(&comm_csec); 
            if (n>0) {if (reqs[k]==NULL) reqs[k]=isend(type[k], r[k]);
                flag=0; if (!process_exit) MPI_Test(reqs[k], &flag, MPI_STATUS_IGNORE);
                if (flag) {if (sent[k]!=NULL) sem_post(sent[k]); remove(k); k=0; }  else k=(k+1)%n;
                }sem_post(&comm_csec); 
            }
        after_irecv(msgt, msgbuf); }
    }
//=============================================

void* alice_code(void* thread) {PeksThread* t = (PeksThread*)thread;
    for (int k=0; k<3; k++) {PeksBlock* b = t->get(2)->get(k); b->alloc(); b->post(); b->store(); }    
    return NULL;
    }
//=============================================

#define DEBUG_NBLOCKS 3
void* alice2_code(void* thread) {
    PeksThread* thd = (PeksThread*)thread;
    for (int k=0; k<DEBUG_NBLOCKS; k++) {PeksBlock* a = thd->get(2)->get(k); 
        a->load(); a->data->set_src(mpi_pid, thd->hometask->taskid, thd->tid, 2, k);
        a->data->set_tgt(mpi_pid+5, thd->hometask->target_taskid, thd->tid, 2, 2-k);  a->send();    }
    return NULL;
    }
//=============================================

void* bob_code(void* thread) {PeksThread* t = (PeksThread*)thread;
    for (int k=0; k<DEBUG_NBLOCKS; k++) {PeksBlock* b = t->get(2)->get(k); b->store();}
    return NULL;
    }
//=============================================

thread_func* user_codes[3] = {alice_code, bob_code, alice2_code};
//=============================================

#include "newtask.c"
int main(int argc, char *argv[]){ MPI_Init(&argc, &argv); 
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_pid); MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    printf("process_%d in %d process\n",mpi_pid,nprocs);
    sem_init(&relay_events_csec, 0, 1); sem_init(&broadcast_event_csec, 0, 1); sem_init(&comm_csec, 0, 1);
    division.init(5); 
    PeksTask* task0 = division.create_task(10);
    alice = division.create_task((char*)"alice", true, 0, 4, 5*3*3, 3, -1, 0); 
    alice->end();  
    MPI_Barrier(MPI_COMM_WORLD);   
    pthread_t comm_thread = attached_thread(communicator, NULL);            
    bob = division.create_task((char*)"bob", true, 5, 9, 5*3*3, 3, -1, 1);
    PeksContentTask* ct = PeksContentTask::create(division.taskid, 5, 5*3*3, 3, bob->taskid, 2, (char*)"alice"); 
    if (task0->lpid==0) {
        task0->broadcast_event(EVENT_TYPE_SURVEY, 8, (void*)"abcdefg");
        task0->broadcast_event(EVENT_TYPE_NEWTASK, ct->size, ct);           
        }
    alice2 = division.create_task(ct);                                      
    bob->sweep_thread();
    alice2->end();   
    bob->end();
    process_exit=1; 
    wait_thread(comm_thread);
    MPI_Finalize();  return 0;
    }
//=============================================


//=== GENERATED CODE SIGNATURES ===
void* _pa_pthd_ta__PA0_main_ta(void* _pa_in);
#ifdef _USE_MIC
__attribute__((target(mic))) void (*_pa_mpi_procs_funcs[])(void*) = { 
    #else
void (*_pa_mpi_procs_funcs[])(void*) = {
    #endif
NULL};
//=============================================

#ifdef __parray__
void _pa_main() {_pa_main_options(_pa_pthd_ta__PA0_main_ta,_pa_mpi_procs_funcs);}
//=============================================

#endif
//=== GENERATED CODE 0 TA _PA0_main_ta ===
void* _pa_pthd_ta__PA0_main_ta(void* _pa_in) {
    int _pa_nprocs_extra_demand=0;
    _pa_mpi_mainthread_complete();
    return (void*)(_pa_nprocs_extra_demand);}
//=============================================

//=== GENERATED CODE 1 TA _PA0_swprocs_ta ===
//=== THE END ===

