// creating new tasks
#include <unistd.h>//@
void PeksTask::sweep_dir() {char cmd[MAX_PATH*2];   // this cmd may try to remove a shared dir more than once
    sprintf(cmd,"rm -r -f %s/%s 2>/dev/null",root_path,name); system(cmd); // modify this to del blocks of local threads
}
void PeksTask::sweep_thread() {// communicator creates a thread to sweep and resumes messaging
    if (lpid<0) return;   // this proc is not a member of the task
    detached_thread(sweep_task, this);  comm.remove_reqs_of_task(taskid);
}
void PeksTask::sweep() {  // we do not care whether this succeeds or not; but make sure no more actions
    for (int i=0; i<nthreads; i++) get(i)->sweep();  sweep_dir();
}
void PeksTask::begin(thread_func code) {if (lpid<0) return;
    for (int i=0; i<nthreads; i++) if (lpids[i]==lpid) get(i)->thread=attached_thread(code, get(i));
}
void PeksTask::end() {
    if (lpid<0) return;  // Only a member proc sleeps to receive messages.
    for (int i=0; i<nthreads; i++) if (lpids[i]==lpid) wait_thread(get(i)->thread);
    sleep(THREAD_EXIT_WAIT_SEC);
}

// In future task0 should be specified in a common file of IP adrs
void PeksTask::init(int i, int nthreads, int nblksets, int* tabnblocks, int lpid, int *lpids, int npt, int* pids) {// task0 task default
    source_taskid = target_taskid = 0;  nlprocs=npt; this->lpid=lpid; this->pids=pids; this->lpids=lpids;
    threads = (PeksThread**)calloc(nthreads, sizeof(void*)); succ = (int*)calloc(nlprocs*NRELAYS, sizeof(int));
    taskid=i; this->nthreads=nthreads; this->nblksets=nblksets; this->tabnblocks=tabnblocks; 
    max_nevents_replied=bdcast_succ_nroot(nthreads);
    for (int root=0; root<nlprocs; root++) for (int k=0;k<NRELAYS;k++) succ[root*NRELAYS+k] = bdcast_succ(lpid, root, k, nlprocs);
    items=(PeksItem*)calloc(nthreads, sizeof(PeksItem));    // after reshaping a task0 task, this must be reallocated
    done_init();
}
// Init a subtask from a task0
void PeksTask::init(PeksTask* task0, int taskid, int nthds, int* superlpids, int nblocks, int nblksets) {
    int nblocks_bs = (nblocks+nblksets-1)/nblksets;  // task0 task lpids 1-onto-1 corresponding with threads
    int* tabbs = (int*)malloc(sizeof(int)*nthds*nblksets); for (int i=0; i<nthds*nblksets; i++) tabbs[i]=nblocks_bs;
    int* tablpids = (int*)malloc(sizeof(int)*nthds); for (int i=0; i<nthds; i++) tablpids[i]=i;  // one thread per proc
    int* tabpids = (int*)malloc(sizeof(int)*nthds); lpid = -1;
    for (int i=0; i<nthds; i++) {tabpids[i]=task0->pids[superlpids[i]]; if (tabpids[i]==mpi_pid) lpid=i; }
    init(taskid, nthds, nblksets, tabbs, lpid, tablpids, nthds, tabpids);
}
// Init a subtask 1 thread per proc in an index range
void PeksTask::init(PeksTask* task0, int taskid, int lpid_min, int lpid_max, int nblocks, int nblksets) {
    int nthds = lpid_max-lpid_min+1;  int* superlpids = (int*)malloc(sizeof(int)*nthds);        // make a default lpids
    for (int i=lpid_min; i<=lpid_max; i++) superlpids[i-lpid_min]=i;
    init(task0, taskid, nthds, superlpids, nblocks, nblksets); free(superlpids);
}

PeksTask* PeksDivision::create_task(int nprocs) {                   // creating the super task, super does not have blocksets
    int* table0 = (int*)malloc(sizeof(int) * nprocs);  for (int i=0; i<nprocs; i++) table0[i]=i;
    tasks[0] = new PeksTask((char*)"super");  tasks[0]->init(0, nprocs, 0, NULL, mpi_pid, table0, nprocs, table0);
    taskid=1; return tasks[0];
}
PeksTask* PeksDivision::create_task(char* name, int if_sweep, int lpid_min, int lpid_max, int nblocks, int nblksets, int tgt_taskid, int codeid) {
    sem_wait(&monitor);
    printf("create task %d: name=%s, sweep=%d, nblocks=%d, nblksets=%d, codeid=%d, taskid=%d, tgt=%d, lpid[%d,%d]\n", mpi_pid,
        name, if_sweep, nblocks, nblksets, codeid, taskid, tgt_taskid, lpid_min, lpid_max);
    tasks[taskid]=new PeksTask(name); if (if_sweep) tasks[taskid]->sweep_dir();
    tasks[taskid]->init(tasks[0], taskid, lpid_min, lpid_max, nblocks, nblksets);
    if (tgt_taskid>=0) tasks[taskid]->target_taskid=tgt_taskid;
    tasks[taskid]->begin(user_codes[codeid]);
    PeksTask* r = tasks[taskid]; taskid++;
    sem_post(&monitor);
    return r;
}
PeksTask* PeksDivision::create_task(PeksContentTask* content) {
    return create_task(content->get_name(), false, 0, content->nprocs-1, content->nblocks, content->nblksets, 
        content->tgt_taskid, content->codeid);
}
