#include "bktpool.h"
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define _GNU_SOURCE
#include <linux/sched.h>
#include <sys/syscall.h> /* Definition of SYS_* constants */

//#define DEBUG
#define INFO
//#define WORK_THREAD
#define WORK_FORK

void *bkwrk_worker(void *arg) {
    sigset_t set;
    int sig;
    int s;
    int i = *((int *)arg); // Default arg is integer of workid
    struct bkworker_t *wrk = &worker[i];

    /* Taking the mask for waking up */
    sigemptyset(&set);
    sigaddset(&set, SIGUSR1);
    sigaddset(&set, SIGQUIT);

#ifdef DEBUG
    fprintf(stderr, "worker %i start living tid %d \n", i, getpid());
    fflush(stderr);
#endif

    while (1) {
        /* wait for signal */
        s = sigwait(&set, &sig);
        if (s != 0)
            continue;

#ifdef INFO
        fprintf(stderr, "worker wake %d up\n", i);
#endif

        /* Busy running */
        if (wrk->func != NULL)
            wrk->func(wrk->arg);

        /* Advertise I DONE WORKING */
        wrkid_busy[i] = 0;
        worker[i].func = NULL;
        worker[i].arg = NULL;
        worker[i].bktaskid = -1;
    }
}

int bktask_assign_worker(unsigned int bktaskid, unsigned int wrkid) {
    if (wrkid < 0 || wrkid >= MAX_WORKER)
        return -1;

    struct bktask_t *tsk = bktask_get_byid(bktaskid);

    if (tsk == NULL)
        return -1;

    /* Advertise I AM WORKING */
    wrkid_busy[wrkid] = 1;

    worker[wrkid].func = tsk->func;
    worker[wrkid].arg = tsk->arg;
    worker[wrkid].bktaskid = bktaskid;

    printf("Assign tsk %d wrk %d \n", tsk->bktaskid, wrkid);
    return 0;
}

int bkwrk_create_worker() {
    unsigned int i;

    // Create worker processes
    for (i = 0; i < MAX_WORKER; i++) {
#ifdef WORK_THREAD
        // Thread-based implementation
        // (code omitted for brevity)
#else // WORK_FORK
        pid_t pid = fork();
        if (pid == -1) {
            perror("fork");
            exit(EXIT_FAILURE);
        }
        if (pid == 0) { // Child process
            bkwrk_worker((void *)&i); // Execute worker function
            exit(EXIT_SUCCESS); // Ensure child process exits after completing the task
        }
        wrkid_tid[i] = pid;
#ifdef INFO
        fprintf(stderr, "bkwrk_create_worker got worker %d\n", pid);
#endif

        // Assign tasks to workers
        // Example task assignment logic:
        int task_id = i; // Assigning task with ID same as worker ID for simplicity
        bktask_assign_worker(task_id, i);
#endif
    }

    return 0;
}

int bkwrk_get_worker() {
    /* The return value is the ID of the worker which is not currently
    * busy or wrkid_busy[1] == 0
    */
    for (int i = 0; i < MAX_WORKER; i++) {
        if (!wrkid_busy[i]) {
            return i;
        }
    }
    return -1;
}

int bkwrk_dispatch_worker(unsigned int wrkid) {
#ifdef WORK_THREAD
    unsigned int tid = wrkid_tid[wrkid];

    /* Invalid task */
    if (worker[wrkid].func == NULL)
        return -1;

#ifdef DEBUG
    fprintf(stderr, "bkwrk dispatch wrkid %d - send signal %u \n", wrkid, tid);
#endif

    syscall(SYS_tkill, tid, SIG_DISPATCH);
#else // WORK_FORK
    pid_t pid = wrkid_tid[wrkid];

    /* Invalid task or worker */
    if (worker[wrkid].func == NULL || pid < 0)
        return -1;

#ifdef DEBUG
    fprintf(stderr, "bkwrk dispatch wrkid %d - send signal %d \n", wrkid, pid);
#endif

    // Send signal to wake up the worker process
    if (kill(pid, SIG_DISPATCH) == -1) {
        perror("kill");
        return -1;
    }
#endif
    return 0;
}


