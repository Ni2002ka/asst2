#include "tasksys.h"
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <cstdio>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

static void run_subtask_thread(runThreadArgs* thread_args) {
    IRunnable* runnable = thread_args->runnable;
    for (int i = thread_args->tasks_start_id; i < thread_args->tasks_end_id; i++){
        runnable->runTask(i, thread_args->total_tasks);
    }
}

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    // Create thread pool with number of threads
    _num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}


void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    std::thread workers[_num_threads];
    runThreadArgs args[_num_threads];

    int batch_size = num_total_tasks / _num_threads;
    for (int i = 0; i < _num_threads; i++){
        args[i].runnable = runnable;
        args[i].thread_ID = i;
        args[i].tasks_start_id = i * batch_size;
        args[i].tasks_end_id = (i == _num_threads - 1) ? num_total_tasks : args[i].tasks_start_id + batch_size;
        args[i].total_tasks = num_total_tasks;
    }


    for (int i=1; i<_num_threads; i++) {
        workers[i] = std::thread(run_subtask_thread, &args[i]);
    }
    run_subtask_thread(&args[0]);

    for (int i=1; i<_num_threads; i++) {
        workers[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    
    _num_threads = num_threads;
    current_task = new Task;
    thread_lock = new std::mutex;
    system_init = false;
    system_shutdown.store(false);

    _workers = new std::thread[num_threads];

    for (int i = 1; i < num_threads; i++){
        _workers[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::wait_for_work, this, i);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // printf("state %d\n", current_task->task_completed.load());
    system_shutdown.store(true);
    // Join threads
    for (int i = 1; i < _num_threads; i++){
        if (_workers[i].joinable()) {
            _workers[i].join();
        }
    }
    delete[] _workers;
    delete current_task;
    delete thread_lock;

}

void TaskSystemParallelThreadPoolSpinning::wait_for_work(int thread_ID) {

    while (!system_shutdown.load()){

        thread_lock->lock();
        if (!system_init || current_task->next_task >= current_task->total_tasks) {
            thread_lock->unlock();
            continue;
        } 

        int task_ID = current_task->next_task++;

        thread_lock->unlock();

        current_task->runnable->runTask(task_ID, current_task->total_tasks);
        thread_lock->lock();
        current_task->completed_tasks++;
        if (current_task->completed_tasks >= current_task->total_tasks){
            current_task->task_completed.store(true);
        }
        thread_lock->unlock();
    }

}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {

    thread_lock->lock();
    current_task->runnable = runnable;
    current_task->total_tasks = num_total_tasks;
    current_task->next_task = 0;
    current_task->completed_tasks = 0;
    current_task->task_completed.store(false);
    system_init = true;
    thread_lock->unlock();
    

    while (!current_task->task_completed.load()){
        // Spin
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    _num_threads = num_threads;
    num_tasks = 0;
    thread_lock = new std::mutex;
    system_init = false;

    _workers = new std::thread[num_threads];

    for (int i = 1; i < num_threads; i++){
        _workers[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::wait_for_work, this, i);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    
    // printf("destructor called\n");
    done_flag.store(true);
    cv_notif_threads.notify_all();
    // Join threads
    for (int i = 1; i < _num_threads; i++){
        if (_workers[i].joinable()) {
            _workers[i].join();
        }
    }
    delete[] _workers;
    delete thread_lock;
}

TaskID TaskSystemParallelThreadPoolSleeping::assigned_task() {

    for (int i = 0; i<num_tasks; i++) {
        // printf("Iter %d of %d, len check %ld\n", i, num_tasks, task_queue.size());
        Task* t = task_queue[i];
        if (t->task_completed.load() || (t->task_started.load() && !t->recruit_workers.load()))    continue;
        bool deps_satisfied = true;
        for (TaskID id: (t->deps)) {
            if (!task_queue[id]->task_completed.load()) {
                // printf("deps %d not satisfied\n", id);
                deps_satisfied = false;
                break;
            }
        }

        if (deps_satisfied){
            return i;
        }
    }

    // If no task could be processed, return -1
    return -1;
}

void TaskSystemParallelThreadPoolSleeping::wait_for_work(int thread_ID) {

    TaskID current_task_id = -1;

    while (!done_flag.load()){

        std::unique_lock<std::mutex> notif_lock(*thread_lock);
        
        if (!system_init || (current_task_id == -1)) {
            // printf("Thread %d waiting for work...\n", thread_ID);
            cv_notif_threads.wait(notif_lock);
        } 
        // printf("Thread %d notified\n", thread_ID);

        current_task_id = assigned_task();
        // printf("Assigned task %d to worker %d\n", current_task_id, thread_ID);
        if (current_task_id == -1) continue;

        Task* current_task = task_queue[current_task_id];
        // printf("Next: %d, total: %d \n", current_task->next_task, current_task->total_tasks);
        int task_ID = current_task->next_task++;
        if (current_task->next_task >= current_task->total_tasks) {
            // printf("Here\n");
            current_task->recruit_workers.store(false);
            // continue;
        }
        current_task->task_started.store(true);

        notif_lock.unlock();
    
        current_task->runnable->runTask(task_ID, current_task->total_tasks);
        thread_lock->lock();
        // printf("Worker %d done\n", thread_ID);
        current_task->completed_tasks++;
        if (current_task->completed_tasks >= current_task->total_tasks){            
            current_task->task_completed.store(true);
            // Since a new task has been completed, notify other threads to check
            // if new task is now available
            cv_notif_threads.notify_all();
        }
        thread_lock->unlock();
    }

}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    // printf("Called run\n");
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync(); // Wait for queued task to finish
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {

    // Add task to queue 
    // printf("Called runAsyncWithDeps\n");
    Task* t = new Task;
    t->runnable = runnable;
    t->completed_tasks = 0;
    t->next_task = 0;
    t->total_tasks = num_total_tasks;
    t->deps = deps;
    t->task_completed.store(false);
    t->recruit_workers.store(true);
    t->task_started.store(false);
    thread_lock->lock();
    t->ID = num_tasks++;
    task_queue.push_back(t);
    system_init = true;
    thread_lock->unlock();

    // Notify threads to go look for tasks to work on
    cv_notif_threads.notify_all();
    
    return t->ID;
}

void TaskSystemParallelThreadPoolSleeping::clean_up_batch() {

    thread_lock->lock();
    num_tasks = 0;
    system_init = false;
    for (Task* t : task_queue) {
        // Free each queue element
        delete t;
    }
    task_queue.clear();
    // printf("Reset for next batch\n");
    thread_lock->unlock();
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    // Go thru queue and check that all have been completed 
    // printf("Called sync\n");
    bool done = false;
    while (!done) {
        thread_lock->lock();
        for (Task* t : task_queue) {
            done = true;
            if (!t->task_completed.load()) {
                done = false;
                break;
            }
        }
        thread_lock->unlock();
    }
    
    
    // Clean up and reset for new runs
    clean_up_batch();
    return;
}
