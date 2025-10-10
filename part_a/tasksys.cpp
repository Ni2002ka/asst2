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
    current_task = new Task;
    thread_lock = new std::mutex;
    system_init = false;
    system_shutdown.store(false);

    _workers = new std::thread[num_threads];

    for (int i = 1; i < num_threads; i++){
        _workers[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::wait_for_work, this, i);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    system_shutdown.store(true);
    cv_notif_threads.notify_all();
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

void TaskSystemParallelThreadPoolSleeping::wait_for_work(int thread_ID) {

    while (!system_shutdown.load()){

        std::unique_lock<std::mutex> notif_lock(*thread_lock);
        
        while (!system_init || !current_task->recruit_workers.load()) {
            // printf("Thread %d waiting for work...\n", thread_ID);
            cv_notif_threads.wait(notif_lock);
            if (system_shutdown.load())     return;
        } 

        if (current_task->next_task >= current_task->total_tasks) {
            current_task->recruit_workers.store(false);
            continue;
        }
        int task_ID = current_task->next_task++;

        notif_lock.unlock();

        current_task->runnable->runTask(task_ID, current_task->total_tasks);
        thread_lock->lock();
        current_task->completed_tasks++;
        if (current_task->completed_tasks >= current_task->total_tasks){
            current_task->task_completed.store(true);
        }
        thread_lock->unlock();
    }

}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    thread_lock->lock();
    current_task->runnable = runnable;
    current_task->total_tasks = num_total_tasks;
    current_task->next_task = 0;
    current_task->completed_tasks = 0;
    current_task->task_completed.store(false);
    current_task->recruit_workers.store(true);
    system_init = true;
    thread_lock->unlock();
    cv_notif_threads.notify_all();
    

    while (!current_task->task_completed.load()){
        // Spin
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
