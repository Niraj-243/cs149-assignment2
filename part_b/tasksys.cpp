#include "tasksys.h"
#include "itasksys.h"
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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

Task_Node::Task_Node(IRunnable *runnable,TaskID task_id,int num_instances,int num_dependent){
    this->runnable = runnable;
    this->task_id = task_id;
    this->num_instances = num_instances;
    this->num_dependent = num_dependent;
}

Task_Node * DAG::insert(IRunnable * runnable,TaskID task_id,int num_instances,const std::vector<TaskID>& deps){
    Task_Node * task_node = new Task_Node(runnable,task_id,num_instances,deps.size());
    task_id_to_node.insert({task_id,task_node});
    for(auto &t:deps){
        dag[t].push_back(task_node);
    }
    return task_node;
}

void DAG::print(){
    for(int i=0;i<dag.size();i++){
        if(dag[i].empty()) continue;
        printf("task id: %d\n",i);
        for(auto &t:dag[i]){
            printf("\t\t%d, ",t->task_id);
        }
        printf("\n\n");
    }
};

void TaskSystemParallelThreadPoolSleeping::thread_function(
    std::queue<std::tuple<Task_Node *,int >> &ready_queue,
    std::mutex &queue_mutex)
{
    while(!shutdown){
        std::unique_lock<std::mutex> lock(queue_mutex);
        // if(ready_queue.empty()){
        //     lock.unlock();
        //     continue;
        // }

        queue_cv.wait(lock, [&]{
            return shutdown || !ready_queue.empty();
        });

        if(shutdown && ready_queue.empty()){
            return;
        }

        auto [node, i] = ready_queue.front();
        ready_queue.pop();
        lock.unlock();
        node->runnable->runTask(i,node->num_instances);

        {
            std::lock_guard<std::mutex> lg(*node_mutex[node->task_id]);
            node->num_finished_instances++;
        }

        fin_inst_cv[node->task_id]->notify_all();
    }
}

void TaskSystemParallelThreadPoolSleeping::thread_node_finish_check(Task_Node *node){
    std::unique_lock<std::mutex> lock(*node_mutex[node->task_id]);
    while((!shutdown) && (node->is_done == false)){
        fin_inst_cv[node->task_id]->wait(lock,[node]{
            return node->num_finished_instances == node->num_instances;
        });
        node->is_done = true;
        // for(int child:dag[node->task_id])
        total_task_finished++;
        std::unique_lock<std::mutex> lock(total_task_finished_mutex);
        total_task_finished_cv.notify_all();
        for(auto &child:dag->dag[node->task_id]){
            child->finished_dependent++;
            node_dependent_check_cv[child->task_id]->notify_all();
        }
    }

}

void push_to_queue(Task_Node * node,
    std::queue<std::tuple<Task_Node *,int >> &ready_queue,
    std::mutex &queue_mutex,std::condition_variable &queue_cv)
{
    std::unique_lock<std::mutex> lock(queue_mutex);

    for(int i=0;i<node->num_instances;i++){
        ready_queue.push({node,i});
    }
    lock.unlock();
    queue_cv.notify_all();
}

void TaskSystemParallelThreadPoolSleeping::thread_dependent_finish_check(Task_Node *node){
    while ((!shutdown) && (node->is_ready == false)){ 
        std::unique_lock<std::mutex> lock(*node_mutex[node->task_id]);
        node_dependent_check_cv[node->task_id]->wait(lock,[node]{
            return node->num_dependent == node->finished_dependent;
        });
        assert((node->is_running==false) && (node->is_ready == false) && (node->is_done == false));
        node->is_ready = true;
        push_to_queue(node,ready_queue,queue_mutex,queue_cv);
    }
}


TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    int max_task_type = 10000;
    dag = new DAG(max_task_type);
    // std::queue<std::tuple<IRunnable *,int,int >> ready_queue;
    
    int max_worker = 16;
    // Workers.resize(max_worker);
    for(int i=0;i<max_worker;i++){
        Workers.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::thread_function, this,ref(ready_queue),ref(queue_mutex)));
    }
}




TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::assign_task_id(IRunnable *runnable){
    TaskID task_id;
    if(tasks_map.find(runnable) != tasks_map.end()){
        task_id = tasks_map[runnable];
    }
    else{
        tasks_map.insert({runnable,total_tasks});
        task_id = total_tasks;
        std::condition_variable cv;
        // fin_inst_cv.push_back(cv);
        // fin_inst_cv.push_back(std::make_unique<std::condition_variable>());
        fin_inst_cv.push_back(std::unique_ptr<std::condition_variable>(new std::condition_variable()));
        // node_dependent_check_cv.push_back(cv);
        // node_dependent_check_cv.push_back(std::make_unique<std::condition_variable>());
        node_dependent_check_cv.push_back(std::unique_ptr<std::condition_variable>(new std::condition_variable()));

        std::mutex mtx;
        // node_mutex.push_back(mtx);
        node_mutex.push_back(std::unique_ptr<std::mutex>(new std::mutex()));
        total_tasks++;
    }
    return task_id;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {

    //
    // TODO: CS149 students will implement this method in Part B.
    //
    TaskID task_id = assign_task_id(runnable);
    Task_Node * task_node=dag->insert(runnable,task_id,num_total_tasks,deps);
    
    Workers.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::thread_node_finish_check,this,task_node));
    Workers.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::thread_dependent_finish_check,this,task_node));

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }

    // dag->print();

    return task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    
    // brute force test
    // for(int i=0;i<total_tasks;i++){
    //     auto node = dag->task_id_to_node[i];
    //     for(int j=0;j<node->num_instances;j++){
    //         node->runnable->runTask(j, node->num_instances);
    //     }
    // }
    
    // while(!ready_queue.empty()){
    //     printf("current queue size: %d\n",(int)ready_queue.size());
    // }
    std::unique_lock<std::mutex> lock(total_task_finished_mutex);
    total_task_finished_cv.wait(lock,[this]{
        return total_task_finished == total_tasks;
    });
    shutdown = true;
    queue_cv.notify_all();
    
    for(auto &w: Workers){
        w.join();
    }
    return;
}
