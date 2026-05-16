#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */

 struct Task_Node{
    IRunnable * runnable;
    TaskID task_id;
    int num_instances;
    std::atomic<int> num_finished_instances {0};
    int num_dependent;
    std::atomic<int> finished_dependent{0};
    bool is_running=false;
    bool is_done=false;
    bool is_ready = false;
    // Task_Node(TaskID task_id,int num_instances,int num_dependent){
    //     this->task_id = task_id;
    //     this->num_instances = num_instances;
    //     this->num_dependent = num_dependent;
    // }
    Task_Node(IRunnable * runnable,TaskID task_id,int num_instances,int num_dependent);
};

class DAG{
    public:
    std::vector<std::vector<Task_Node *>> dag;
    std::map<TaskID,Task_Node *> task_id_to_node;

    DAG(int max_task_type){
        dag.resize(max_task_type,std::vector<Task_Node*>());
    }

    // void insert(TaskID task_id,int num_instances,std::vector<TaskID>& deps){
    //     Task_Node * task_node = new Task_Node(task_id,num_instances,deps.size());
    //     task_id_to_node.insert({task_id,task_node});

    //     for(auto &t:deps){
    //         dag[t].push_back(task_node);
    //     }
    // }
    Task_Node * insert(IRunnable * runnable,TaskID task_id,int num_instances,const std::vector<TaskID>& deps);
    void print();
};
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        TaskID assign_task_id(IRunnable * runnable);
        std::map<IRunnable *,TaskID> tasks_map;
        std::queue<std::tuple<Task_Node *,int >> ready_queue;
        std::atomic<int> total_tasks{0};
        DAG * dag;
        std::condition_variable cv;
        std::mutex queue_mutex;
        std::vector<std::thread> Workers;
        void thread_function(std::queue<std::tuple<Task_Node *,int >> &ready_queue,std::mutex &queue_mutex);
        void sync();
        // std::vector<std::condition_variable> fin_inst_cv;
        std::vector<std::unique_ptr<std::condition_variable>> fin_inst_cv;
        // std::vector<std::mutex> node_mutex;
        std::vector<std::unique_ptr<std::mutex>> node_mutex;

        // std::vector<std::condition_variable> node_dependent_check_cv;
        std::vector<std::unique_ptr<std::condition_variable>> node_dependent_check_cv;

        bool shutdown = false;
        std::condition_variable queue_cv;
        void thread_node_finish_check(Task_Node *node);
        void thread_dependent_finish_check(Task_Node *node);
        std::atomic<int> total_task_finished{0};
        std::mutex total_task_finished_mutex;
        std::condition_variable total_task_finished_cv;

};

#endif
