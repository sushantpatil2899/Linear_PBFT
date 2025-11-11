#include <queue>
#include <condition_variable>
using namespace std;

class ThreadPool {
public:
    explicit ThreadPool(size_t threads = 4) : stop_(false) {
        for (size_t i = 0; i < threads; ++i) {
            workers_.emplace_back([this] {
                while (true) {
                    function<void()> task;
                    {
                        unique_lock<mutex> lock(mx_);
                        cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
                        if (stop_ && tasks_.empty()) return;
                        task = move(tasks_.front());
                        tasks_.pop();
                    }
                    task();
                }
            });
        }
    }

    template<class F>
    void enqueue(F&& f) {
        {
            unique_lock<mutex> lock(mx_);
            tasks_.emplace(forward<F>(f));
        }
        cv_.notify_one();
    }

    ~ThreadPool() {
        {
            unique_lock<mutex> lock(mx_);
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& w : workers_) w.join();
    }

private:
    vector<thread> workers_;
    queue<function<void()>> tasks_;
    mutex mx_;
    condition_variable cv_;
    bool stop_;
};
