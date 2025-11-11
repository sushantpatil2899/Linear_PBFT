#pragma once
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <functional>

using namespace std;

class ResettableTimer {
public:
    ResettableTimer() : stop_(false), active_(false), reset_(false) {}

    ~ResettableTimer() {
        stop();
    }

    void start(chrono::milliseconds duration, function<void()> callback) {
        {
            lock_guard<mutex> lock(mtx_);
            duration_ = duration;
            callback_ = move(callback);
            active_ = true;
            reset_ = false;
            stop_ = false;
        }
        cv_.notify_one();

        if (!worker_.joinable()) {
            worker_ = thread(&ResettableTimer::run, this);
        }
    }

    void reset() {
        lock_guard<mutex> lock(mtx_);
        if (!stop_) {
            active_ = true;
            reset_ = true;
            cv_.notify_one();
        }
    }

    void reset(chrono::milliseconds new_duration) {
        lock_guard<mutex> lock(mtx_);
        if (!stop_) {
            duration_ = new_duration;
            active_ = true;
            reset_ = true;
            cv_.notify_one();
        }
    }

    bool is_running() const {
        lock_guard<mutex> lock(mtx_);
        return active_ && !stop_;
    }

    void stop() {
        {
            lock_guard<mutex> lock(mtx_);
            stop_ = true;
            active_ = false;
            reset_ = false;
        }
        cv_.notify_one();
        if (worker_.joinable()) worker_.join();
    }

private:
    void run() {
        unique_lock<mutex> lock(mtx_);
        while (!stop_) {
            if (!active_) {
                cv_.wait(lock, [this] { return active_ || stop_; });
            }
            if (stop_) break;

            auto expiry = chrono::steady_clock::now() + duration_;
            bool expired = !cv_.wait_until(lock, expiry, [this] { return !active_ || stop_ || reset_; });

            if (stop_) break;
            if (reset_) {
                reset_ = false;
                continue;
            }

            if (expired && active_) {
                active_ = false;
                lock.unlock();
                try {
                    if (callback_) callback_();
                } catch (...) {
                }
                lock.lock();
            }
        }
    }

    mutable mutex mtx_;
    condition_variable cv_;
    thread worker_;
    atomic<bool> stop_;
    bool active_;
    bool reset_;
    chrono::milliseconds duration_;
    function<void()> callback_;
};
