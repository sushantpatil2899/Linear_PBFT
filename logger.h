#pragma once
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <unordered_set>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <ctime>

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;
using std::cout;
using std::endl;
using std::ios;
using std::lock_guard;
using std::mutex;
using std::ofstream;
using std::put_time;
using std::runtime_error;
using std::setfill;
using std::setw;
using std::stringstream;
using std::string;
using std::unordered_set;
using std::localtime;

class Logger {
public:
    enum class Level { DEBUG, INFO, WARN, ERROR, MSG_IN, MSG_OUT };
    enum class Output { TERMINAL, FILE, BOTH, NONE };

    Logger(const string& filename = "log.txt",
           const unordered_set<Level>& defaultLevels = {Level::INFO, Level::WARN, Level::ERROR},
           Output outputMode = Output::FILE)
        : filename_(filename), outputMode_(outputMode)
    {   cout<<"Filename passed to the logger is "<<filename<<" and set file name is "<<filename_<<"\n";
        if (outputMode_ == Output::FILE || outputMode_ == Output::BOTH) {
            openLogFile();
        }
        enabledLevels_ = defaultLevels;
    }

    void enable(Level level) {
        lock_guard<mutex> lock(mutex_);
        enabledLevels_.insert(level);
    }

    void disable(Level level) {
        lock_guard<mutex> lock(mutex_);
        enabledLevels_.erase(level);
    }

    void setOutputMode(Output mode) {
        lock_guard<mutex> lock(mutex_);
        outputMode_ = mode;

        if ((outputMode_ == Output::FILE || outputMode_ == Output::BOTH) && !logFile_.is_open()) {
            openLogFile();
        }
    }

    string log(Level level, const string& msg) {
        lock_guard<mutex> lock(mutex_);
        if (enabledLevels_.find(level) == enabledLevels_.end()) return "";

        string logMsg = timestamp() + " [" + levelToString(level) + "] " + msg;

        if (outputMode_ == Output::TERMINAL || outputMode_ == Output::BOTH) {
            cout << logMsg << endl;
        }

        if ((outputMode_ == Output::FILE || outputMode_ == Output::BOTH) && logFile_.is_open()) {
            logFile_ << logMsg << endl;
            logFile_.flush();
        }
        return logMsg;
    }

private:
    void openLogFile() {
        logFile_.open(filename_, ios::out | ios::trunc); // overwrite mode
        if (!logFile_.is_open()) {
            throw runtime_error("Failed to open log file: " + filename_);
        }
    }

    string timestamp() const {
        auto now = system_clock::now();
        auto itt = system_clock::to_time_t(now);
        auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;

        stringstream ss;
        ss << put_time(localtime(&itt), "%Y-%m-%d %H:%M:%S");
        ss << '.' << setfill('0') << setw(3) << ms.count();
        return ss.str();
    }

    string levelToString(Level level) const {
        switch (level) {
            case Level::DEBUG: return "DEBUG";
            case Level::INFO: return "INFO";
            case Level::WARN: return "WARN";
            case Level::ERROR: return "ERROR";
            case Level::MSG_IN: return "MSG_IN";
            case Level::MSG_OUT: return "MSG_OUT";
        }
        return "UNKNOWN";
    }

    string filename_;
    unordered_set<Level> enabledLevels_;
    mutable mutex mutex_;
    ofstream logFile_;
    Output outputMode_;
};
