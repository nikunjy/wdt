/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <mutex>
#include <unordered_map>
#include "TransferLogManager.h"
#include "FileCreator.h"
#include "Throttler.h"
#include <condition_variable>
namespace facebook {
namespace wdt {
extern int64_t RUNNING, WAITING, WAITING_ERROR, ERRORED, FINISHED;

class ExecuteOnceFunc {
 public:
  ExecuteOnceFunc(int numThreads, bool execFirst) {
    execFirst_ = execFirst;
    numThreads_ = numThreads;
  }
  template <typename Func>
  void execute(const Func &execFunc) {
    std::unique_lock<std::mutex> lock(mutex_);
    ++numHits_;
    if (execFirst_) {
      if (numHits_ == 1) {
        execFunc();
      }
    } else {
      if (numHits_ == numThreads_) {
        execFunc();
      }
    }
    if (numHits_ == numThreads_) {
      numHits_ = 0;
    }
  }

 private:
  std::mutex mutex_;
  size_t numHits_{0};
  bool execFirst_{true};
  size_t numThreads_;
};

class Barrier {
 public:
  Barrier(int numThreads) {
    numThreads_ = numThreads;
  }
  void execute() {
    std::unique_lock<std::mutex> lock(mutex_);
    ++numHits_;
    if (numHits_ == numThreads_) {
      numHits_ = 0;
      cv_.notify_all();
    } else {
      cv_.wait(lock);
    }
  }
 private:
  std::condition_variable cv_;
  size_t numHits_{0};
  size_t numThreads_;
  std::mutex mutex_;
};

class FunnelExecute {
  enum FunnelStatus { FUNNEL_START, FUNNEL_PROGRESS, FUNNEL_END };

 public:
  FunnelExecute(int numThreads) {
    numThreads_ = numThreads;
  }
  template <typename StartFn, typename ProgressFn, typename EndFn>
  void execute(const StartFn &startFn, const ProgressFn &progressFn,
               const EndFn &endFn, int waitingTime = -1) {
    std::unique_lock<std::mutex> lock(mutex_);
    switch (status_) {
      case FUNNEL_START:
        status_ = FUNNEL_PROGRESS;
        lock.unlock();
        if (startFn()) {
          lock.lock();
          status_ = FUNNEL_END;
          cv_.notify_all();
        } else {
          lock.lock();
          status_ = FUNNEL_START;
          cv_.notify_one();
        }
        break;
      case FUNNEL_PROGRESS:
        lock.unlock();
        if (progressFn()) {
          lock.lock();
          if (status_ == FUNNEL_PROGRESS) {
            if (waitingTime > 0) {
              auto waitMillis = std::chrono::milliseconds(waitingTime);
              cv_.wait_for(lock, waitMillis);
            } else {
              // Wait forever till notified
              cv_.wait(lock);
            }
          }
        }
        break;
      case FUNNEL_END:
        lock.unlock();
        endFn();
    }
  }

 private:
  FunnelStatus status_{FUNNEL_START};
  std::mutex mutex_;
  std::condition_variable cv_;
  int numThreads_;
};

/**
 * Controller class responsible for the receiver
 * threads. Manages the states of threads and
 * session information
 */
class ThreadsController {
 public:
  /// Constructor that takes in the metadata
  ThreadsController(void *metaData, size_t totalThreads);

  /**
   * Mark the state of a thread
   */
  void markState(int64_t threadIndex, int64_t state);

  template <typename Func>
  void executeAtStart(const Func &func) const {
    execAtStart_->execute<decltype(func)>(func);
  }

  template <typename Func>
  void executeAtEnd(const Func &func) const {
    execAtEnd_->execute<decltype(func)>(func);
  }

  /// Returns a funnel executor shared between the threads
  /// If the executor does not exist then it creates one
  std::shared_ptr<FunnelExecute> getFunnel(const std::string &funnelName);

  /// Returns a barrier shared between the threads
  /// If the executor does not exist then it creates one
  std::shared_ptr<Barrier> getBarrier(const std::string &barrierName);

  /*
   * Return backs states of all the threads
   */
  std::unordered_map<int64_t, int64_t> getThreadStates() const;

  /// Register a thread, a thread registers with the state RUNNING
  void registerThread(int64_t threadIndex);

  /// De-register a thread, marks it ended
  void deRegisterThread(int64_t threadIndex);

  /// Finish the threads controller
  ErrorCode finish();

  /// Returns back the metadata stored like Sender/Receiver
  virtual void *getMetaData();

  /// Mark the thread a particular state if all threads are in one of the states
  /// as asked for
  bool markIfAllThreads(int64_t threadIndex, int64_t threadStates,
                        int64_t successState);

  /// Mark with succeess state if at least one thread is found with the running
  /// state
  bool markIfFound(int64_t threadIndex, int64_t threadStates,
                   int64_t successState);

 private:
  /// Metadata shared between the threads
  void *metaData_;

  /// Total number of threads managed by the thread controller
  size_t totalThreads_;

  typedef std::unique_lock<std::mutex> GuardLock;

  /// Mutex used in all the setter
  mutable std::mutex controllerMutex_;

  /// States of the threads
  std::unordered_map<int64_t, int64_t> threadStateMap_;

  /// Executor to execute things at the start of transfer
  ExecuteOnceFunc *execAtStart_;

  /// Executor to execute things at the end of transfer
  ExecuteOnceFunc *execAtEnd_;

  /// Map of funnel executors and their names
  std::unordered_map<std::string, std::shared_ptr<FunnelExecute>>
      funnelExecutors_;

  /// Map of barriers and their names
  std::unordered_map<std::string, std::shared_ptr<Barrier>> barriers_;
};
}
}
