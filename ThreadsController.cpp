#include "ThreadsController.h"
#include "WdtOptions.h"
using namespace std;
namespace facebook {
namespace wdt {
using namespace std;
int64_t RUNNING = 0;
int64_t WAITING = 1 << 1;
int64_t WAITING_ERROR = 1 << 2;
int64_t ERRORED = 1 << 3;
int64_t FINISHED = 1 << 4;

ThreadsController::ThreadsController(void* metaData, size_t totalThreads) {
  metaData_ = metaData;
  totalThreads_ = totalThreads;
  for (size_t threadNum = 0; threadNum < totalThreads; ++threadNum) {
    threadStateMap_[threadNum] = RUNNING;
  }
  execAtStart_ = new ExecuteOnceFunc(totalThreads_, true);
  execAtEnd_ = new ExecuteOnceFunc(totalThreads_, false);
}

ErrorCode ThreadsController::finish() {
  return OK;
}

void ThreadsController::registerThread(int64_t threadIndex) {
  GuardLock lock(controllerMutex_);
  auto it = threadStateMap_.find(threadIndex);
  WDT_CHECK(it != threadStateMap_.end());
  threadStateMap_[threadIndex] = RUNNING;
}

void ThreadsController::deRegisterThread(int64_t threadIndex) {
  GuardLock lock(controllerMutex_);
  auto it = threadStateMap_.find(threadIndex);
  WDT_CHECK(it != threadStateMap_.end());
  threadStateMap_[threadIndex] = FINISHED;
}

void ThreadsController::markState(int64_t threadIndex, int64_t threadState) {
  GuardLock lock(controllerMutex_);
  threadStateMap_[threadIndex] = threadState;
}

unordered_map<int64_t, int64_t> ThreadsController::getThreadStates() const {
  GuardLock lock(controllerMutex_);
  return threadStateMap_;
}

void* ThreadsController::getMetaData() {
  return metaData_;
}

bool ThreadsController::markIfAllThreads(int64_t threadIndex,
                                         int64_t threadStates,
                                         int64_t successState) {
  GuardLock lock(controllerMutex_);
  for (auto threadPair : threadStateMap_) {
    if (threadPair.first == threadIndex) {
      continue;
    }
    if (!(threadPair.second & threadStates)) {
      return false;
    }
  }
  threadStateMap_[threadIndex] = successState;
  return true;
}

bool ThreadsController::markIfFound(int64_t threadIndex, int64_t threadStates,
                                    int64_t successState) {
  GuardLock lock(controllerMutex_);
  for (auto threadPair : threadStateMap_) {
    if (threadPair.first == threadIndex) {
      continue;
    }
    if (threadPair.second & threadStates) {
      threadStateMap_[threadIndex] = successState;
      return true;
    }
  }
  return false;
}

std::shared_ptr<Barrier> ThreadsController::getBarrier(
  const std::string& barrierName) {
  GuardLock lock(controllerMutex_);
  auto it = barriers_.find(barrierName);
  if (it == barriers_.end()) {
    std::shared_ptr<Barrier> execPtr =
        std::make_shared<Barrier>(totalThreads_);
    barriers_[barrierName] = execPtr;
    return execPtr;
  }
  return it->second;
}

std::shared_ptr<FunnelExecute> ThreadsController::getFunnel(
    const std::string& funnelName) {
  GuardLock lock(controllerMutex_);
  auto it = funnelExecutors_.find(funnelName);
  if (it == funnelExecutors_.end()) {
    std::shared_ptr<FunnelExecute> execPtr =
        std::make_shared<FunnelExecute>(totalThreads_);
    funnelExecutors_[funnelName] = execPtr;
    return execPtr;
  }
  return it->second;
}
}
}
