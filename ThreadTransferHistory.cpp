#include "ThreadTransferHistory.h"
namespace facebook {
namespace wdt {
ThreadTransferHistory::ThreadTransferHistory(DirectorySourceQueue &queue,
                                             TransferStats &threadStats)
    : queue_(queue), threadStats_(threadStats) {
}

std::string ThreadTransferHistory::getSourceId(int64_t index) {
  folly::SpinLockGuard guard(lock_);
  std::string sourceId;
  const int64_t historySize = history_.size();
  if (index >= 0 && index < historySize) {
    sourceId = history_[index]->getIdentifier();
  } else {
    LOG(WARNING) << "Trying to read out of bounds data " << index << " "
                 << history_.size();
  }
  return sourceId;
}

bool ThreadTransferHistory::addSource(std::unique_ptr<ByteSource> &source) {
  folly::SpinLockGuard guard(lock_);
  if (globalCheckpoint_) {
    // already received an error for this thread
    VLOG(1) << "adding source after global checkpoint is received. returning "
               "the source to the queue";
    markSourceAsFailed(source, lastBlockReceivedBytes_);
    lastBlockReceivedBytes_ = 0;
    queue_.returnToQueue(source);
    return false;
  }
  history_.emplace_back(std::move(source));
  return true;
}

int64_t ThreadTransferHistory::setCheckpointAndReturnToQueue(
    int64_t numReceivedSources, int64_t lastBlockReceivedBytes,
    bool globalCheckpoint) {
  folly::SpinLockGuard guard(lock_);
  const int64_t historySize = history_.size();
  if (numReceivedSources > historySize) {
    LOG(ERROR)
        << "checkpoint is greater than total number of sources transfereed "
        << history_.size() << " " << numReceivedSources;
    return -1;
  }
  if (numReceivedSources < numAcknowledged_) {
    LOG(ERROR) << "new checkpoint is less than older checkpoint "
               << numAcknowledged_ << " " << numReceivedSources;
    return -1;
  }
  int64_t numFailedSources = historySize - numReceivedSources;
  if (numFailedSources == 0 && lastBlockReceivedBytes > 0) {
    if (!globalCheckpoint) {
      LOG(ERROR) << "Invalid local checkpoint " << numFailedSources << " "
                 << lastBlockReceivedBytes;
      return -1;
    }
    lastBlockReceivedBytes_ = lastBlockReceivedBytes;
  }
  globalCheckpoint_ |= globalCheckpoint;
  numAcknowledged_ = numReceivedSources;
  std::vector<std::unique_ptr<ByteSource>> sourcesToReturn;
  for (int64_t i = 0; i < numFailedSources; i++) {
    std::unique_ptr<ByteSource> source = std::move(history_.back());
    history_.pop_back();
    int64_t receivedBytes =
        (i == numFailedSources - 1 ? lastBlockReceivedBytes : 0);
    markSourceAsFailed(source, receivedBytes);
    sourcesToReturn.emplace_back(std::move(source));
  }
  queue_.returnToQueue(sourcesToReturn);
  return numFailedSources;
}

std::vector<TransferStats> ThreadTransferHistory::popAckedSourceStats() {
  const int64_t historySize = history_.size();
  WDT_CHECK(numAcknowledged_ == historySize);
  // no locking needed, as this should be called after transfer has finished
  std::vector<TransferStats> sourceStats;
  while (!history_.empty()) {
    sourceStats.emplace_back(std::move(history_.back()->getTransferStats()));
    history_.pop_back();
  }
  return sourceStats;
}

void ThreadTransferHistory::markAllAcknowledged() {
  folly::SpinLockGuard guard(lock_);
  numAcknowledged_ = history_.size();
}

int64_t ThreadTransferHistory::returnUnackedSourcesToQueue() {
  return setCheckpointAndReturnToQueue(numAcknowledged_, 0, false);
}

void ThreadTransferHistory::markSourceAsFailed(
    std::unique_ptr<ByteSource> &source, int64_t receivedBytes) {
  TransferStats &sourceStats = source->getTransferStats();
  if (sourceStats.getErrorCode() != OK) {
    // already marked as failed
    sourceStats.addEffectiveBytes(0, receivedBytes);
    threadStats_.addEffectiveBytes(0, receivedBytes);
  } else {
    auto dataBytes = source->getSize();
    auto headerBytes = sourceStats.getEffectiveHeaderBytes();
    int64_t wastedBytes = dataBytes - receivedBytes;
    sourceStats.subtractEffectiveBytes(headerBytes, wastedBytes);
    sourceStats.decrNumBlocks();
    sourceStats.setErrorCode(SOCKET_WRITE_ERROR);
    sourceStats.incrFailedAttempts();

    threadStats_.subtractEffectiveBytes(headerBytes, wastedBytes);
    threadStats_.decrNumBlocks();
    threadStats_.incrFailedAttempts();
  }
  source->advanceOffset(receivedBytes);
}

TransferHistoryController::TransferHistoryController(
    DirectorySourceQueue &dirQueue)
    : dirQueue_(dirQueue) {
}

ThreadTransferHistory &TransferHistoryController::getTransferHistory(
    int32_t port) {
  auto it = threadHistoriesMap_.find(port);
  WDT_CHECK(it != threadHistoriesMap_.end()) << "port not found" << port;
  return it->second;
}

void TransferHistoryController::addThreadHistory(int32_t port,
                                                 TransferStats &threadStats) {
  threadHistoriesMap_.emplace(port,
                              ThreadTransferHistory(dirQueue_, threadStats));
}

void TransferHistoryController::handleCheckpoint(const Checkpoint &checkpoint) {
  auto errPort = checkpoint.port;
  auto errPoint = checkpoint.numBlocks;
  auto lastBlockReceivedBytes = checkpoint.lastBlockReceivedBytes;
  auto it = threadHistoriesMap_.find(errPort);
  if (it == threadHistoriesMap_.end()) {
    LOG(ERROR) << "Invalid checkpoint " << errPoint
               << ". No sender thread running on port " << errPort;
    return;
  }
  VLOG(1) << "received global checkpoint " << errPort << " -> " << errPoint
          << ", " << lastBlockReceivedBytes;
  it->second.setCheckpointAndReturnToQueue(errPoint, lastBlockReceivedBytes,
                                           true);
}
}
}