/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "Receiver.h"
#include "ServerSocket.h"
#include "FileWriter.h"
#include "SocketUtils.h"

#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/String.h>
#include <folly/ScopeGuard.h>
#include <folly/Bits.h>
#include <folly/Checksum.h>

#include <fcntl.h>
#include <unistd.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
using std::vector;
namespace facebook {
namespace wdt {
Receiver::Receiver(const WdtTransferRequest &transferRequest) {
  LOG(INFO) << "WDT Receiver " << Protocol::getFullVersion();
  transferId_ = transferRequest.transferId;
  if (transferId_.empty()) {
    transferId_ = WdtBase::generateTransferId();
  }
  setProtocolVersion(transferRequest.protocolVersion);
  setDir(transferRequest.directory);
  for (int32_t portNum : transferRequest.ports) {
    ports_.push_back(portNum);
  }
  threadsController_ =
      std::make_shared<ThreadsController>((void *)this, ports_.size());
}

Receiver::Receiver(int port, int numSockets, const std::string &destDir)
    : Receiver(WdtTransferRequest(port, numSockets, destDir)) {
}

void Receiver::addCheckpoint(Checkpoint checkpoint) {
  LOG(INFO) << "Adding global checkpoint " << checkpoint.port << " "
            << checkpoint.numBlocks << " " << checkpoint.lastBlockReceivedBytes;
  checkpoints_.emplace_back(checkpoint);
}

std::vector<Checkpoint> Receiver::getNewCheckpoints(int startIndex) {
  std::vector<Checkpoint> checkpoints;
  const int64_t numCheckpoints = checkpoints_.size();
  for (int64_t i = startIndex; i < numCheckpoints; i++) {
    checkpoints.emplace_back(checkpoints_[i]);
  }
  return checkpoints;
}

void Receiver::startNewGlobalSession(const std::string &peerIp) {
  LOG(INFO) << "Starting new transfer";
  if (throttler_) {
    // If throttler is configured/set then register this session
    // in the throttler. This is guranteed to work in either of the
    // modes long running or not. We will de register from the throttler
    // when the current session ends
    throttler_->registerTransfer();
  }
  startTime_ = Clock::now();
  const auto &options = WdtOptions::get();
  if (options.enable_download_resumption) {
    transferLogManager_.openAndStartWriter(peerIp);
  }
}

void Receiver::endCurGlobalSession() {
  LOG(INFO) << "Ending the transfer";
  const auto &options = WdtOptions::get();
  if (throttler_) {
    throttler_->deRegisterTransfer();
  }
  if (options.enable_download_resumption) {
    transferLogManager_.closeAndStopWriter();
  }
  if (options.enable_download_resumption && !options.keep_transfer_log) {
    transferLogManager_.unlink();
  }
}

WdtTransferRequest Receiver::init() {
  const auto &options = WdtOptions::get();
  auto backlog = options.backlog;
  int64_t bufferSize = options.buffer_size;
  if (bufferSize < Protocol::kMaxHeader) {
    // round up to even k
    bufferSize = 2 * 1024 * ((Protocol::kMaxHeader - 1) / (2 * 1024) + 1);
    LOG(INFO) << "Specified -buffer_size " << options.buffer_size
              << " smaller than " << Protocol::kMaxHeader << " using "
              << bufferSize << " instead";
  }
  if (!fileCreator_) {
    fileCreator_.reset(new FileCreator(destDir_, receiverThreads_.size(),
                                       transferLogManager_));
  }
  for (size_t index = 0; index < ports_.size(); index++) {
    auto portNum = ports_[index];
    ServerSocket socket(portNum, backlog, &abortCheckerCallback_);
    receiverThreads_.emplace_back(folly::make_unique<ReceiverThread>(
        socket, index, protocolVersion_, bufferSize, threadsController_));
  }
  size_t numSuccessfulInitThreads = 0;
  for (size_t i = 0; i < receiverThreads_.size(); i++) {
    ErrorCode code = receiverThreads_[i]->init();
    if (code == OK) {
      ++numSuccessfulInitThreads;
    }
  }
  LOG(INFO) << "Registered " << numSuccessfulInitThreads << " threads";
  ErrorCode code = OK;
  if (numSuccessfulInitThreads != ports_.size()) {
    code = FEWER_PORTS;
    if (numSuccessfulInitThreads == 0) {
      code = ERROR;
    }
  }
  WdtTransferRequest transferRequest(getPorts());
  transferRequest.protocolVersion = protocolVersion_;
  transferRequest.transferId = transferId_;
  LOG(INFO) << "Transfer id " << transferRequest.transferId;
  if (transferRequest.hostName.empty()) {
    char hostName[1024];
    int ret = gethostname(hostName, sizeof(hostName));
    if (ret == 0) {
      transferRequest.hostName.assign(hostName);
    } else {
      PLOG(ERROR) << "Couldn't find the host name";
      code = ERROR;
    }
  }
  transferRequest.directory = getDir();
  transferRequest.errorCode = code;
  return transferRequest;
}

void Receiver::setDir(const std::string &destDir) {
  destDir_ = destDir;
  transferLogManager_.setRootDir(destDir);
}

void Receiver::setNumBlocksSend(int64_t numBlocksSend) {
  LOG(INFO) << "Setting number of blocks sent " << numBlocksSend;
  std::unique_lock<std::mutex> lock(mutex_);
  numBlocksSend_ = numBlocksSend;
}

TransferLogManager &Receiver::getTransferLogManager() {
  return transferLogManager_;
}

std::shared_ptr<Throttler> Receiver::getThrottler() const {
  return throttler_;
}

std::unique_ptr<FileCreator> &Receiver::getFileCreator() {
  return fileCreator_;
}

void Receiver::setTotalSenderBytes(int64_t totalSenderBytes) {
  LOG(INFO) << "Setting total number of bytes " << totalSenderBytes;
  std::unique_lock<std::mutex> lock(mutex_);
  totalSenderBytes_ = totalSenderBytes;
}

const std::string &Receiver::getDir() {
  return destDir_;
}

void Receiver::setRecoveryId(const std::string &recoveryId) {
  recoveryId_ = recoveryId;
  LOG(INFO) << "recovery id " << recoveryId_;
}

Receiver::~Receiver() {
  if (hasPendingTransfer()) {
    LOG(WARNING) << "There is an ongoing transfer and the destructor"
                 << " is being called. Trying to finish the transfer";
    abort(ABORTED_BY_APPLICATION);
  }
  finish();
}

vector<int32_t> Receiver::getPorts() const {
  vector<int32_t> ports;
  for (int i = 0; i < ports_.size(); i++) {
    ports.push_back(receiverThreads_[i]->getPort());
  }
  return ports;
}

bool Receiver::hasPendingTransfer() {
  std::unique_lock<std::mutex> lock(instanceManagementMutex_);
  return !transferFinished_;
}

void Receiver::markTransferFinished(bool isFinished) {
  std::unique_lock<std::mutex> lock(instanceManagementMutex_);
  transferFinished_ = isFinished;
  if (isFinished) {
    conditionRecvFinished_.notify_one();
  }
}

std::unique_ptr<TransferReport> Receiver::finish() {
  std::unique_lock<std::mutex> instanceLock(instanceManagementMutex_);
  if (areThreadsJoined_) {
    VLOG(1) << "Threads have already been joined. Returning the "
            << "transfer report";
    return getTransferReport();
  }
  instanceLock.unlock();

  const auto &options = WdtOptions::get();
  if (!isJoinable_) {
    // TODO: don't complain about this when coming from runForever()
    LOG(WARNING) << "The receiver is not joinable. The threads will never"
                 << " finish and this method will never return";
  }
  for (auto &receiverThread : receiverThreads_) {
    receiverThread->finish();
  }
  LOG(INFO) << "Threads finished";
  // A very important step to mark the transfer finished
  // No other transferAsync, or runForever can be called on this
  // instance unless the current transfer has finished
  markTransferFinished(true);

  if (isJoinable_) {
    // Make sure to join the progress thread.
    progressTrackerThread_.join();
  }
  threadsController_->finish();
  instanceLock.lock();
  std::unique_ptr<TransferReport> report = getTransferReport();
  if (progressReporter_ && totalSenderBytes_ >= 0) {
    report->setTotalFileSize(totalSenderBytes_);
    report->setTotalTime(durationSeconds(Clock::now() - startTime_));
    progressReporter_->end(report);
  }
  if (options.enable_perf_stat_collection) {
    PerfStatReport globalPerfReport;
    for (auto &receiverThread : receiverThreads_) {
      globalPerfReport += receiverThread->getPerfReport();
    }
    LOG(INFO) << globalPerfReport;
  }

  LOG(WARNING) << "WDT receiver's transfer has been finished";
  LOG(INFO) << *report;
  receiverThreads_.clear();
  areThreadsJoined_ = true;
  return report;
}

std::unique_ptr<TransferReport> Receiver::getTransferReport() {
  TransferStats globalStats;
  const auto &options = WdtOptions::get();
  for (const auto &receiverThread : receiverThreads_) {
    globalStats += receiverThread->getTransferStats();
  }
  std::unique_ptr<TransferReport> report =
      folly::make_unique<TransferReport>(std::move(globalStats));
  const TransferStats &summary = report->getSummary();
  if (numBlocksSend_ == -1 || numBlocksSend_ != summary.getNumBlocks()) {
    // either none of the threads finished properly or not all of the blocks
    // were transferred
    report->setErrorCode(ERROR);
  } else if (totalSenderBytes_ != -1 &&
             totalSenderBytes_ != summary.getEffectiveDataBytes()) {
    // did not receive all the bytes
    LOG(ERROR) << "Number of bytes sent and received do not match "
               << totalSenderBytes_ << " " << summary.getEffectiveDataBytes();
    report->setErrorCode(ERROR);
  } else {
    report->setErrorCode(OK);
    if (options.enable_download_resumption && !options.keep_transfer_log) {
      transferLogManager_.unlink();
    }
  }
  return report;
}

ErrorCode Receiver::transferAsync() {
  const auto &options = WdtOptions::get();
  if (hasPendingTransfer()) {
    // finish is the only method that should be able to
    // change the value of transferFinished_
    LOG(ERROR) << "There is already a transfer running on this "
               << "instance of receiver";
    return ERROR;
  }
  isJoinable_ = true;
  int progressReportIntervalMillis = options.progress_report_interval_millis;
  if (!progressReporter_ && progressReportIntervalMillis > 0) {
    // if progress reporter has not been set, use the default one
    progressReporter_ = folly::make_unique<ProgressReporter>();
  }
  if (options.enable_download_resumption) {
    WDT_CHECK(!options.skip_writes)
        << "Can not skip transfers with download resumption turned on";
    transferLogManager_.parseAndMatch(recoveryId_);
  }
  start();
  return OK;
}

ErrorCode Receiver::runForever() {
  if (hasPendingTransfer()) {
    // finish is the only method that should be able to
    // change the value of transferFinished_
    LOG(ERROR) << "There is already a transfer running on this "
               << "instance of receiver";
    return ERROR;
  }

  const auto &options = WdtOptions::get();
  WDT_CHECK(!options.enable_download_resumption)
      << "Transfer resumption not supported in long running mode";

  // Enforce the full reporting to be false in the daemon mode.
  // These statistics are expensive, and useless as they will never
  // be received/reviewed in a forever running process.
  start();
  finish();
  // This method should never finish
  return ERROR;
}

void Receiver::progressTracker() {
  const auto &options = WdtOptions::get();
  // Progress tracker will check for progress after the time specified
  // in milliseconds.
  int progressReportIntervalMillis = options.progress_report_interval_millis;
  int throughputUpdateIntervalMillis =
      WdtOptions::get().throughput_update_interval_millis;
  if (progressReportIntervalMillis <= 0 || throughputUpdateIntervalMillis < 0 ||
      !isJoinable_) {
    return;
  }
  int throughputUpdateInterval =
      throughputUpdateIntervalMillis / progressReportIntervalMillis;

  int64_t lastEffectiveBytes = 0;
  std::chrono::time_point<Clock> lastUpdateTime = Clock::now();
  int intervalsSinceLastUpdate = 0;
  double currentThroughput = 0;
  LOG(INFO) << "Progress reporter updating every "
            << progressReportIntervalMillis << " ms";
  auto waitingTime = std::chrono::milliseconds(progressReportIntervalMillis);
  int64_t totalSenderBytes;
  while (true) {
    {
      std::unique_lock<std::mutex> lock(instanceManagementMutex_);
      conditionRecvFinished_.wait_for(lock, waitingTime);
      if (transferFinished_ || getCurAbortCode() != OK) {
        break;
      }
      if (totalSenderBytes_ == -1) {
        continue;
      }
      totalSenderBytes = totalSenderBytes_;
    }
    double totalTime = durationSeconds(Clock::now() - startTime_);
    TransferStats globalStats;
    for (const auto &receiverThread : receiverThreads_) {
      globalStats += receiverThread->getTransferStats();
    }
    auto transferReport = folly::make_unique<TransferReport>(
        std::move(globalStats), totalTime, totalSenderBytes);
    intervalsSinceLastUpdate++;
    if (intervalsSinceLastUpdate >= throughputUpdateInterval) {
      auto curTime = Clock::now();
      int64_t curEffectiveBytes =
          transferReport->getSummary().getEffectiveDataBytes();
      double time = durationSeconds(curTime - lastUpdateTime);
      currentThroughput = (curEffectiveBytes - lastEffectiveBytes) / time;
      lastEffectiveBytes = curEffectiveBytes;
      lastUpdateTime = curTime;
      intervalsSinceLastUpdate = 0;
    }
    transferReport->setCurrentThroughput(currentThroughput);

    progressReporter_->progress(transferReport);
  }
}

void Receiver::start() {
  startTime_ = Clock::now();
  if (hasPendingTransfer()) {
    LOG(WARNING) << "There is an existing transfer in progress on this object";
  }
  areThreadsJoined_ = false;
  LOG(INFO) << "Starting (receiving) server on ports [ " << getPorts()
            << "] Target dir : " << destDir_;
  markTransferFinished(false);
  if (!throttler_) {
    configureThrottler();
  } else {
    LOG(INFO) << "Throttler set externally. Throttler : " << *throttler_;
  }
  while (true) {
    for (auto &receiverThread : receiverThreads_) {
      receiverThread->startThread();
    }
    if (!isJoinable_) {
      for (auto &receiverThread : receiverThreads_) {
        receiverThread->finish();
      }
      continue;
    }
    break;
  }
  if (isJoinable_) {
    if (progressReporter_) {
      progressReporter_->start();
    }
    std::thread trackerThread(&Receiver::progressTracker, this);
    progressTrackerThread_ = std::move(trackerThread);
  }
}
}
}  // namespace facebook::wdt
