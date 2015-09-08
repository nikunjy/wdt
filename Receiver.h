/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include "WdtBase.h"
#include "FileCreator.h"
#include "ErrorCodes.h"
#include "WdtOptions.h"
#include "Reporting.h"
#include "ServerSocket.h"
#include "Protocol.h"
#include "Writer.h"
#include "Throttler.h"
#include "ReceiverThread.h"
#include "TransferLogManager.h"
#include "ThreadsController.h"
#include <memory>
#include <string>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <chrono>

namespace facebook {
namespace wdt {
class ReceiverThread;
/**
 * Receiver is the receiving side of the transfer. Receiver listens on ports
 * accepts connections, receives the files and writes to the destination
 * directory. Receiver has two modes of operation : You can spawn a receiver
 * for one transfer or alternatively it can also be used in a long running
 * mode where it accepts subsequent transfers and runs in an infinte loop.
 */
class Receiver : public WdtBase {
 public:
  /// Constructor using wdt transfer request (@see in WdtBase.h)
  explicit Receiver(const WdtTransferRequest &transferRequest);

  /**
   * Constructor with start port, number of ports and directory to write to.
   * If the start port is specified as zero, it auto configures the ports
   */
  Receiver(int port, int numSockets, const std::string &destDir);

  /// Setup before starting (@see WdtBase.h)
  WdtTransferRequest init() override;

  /**
   * Joins on the threads spawned by start. This method
   * is called by default when the wdt receiver is expected
   * to run as forever running process. However this has to
   * be explicitly called when the caller expects to conclude
   * a transfer.
   */
  std::unique_ptr<TransferReport> finish() override;

  /**
   * Call this method instead of transferAsync() when you don't
   * want the wdt receiver to stop after one transfer.
   */
  ErrorCode runForever();

  /**
   * Starts the threads, and returns. Caller should call finish() after
   * calling this method to get the statistics of the transfer.
   */
  ErrorCode transferAsync() override;

  /// Setter for the directory where the files received are written to
  void setDir(const std::string &destDir);

  /// Get the dir where receiver is transferring
  const std::string &getDir();

  /// @param recoveryId   unique-id used to verify transfer log
  void setRecoveryId(const std::string &recoveryId);

  /**
   * Destructor for the receiver. The destructor automatically cancels
   * any incomplete transfers that are going on. 'Incomplete transfer' is a
   * transfer where there is no receiver thread that has received
   * confirmation from wdt sender that the transfer is 'DONE'. Destructor also
   * internally calls finish() for every transfer if finish() wasn't called
   */
  virtual ~Receiver();

  /**
   * Take a lock on the instance mutex and return the value of
   * whether the existing transfer has been finished
   */
  bool hasPendingTransfer();

  /**
   * Use the method to get the list of ports receiver is listening on
   */
  std::vector<int32_t> getPorts() const;

 protected:
  friend class ReceiverThread;

  /// Get file creator, used by receiver threads
  std::unique_ptr<FileCreator> &getFileCreator();

  /// Get the throttler this receiver is using
  std::shared_ptr<Throttler> getThrottler() const;

  /// Get the ref to transfer log manager
  TransferLogManager &getTransferLogManager();

  /// Used by receiver threads to set number of blocks sent by sender
  void setNumBlocksSend(int64_t numBlocksSend);

  /// Used by receiver threads to set number of bytes sent by sender
  void setTotalSenderBytes(int64_t totalSenderBytes);

  /**
   * @param isFinished         Mark transfer active/inactive
   */
  void markTransferFinished(bool isFinished);

  /// Responsible for basic setup and starting threads
  void start();

  /**
   * Periodically calculates current transfer report and send it to progress
   * reporter. This only works in the single transfer mode.
   */
  void progressTracker();

  /**
   * Adds a checkpoint to the global checkpoint list
   * @param checkpoint    checkpoint to be added
   */
  void addCheckpoint(Checkpoint checkpoint);

  /**
   * @param startIndex    number of checkpoints already transferred by the
   *                      calling thread
   * @return              list of new checkpoints
   */
  std::vector<Checkpoint> getNewCheckpoints(int startIndex);

  /**
   * Start new transfer by incrementing transferStartedCount_
   * A thread must hold lock on mutex_ before calling this
   */
  void startNewGlobalSession(const std::string &peerIp);

  /// Ends current global session
  void endCurGlobalSession();

  /**
   * Get transfer report, meant to be called after threads have been finished
   * This method is not thread safe
   */
  std::unique_ptr<TransferReport> getTransferReport();

  /// The thread that is responsible for calling running the progress tracker
  std::thread progressTrackerThread_;
  /**
   * Flags that represents if a transfer has finished. Threads on completion
   * set this flag. This is always accurate even if you don't call finish()
   * No transfer can be started as long as this flag is false.
   */
  bool transferFinished_{true};

  /// Flag based on which threads finish processing on receiving a done
  bool isJoinable_{false};

  /// Destination directory where the received files will be written
  std::string destDir_;

  /// Responsible for writing files on the disk
  std::unique_ptr<FileCreator> fileCreator_;

  /**
   * Unique-id used to verify transfer log. This value must be same for
   * transfers across resumption
   */
  std::string recoveryId_;

  /**
   * Progress tracker thread is a thread which has to be joined when the
   * transfer is finished. The root thread in finish() and the progress
   * tracker coordinate with each other through the boolean and this
   * condition variable.
   */
  std::condition_variable conditionRecvFinished_;

  /**
   * The instance of the receiver threads are stored in this vector.
   * This will not be destroyed until this object is destroyed, hence
   * it has to be made sure that these threads are joined at least before
   * the destruction of this object.
   */
  std::vector<std::unique_ptr<ReceiverThread>> receiverThreads_;

  /// Transfer log manager
  TransferLogManager transferLogManager_;

  /// Enum representing status of file chunks transfer
  enum SendChunkStatus { NOT_STARTED, IN_PROGRESS, SENT };

  /// State of the receiver when sending file chunks in sendFileChunksCmd
  SendChunkStatus sendChunksStatus_{NOT_STARTED};

  /**
   * All threads coordinate with each other to send previously received file
   * chunks using this condition variable.
   */
  mutable std::condition_variable conditionFileChunksSent_;

  /// Number of blocks sent by the sender
  int64_t numBlocksSend_{-1};

  /// Global list of checkpoints
  std::vector<Checkpoint> checkpoints_;

  /// Total number of data bytes sender wants to transfer
  int64_t totalSenderBytes_{-1};

  /// Start time of the session
  std::chrono::time_point<Clock> startTime_;

  /// Mutex to guard all the shared variables
  mutable std::mutex mutex_;

  /// Condition variable to coordinate transfer finish
  mutable std::condition_variable conditionAllFinished_;

  /**
   * Returns true if threads have been joined (done in finish())
   * This is how destructor determines whether it should join threads
   */
  bool areThreadsJoined_{false};

  /// Number of active threads, decremented every time a thread is finished
  int32_t numActiveThreads_{0};

  std::shared_ptr<ThreadsController> threadsController_;

  /**
   * Mutex for the management of this instance, specifically to keep the
   * instance sane for multi threaded public API calls
   */
  std::mutex instanceManagementMutex_;
};
}
}  // namespace facebook::wdt
