/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once
#include <thread>
#include "Sender.h"
#include "ClientSocket.h"
#include "Reporting.h"
#include "ThreadTransferHistory.h"
#include "ThreadsController.h"
namespace facebook {
namespace wdt {
class DirectorySourceQueue;
/// state machine states
enum SenderState {
  CONNECT,
  READ_LOCAL_CHECKPOINT,
  SEND_SETTINGS,
  SEND_BLOCKS,
  SEND_DONE_CMD,
  SEND_SIZE_CMD,
  CHECK_FOR_ABORT,
  READ_FILE_CHUNKS,
  READ_RECEIVER_CMD,
  PROCESS_DONE_CMD,
  PROCESS_WAIT_CMD,
  PROCESS_ERR_CMD,
  PROCESS_ABORT_CMD,
  PROCESS_VERSION_MISMATCH,
  END
};

class SenderThread {
 public:
  SenderThread(int threadIndex, int64_t port,
               std::unique_ptr<DirectorySourceQueue> &dirQueue,
               std::shared_ptr<TransferHistoryController> historyController,
               std::shared_ptr<ThreadsController> threadsController)
      : threadIndex_(threadIndex),
        port_(port),
        dirQueue_(dirQueue),
        transferHistoryController_(historyController),
        controller_(threadsController) {
    controller_->registerThread(threadIndex_);
    transferHistoryController_->addThreadHistory(port_, threadStats_);
    threadStats_.setId(folly::to<std::string>(threadIndex_));
  }

  typedef SenderState (SenderThread::*StateFunction)();

  /// Starts a thread which runs the sender functionality
  void startThread();

  /// Get the perf stats of the transfer for this thread
  PerfStatReport getPerfReport() const;

  /// Conclude the thread transfer
  TransferStats finish();

  /// Get the transfer stats recorded by this thread
  const TransferStats &getTransferStats() const;

  /// Returns the neogtiated protocol
  int getNegotiatedProtocol() const;

  /// Destructor of the sender thread
  ~SenderThread();

 private:
  /// The main entry point of the thread
  void start();

  /// Reset the sender thread
  void reset();

  /// Get the local transfer history
  ThreadTransferHistory &getTransferHistory() {
    return transferHistoryController_->getTransferHistory(port_);
  }

  /**
   * tries to connect to the receiver
   * Previous states : Almost all states(in case of network errors, all states
   *                   move to this state)
   * Next states : SEND_SETTINGS(if there is no previous error)
   *               READ_LOCAL_CHECKPOINT(if there is previous error)
   *               END(failed)
   */
  SenderState connect();
  /**
   * tries to read local checkpoint and return unacked sources to queue. If the
   * checkpoint value is -1, then we know previous attempt to send DONE had
   * failed. So, we move to READ_RECEIVER_CMD state.
   * Previous states : CONNECT
   * Next states : CONNECT(read failure),
   *               END(protocol error or global checkpoint found),
   *               READ_RECEIVER_CMD(if checkpoint is -1),
   *               SEND_SETTINGS(success)
   */
  SenderState readLocalCheckPoint();
  /**
   * sends sender settings to the receiver
   * Previous states : READ_LOCAL_CHECKPOINT,
   *                   CONNECT
   * Next states : SEND_BLOCKS(success),
   *               CONNECT(failure)
   */
  SenderState sendSettings();
  /**
   * sends blocks to receiver till the queue is not empty. After transferring a
   * block, we add it to the history. While adding to history, if it is found
   * that global checkpoint has been received for this thread, we move to END
   * state.
   * Previous states : SEND_SETTINGS,
   *                   PROCESS_ERR_CMD
   * Next states : SEND_BLOCKS(success),
   *               END(global checkpoint received),
   *               CHECK_FOR_ABORT(socket write failure),
   *               SEND_DONE_CMD(no more blocks left to transfer)
   */
  SenderState sendBlocks();
  /**
   * sends DONE cmd to the receiver
   * Previous states : SEND_BLOCKS
   * Next states : CONNECT(failure),
   *               READ_RECEIVER_CMD(success)
   */
  SenderState sendDoneCmd();
  /**
   * sends size cmd to the receiver
   * Previous states : SEND_BLOCKS
   * Next states : CHECK_FOR_ABORT(failure),
   *               SEND_BLOCKS(success)
   */
  SenderState sendSizeCmd();
  /**
   * checks to see if the receiver has sent ABORT or not
   * Previous states : SEND_BLOCKS,
   *                   SEND_DONE_CMD
   * Next states : CONNECT(no ABORT cmd),
   *               END(protocol error),
   *               PROCESS_ABORT_CMD(read ABORT cmd)
   */
  SenderState checkForAbort();
  /**
   * reads previously transferred file chunks list. If it receives an ACK cmd,
   * then it moves on. If wait cmd is received, it waits. Otherwise reads the
   * file chunks and when done starts directory queue thread.
   * Previous states : SEND_SETTINGS,
   * Next states: READ_FILE_CHUNKS(if wait cmd is received),
   *              CHECK_FOR_ABORT(network error),
   *              END(protocol error),
   *              SEND_BLOCKS(success)
   *
   */
  SenderState readFileChunks();
  /**
   * reads receiver cmd
   * Previous states : SEND_DONE_CMD
   * Next states : PROCESS_DONE_CMD,
   *               PROCESS_WAIT_CMD,
   *               PROCESS_ERR_CMD,
   *               END(protocol error),
   *               CONNECT(failure)
   */
  SenderState readReceiverCmd();
  /**
   * handles DONE cmd
   * Previous states : READ_RECEIVER_CMD
   * Next states : END
   */
  SenderState processDoneCmd();
  /**
   * handles WAIT cmd
   * Previous states : READ_RECEIVER_CMD
   * Next states : READ_RECEIVER_CMD
   */
  SenderState processWaitCmd();
  /**
   * reads list of global checkpoints and returns unacked sources to queue.
   * Previous states : READ_RECEIVER_CMD
   * Next states : CONNECT(socket read failure)
   *               END(checkpoint list decode failure),
   *               SEND_BLOCKS(success)
   */
  SenderState processErrCmd();
  /**
   * processes ABORT cmd
   * Previous states : CHECK_FOR_ABORT,
   *                   READ_RECEIVER_CMD
   * Next states : END
   */
  SenderState processAbortCmd();

  /**
   * waits for all active threads to be aborted, checks to see if the abort was
   * due to version mismatch. Also performs various sanity checks.
   * Previous states : Almost all threads, abort flags is checked between every
   *                   state transition
   * Next states : CONNECT(Abort was due to version kismatch),
   *               END(if abort was not due to version mismatch or some sanity
   *               check failed)
   */
  SenderState processVersionMismatch();

  /// mapping from sender states to state functions
  static const StateFunction stateMap_[];

  /// Pointer to the std::thread executing the transfer
  std::unique_ptr<std::thread> threadPtr_;

  /// Index of this thread with respect to other sender threads
  const int threadIndex_;

  /// Port number of this sender thread
  const int32_t port_;

  /// Negotiated protocol of the sender thread
  int negotiatedProtocol_{-1};

  /// Transfer stats for this thread
  TransferStats threadStats_;

  /// Pointer to client socket to maintain connection to the receiver
  std::unique_ptr<ClientSocket> socket_;

  /// Buffer used by the sender thread to read/write data
  char buf_[Protocol::kMinBufLength];

  /// whether total file size has been sent to the receiver
  bool totalSizeSent_{false};

  /// Ref to the directory queue of parent sender
  std::unique_ptr<DirectorySourceQueue> &dirQueue_;

  /// Perf stats report for this thread
  PerfStatReport perfReport_;

  /// Gets the pointer to the parent sender
  Sender *getMetaData() const;

  /// Thread history controller shared across all threads
  std::shared_ptr<TransferHistoryController> transferHistoryController_;

  /// Thread controller for all the sender threads
  std::shared_ptr<ThreadsController> controller_;
};
}
}
