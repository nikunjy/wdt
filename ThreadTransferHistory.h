#include "DirectorySourceQueue.h"
#include "Reporting.h"
#include "Protocol.h"
#include <vector>
#include <folly/SpinLock.h>
namespace facebook {
namespace wdt {
/// transfer history of a sender thread
class ThreadTransferHistory {
 public:
  /**
   * @param queue        directory queue
   * @param threadStats  stat object of the thread
   */
  ThreadTransferHistory(DirectorySourceQueue &queue,
                        TransferStats &threadStats);

  /**
   * @param             index of the source
   * @return            if index is in bounds, returns the identifier for the
   *                    source, else returns empty string
   */
  std::string getSourceId(int64_t index);

  /**
   * Adds the source to the history. If global checkpoint has already been
   * received, then the source is returned to the queue.
   *
   * @param source      source to be added to history
   * @return            true if added to history, false if not added due to a
   *                    global checkpoint
   */
  bool addSource(std::unique_ptr<ByteSource> &source);

  /**
   * Sets checkpoint. Also, returns unacked sources to queue
   *
   * @param numReceivedSources     number of sources acked by the receiver
   * @param lastBlockReceivedBytes number of bytes received for the last block
   * @param globalCheckpoint       global or local checkpoint
   * @return                       number of sources returned to queue, -1 in
   *                               case of error
   */
  int64_t setCheckpointAndReturnToQueue(int64_t numReceivedSources,
                                        int64_t lastBlockReceivedBytes,
                                        bool globalCheckpoint);

  /**
   * @return            stats for acked sources, must be called after all the
   *                    unacked sources are returned to the queue
   */
  std::vector<TransferStats> popAckedSourceStats();

  /// marks all the sources as acked
  void markAllAcknowledged();

  /**
   * returns all unacked sources to the queue
   * @return            number of sources returned to queue, -1 in case of error
   */
  int64_t returnUnackedSourcesToQueue();

  /**
   * @return    number of sources acked by the receiver
   */
  int64_t getNumAcked() const {
    return numAcknowledged_;
  }

 private:
  void markSourceAsFailed(std::unique_ptr<ByteSource> &source,
                          int64_t receivedBytes);

  /// reference to global queue
  DirectorySourceQueue &queue_;
  /// reference to thread stats
  TransferStats &threadStats_;
  /// history of the thread
  std::vector<std::unique_ptr<ByteSource>> history_;
  /// whether a global error checkpoint has been received or not
  bool globalCheckpoint_{false};
  /// number of sources acked by the receiver thread
  int64_t numAcknowledged_{0};
  /// number of bytes received for the last block
  int64_t lastBlockReceivedBytes_{0};
  folly::SpinLock lock_;
};

/// Controller for history across the sender threads
class TransferHistoryController {
 public:
  TransferHistoryController(DirectorySourceQueue &dirQueue);
  void addThreadHistory(int32_t port, TransferStats &threadStats);
  ThreadTransferHistory &getTransferHistory(int32_t port);
  void handleCheckpoint(const Checkpoint &checkpoint);

 private:
  DirectorySourceQueue &dirQueue_;
  std::unordered_map<int32_t, ThreadTransferHistory> threadHistoriesMap_;
};
}
}