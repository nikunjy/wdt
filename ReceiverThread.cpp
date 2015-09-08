/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "ReceiverThread.h"
#include "FileWriter.h"
#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/String.h>
#include <folly/ScopeGuard.h>
#include <folly/Bits.h>
#include <folly/Checksum.h>

namespace facebook {
namespace wdt {

const static int kTimeoutBufferMillis = 1000;
const static int kWaitTimeoutFactor = 5;

std::ostream &operator<<(std::ostream &os,
                         const ReceiverThread &receiverThread) {
  os << "Thread[" << receiverThread.threadIndex_
     << ", port: " << receiverThread.socket_.getPort() << "] ";
  return os;
}

int64_t readAtLeast(ServerSocket &s, char *buf, int64_t max, int64_t atLeast,
                    int64_t len) {
  VLOG(4) << "readAtLeast len " << len << " max " << max << " atLeast "
          << atLeast << " from " << s.getFd();
  CHECK(len >= 0) << "negative len " << len;
  CHECK(atLeast >= 0) << "negative atLeast " << atLeast;
  int count = 0;
  while (len < atLeast) {
    // because we want to process data as soon as it arrives, tryFull option for
    // read is false
    int64_t n = s.read(buf + len, max - len, false);
    if (n < 0) {
      PLOG(ERROR) << "Read error on " << s.getPort() << " after " << count;
      if (len) {
        return len;
      } else {
        return n;
      }
    }
    if (n == 0) {
      VLOG(2) << "Eof on " << s.getPort() << " after " << count << " reads "
              << "got " << len;
      return len;
    }
    len += n;
    count++;
  }
  VLOG(3) << "Took " << count << " reads to get " << len
          << " from fd : " << s.getFd();
  return len;
}

int64_t readAtMost(ServerSocket &s, char *buf, int64_t max, int64_t atMost) {
  const int64_t target = atMost < max ? atMost : max;
  VLOG(3) << "readAtMost target " << target;
  // because we want to process data as soon as it arrives, tryFull option for
  // read is false
  int64_t n = s.read(buf, target, false);
  if (n < 0) {
    PLOG(ERROR) << "Read error on " << s.getPort() << " with target " << target;
    return n;
  }
  if (n == 0) {
    LOG(WARNING) << "Eof on " << s.getFd();
    return n;
  }
  VLOG(3) << "readAtMost " << n << " / " << atMost << " from " << s.getFd();
  return n;
}

const ReceiverThread::StateFunction ReceiverThread::stateMap_[] = {
    &ReceiverThread::listen,
    &ReceiverThread::acceptFirstConnection,
    &ReceiverThread::acceptWithTimeout,
    &ReceiverThread::sendLocalCheckpoint,
    &ReceiverThread::readNextCmd,
    &ReceiverThread::processFileCmd,
    &ReceiverThread::processExitCmd,
    &ReceiverThread::processSettingsCmd,
    &ReceiverThread::processDoneCmd,
    &ReceiverThread::processSizeCmd,
    &ReceiverThread::sendFileChunks,
    &ReceiverThread::sendGlobalCheckpoint,
    &ReceiverThread::sendDoneCmd,
    &ReceiverThread::sendAbortCmd,
    &ReceiverThread::waitForFinishOrNewCheckpoint,
    &ReceiverThread::waitForFinishWithThreadError};

ReceiverThread::ReceiverThread(ServerSocket &socket, int threadIndex,
                               int protocolVersion, int64_t bufferSize,
                               std::shared_ptr<ThreadsController> controller)
    : socket_(std::move(socket)),
      threadIndex_(threadIndex),
      threadProtocolVersion_(protocolVersion),
      bufferSize_(bufferSize),
      controller_(controller) {
  buf_ = new char[bufferSize_];
  controller_->registerThread(threadIndex_);
}

const TransferStats &ReceiverThread::getTransferStats() const {
  return threadStats_;
}

PerfStatReport ReceiverThread::getPerfReport() {
  return perfStatReport_;
}

Receiver *ReceiverThread::getMetaData() const {
  return (Receiver *)controller_->getMetaData();
}

/***LISTEN STATE***/
ReceiverState ReceiverThread::listen() {
  VLOG(1) << *this << " entered LISTEN state ";
  const auto &options = WdtOptions::get();
  const bool doActualWrites = !options.skip_writes;
  int32_t port = socket_.getPort();
  Receiver *metaData = getMetaData();
  VLOG(1) << "Server Thread for port " << port << " with backlog "
          << socket_.getBackLog() << " on " << metaData->getDir()
          << " writes = " << doActualWrites;

  for (int retry = 1; retry < options.max_retries; ++retry) {
    ErrorCode code = socket_.listen();
    if (code == OK) {
      break;
    } else if (code == CONN_ERROR) {
      threadStats_.setErrorCode(code);
      return FAILED;
    }
    LOG(INFO) << "Sleeping after failed attempt " << retry;
    /* sleep override */
    usleep(options.sleep_millis * 1000);
  }
  // one more/last try (stays true if it worked above)
  if (socket_.listen() != OK) {
    LOG(ERROR) << "Unable to listen/bind despite retries";
    threadStats_.setErrorCode(CONN_ERROR);
    return FAILED;
  }
  return ACCEPT_FIRST_CONNECTION;
}

/***ACCEPT_FIRST_CONNECTION***/
ReceiverState ReceiverThread::acceptFirstConnection() {
  VLOG(1) << *this << " entered ACCEPT_FIRST_CONNECTION state ";
  const auto &options = WdtOptions::get();
  reset();
  socket_.closeCurrentConnection();
  auto timeout = options.accept_timeout_millis;
  int acceptAttempts = 0;
  auto metaData = getMetaData();
  while (true) {
    if (acceptAttempts == options.max_accept_retries) {
      LOG(ERROR) << "unable to accept after " << acceptAttempts << " attempts";
      threadStats_.setErrorCode(CONN_ERROR);
      return FAILED;
    }
    if (metaData->getCurAbortCode() != OK) {
      LOG(ERROR) << "Thread marked to abort while trying to accept first"
                 << " connection. Num attempts " << acceptAttempts;
      // Even though there is a transition FAILED here
      // getCurAbortCode() is going to be checked again in the receiveOne.
      // So this is pretty much irrelavant
      return FAILED;
    }

    ErrorCode code = socket_.acceptNextConnection(timeout);
    if (code == OK) {
      break;
    }
    acceptAttempts++;
  }
  auto executeOnce = [&]() {
    metaData->startNewGlobalSession(socket_.getPeerIp());
  };
  controller_->executeAtStart<decltype(executeOnce)>(executeOnce);
  return READ_NEXT_CMD;
}

/***ACCEPT_WITH_TIMEOUT STATE***/
ReceiverState ReceiverThread::acceptWithTimeout() {
  LOG(INFO) << *this << " entered ACCEPT_WITH_TIMEOUT state ";
  const auto &options = WdtOptions::get();
  socket_.closeCurrentConnection();
  auto timeout = options.accept_window_millis;
  if (senderReadTimeout_ > 0) {
    // transfer is in progress and we have alreay got sender settings
    timeout = std::max(senderReadTimeout_, senderWriteTimeout_) +
              kTimeoutBufferMillis;
  }

  ErrorCode code = socket_.acceptNextConnection(timeout);
  if (code != OK) {
    LOG(ERROR) << "accept() failed with timeout " << timeout;
    threadStats_.setErrorCode(code);
    if (doneSendFailure_) {
      // if SEND_DONE_CMD state had already been reached, we do not need to
      // wait for other threads to end
      return END;
    }
    return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
  }

  if (doneSendFailure_) {
    // no need to reset any session variables in this case
    return SEND_LOCAL_CHECKPOINT;
  }

  numRead_ = off_ = 0;
  pendingCheckpointIndex_ = checkpointIndex_;
  ReceiverState nextState = READ_NEXT_CMD;
  if (threadStats_.getErrorCode() != OK) {
    nextState = SEND_LOCAL_CHECKPOINT;
  }
  // reset thread status
  threadStats_.setErrorCode(OK);
  return nextState;
}

/***SEND_LOCAL_CHECKPOINT STATE***/
ReceiverState ReceiverThread::sendLocalCheckpoint() {
  LOG(INFO) << *this << " entered SEND_LOCAL_CHECKPOINT state ";
  // in case SEND_DONE failed, a special checkpoint(-1) is sent to signal this
  // condition
  auto checkpoint = doneSendFailure_ ? -1 : threadStats_.getNumBlocks();
  std::vector<Checkpoint> checkpoints;
  checkpoints.emplace_back(socket_.getPort(), checkpoint,
                           curBlockWrittenBytes_);
  int64_t off_ = 0;
  Protocol::encodeCheckpoints(threadProtocolVersion_, buf_, off_,
                              Protocol::kMaxLocalCheckpoint, checkpoints);
  auto written = socket_.write(buf_, Protocol::kMaxLocalCheckpoint);
  if (written != Protocol::kMaxLocalCheckpoint) {
    LOG(ERROR) << "unable to write local checkpoint. write mismatch "
               << Protocol::kMaxLocalCheckpoint << " " << written;
    threadStats_.setErrorCode(SOCKET_WRITE_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  }
  threadStats_.addHeaderBytes(Protocol::kMaxLocalCheckpoint);
  if (doneSendFailure_) {
    return SEND_DONE_CMD;
  }
  return READ_NEXT_CMD;
}

/***READ_NEXT_CMD***/
ReceiverState ReceiverThread::readNextCmd() {
  VLOG(1) << *this << " entered READ_NEXT_CMD state ";
  oldOffset_ = off_;
  numRead_ = readAtLeast(socket_, buf_ + off_, bufferSize_ - off_,
                         Protocol::kMinBufLength, numRead_);
  if (numRead_ <= 0) {
    LOG(ERROR) << "socket read failure " << Protocol::kMinBufLength << " "
               << numRead_;
    threadStats_.setErrorCode(SOCKET_READ_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  }
  Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf_[off_++];
  if (cmd == Protocol::EXIT_CMD) {
    return PROCESS_EXIT_CMD;
  }
  if (cmd == Protocol::DONE_CMD) {
    return PROCESS_DONE_CMD;
  }
  if (cmd == Protocol::FILE_CMD) {
    return PROCESS_FILE_CMD;
  }
  if (cmd == Protocol::SETTINGS_CMD) {
    return PROCESS_SETTINGS_CMD;
  }
  if (cmd == Protocol::SIZE_CMD) {
    return PROCESS_SIZE_CMD;
  }
  LOG(ERROR) << "received an unknown cmd";
  threadStats_.setErrorCode(PROTOCOL_ERROR);
  return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
}

/***PROCESS_EXIT_CMD STATE***/
ReceiverState ReceiverThread::processExitCmd() {
  LOG(INFO) << *this << " entered PROCESS_EXIT_CMD state ";
  if (numRead_ != 1) {
    LOG(ERROR) << "Unexpected state for exit command. probably junk "
                  "content. ignoring...";
    threadStats_.setErrorCode(PROTOCOL_ERROR);
    return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
  }
  LOG(ERROR) << "Got exit command in port " << socket_.getPort()
             << " - exiting";
  exit(0);
}

/***PROCESS_SETTINGS_CMD***/
ReceiverState ReceiverThread::processSettingsCmd() {
  VLOG(1) << *this << " entered PROCESS_SETTINGS_CMD state ";
  Settings settings;
  int senderProtocolVersion;

  bool success = Protocol::decodeVersion(
      buf_, off_, oldOffset_ + Protocol::kMaxVersion, senderProtocolVersion);
  if (!success) {
    LOG(ERROR) << "Unable to decode version " << threadIndex_;
    threadStats_.setErrorCode(PROTOCOL_ERROR);
    return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
  }
  if (senderProtocolVersion != threadProtocolVersion_) {
    LOG(ERROR) << "Receiver and sender protocol version mismatch "
               << senderProtocolVersion << " " << threadProtocolVersion_;
    int negotiatedProtocol = Protocol::negotiateProtocol(
        senderProtocolVersion, threadProtocolVersion_);
    if (negotiatedProtocol == 0) {
      LOG(WARNING) << "Can not support sender with version "
                   << senderProtocolVersion << ", aborting!";
      threadStats_.setErrorCode(VERSION_INCOMPATIBLE);
      return SEND_ABORT_CMD;
    } else {
      LOG_IF(INFO, threadProtocolVersion_ != negotiatedProtocol)
          << "Changing receiver protocol version to " << negotiatedProtocol;
      threadProtocolVersion_ = negotiatedProtocol;
      if (negotiatedProtocol != senderProtocolVersion) {
        threadStats_.setErrorCode(VERSION_MISMATCH);
        return SEND_ABORT_CMD;
      }
    }
  }

  success = Protocol::decodeSettings(
      threadProtocolVersion_, buf_, off_,
      oldOffset_ + Protocol::kMaxVersion + Protocol::kMaxSettings, settings);
  if (!success) {
    LOG(ERROR) << "Unable to decode settings cmd " << threadIndex_;
    threadStats_.setErrorCode(PROTOCOL_ERROR);
    return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
  }
  auto senderId = settings.transferId;
  Receiver *metaData = getMetaData();
  auto transferId = metaData->getTransferId();
  if (transferId != senderId) {
    LOG(ERROR) << "Receiver and sender id mismatch " << senderId << " "
               << transferId;
    threadStats_.setErrorCode(ID_MISMATCH);
    return SEND_ABORT_CMD;
  }
  senderReadTimeout_ = settings.readTimeoutMillis;
  senderWriteTimeout_ = settings.writeTimeoutMillis;
  enableChecksum_ = settings.enableChecksum;
  if (settings.sendFileChunks) {
    // We only move to SEND_FILE_CHUNKS state, if download resumption is enabled
    // in the sender side
    numRead_ = off_ = 0;
    return SEND_FILE_CHUNKS;
  }
  auto msgLen = off_ - oldOffset_;
  numRead_ -= msgLen;
  return READ_NEXT_CMD;
}

/***PROCESS_FILE_CMD***/
ReceiverState ReceiverThread::processFileCmd() {
  VLOG(1) << *this << " entered PROCESS_FILE_CMD state ";
  const auto &options = WdtOptions::get();
  curBlockWrittenBytes_ = 0;
  BlockDetails blockDetails;
  auto &socket = socket_;
  auto &threadStats = threadStats_;
  auto guard = folly::makeGuard([&] {
    if (threadStats.getErrorCode() != OK) {
      threadStats.incrFailedAttempts();
    }
  });

  ErrorCode transferStatus = (ErrorCode)buf_[off_++];
  if (transferStatus != OK) {
    // TODO: use this status information to implement fail fast mode
    VLOG(1) << "sender entered into error state "
            << errorCodeToStr(transferStatus);
  }
  int16_t headerLen = folly::loadUnaligned<int16_t>(buf_ + off_);
  headerLen = folly::Endian::little(headerLen);
  VLOG(2) << "Processing FILE_CMD, header len " << headerLen;

  if (headerLen > numRead_) {
    int64_t end = oldOffset_ + numRead_;
    numRead_ = readAtLeast(socket_, buf_ + end, bufferSize_ - end, headerLen,
                           numRead_);
  }
  if (numRead_ < headerLen) {
    LOG(ERROR) << "Unable to read full header " << headerLen << " " << numRead_;
    threadStats_.setErrorCode(SOCKET_READ_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  }
  off_ += sizeof(int16_t);
  bool success = Protocol::decodeHeader(threadProtocolVersion_, buf_, off_,
                                        numRead_ + oldOffset_, blockDetails);
  int64_t headerBytes = off_ - oldOffset_;
  // transferred header length must match decoded header length
  WDT_CHECK(headerLen == headerBytes);
  threadStats_.addHeaderBytes(headerBytes);
  if (!success) {
    LOG(ERROR) << "Error decoding at"
               << " ooff:" << oldOffset_ << " off_: " << off_
               << " numRead_: " << numRead_;
    threadStats_.setErrorCode(PROTOCOL_ERROR);
    return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
  }

  // received a well formed file cmd, apply the pending checkpoint update
  checkpointIndex_ = pendingCheckpointIndex_;
  VLOG(1) << "Read id:" << blockDetails.fileName
          << " size:" << blockDetails.dataSize << " ooff:" << oldOffset_
          << " off_: " << off_ << " numRead_: " << numRead_;
  Receiver *metaData = getMetaData();
  auto &fileCreator = metaData->getFileCreator();
  FileWriter writer(threadIndex_, &blockDetails, fileCreator.get());
  auto writtenGuard = folly::makeGuard([&] {
    if (threadProtocolVersion_ >= Protocol::CHECKPOINT_OFFSET_VERSION) {
      // considering partially written block contents as valid, this bypasses
      // checksum verification
      // TODO: Make sure checksum verification work with checkpoint offsets
      curBlockWrittenBytes_ = writer.getTotalWritten();
      threadStats_.addEffectiveBytes(headerBytes, curBlockWrittenBytes_);
    }
  });

  if (writer.open() != OK) {
    threadStats_.setErrorCode(FILE_WRITE_ERROR);
    return SEND_ABORT_CMD;
  }
  int32_t checksum = 0;
  int64_t remainingData = numRead_ + oldOffset_ - off_;
  int64_t toWrite = remainingData;
  WDT_CHECK(remainingData >= 0);
  if (remainingData >= blockDetails.dataSize) {
    toWrite = blockDetails.dataSize;
  }
  threadStats_.addDataBytes(toWrite);
  if (enableChecksum_) {
    checksum = folly::crc32c((const uint8_t *)(buf_ + off_), toWrite, checksum);
  }
  auto throttler = metaData->getThrottler();
  if (throttler) {
    // We might be reading more than we require for this file but
    // throttling should make sense for any additional bytes received
    // on the network
    throttler->limit(toWrite + headerBytes);
  }
  ErrorCode code = writer.write(buf_ + off_, toWrite);
  if (code != OK) {
    threadStats_.setErrorCode(code);
    return SEND_ABORT_CMD;
  }
  off_ += toWrite;
  remainingData -= toWrite;
  // also means no leftOver so it's ok we use buf_ from start
  while (writer.getTotalWritten() < blockDetails.dataSize) {
    if (metaData->getCurAbortCode() != OK) {
      LOG(ERROR) << "Thread marked for abort while processing a file."
                 << " port : " << socket_.getPort();
      return FAILED;
    }
    int64_t nres = readAtMost(socket_, buf_, bufferSize_,
                              blockDetails.dataSize - writer.getTotalWritten());
    if (nres <= 0) {
      break;
    }
    if (throttler) {
      // We only know how much we have read after we are done calling
      // readAtMost. Call throttler with the bytes read off_ the wire.
      throttler->limit(nres);
    }
    threadStats_.addDataBytes(nres);
    if (enableChecksum_) {
      checksum = folly::crc32c((const uint8_t *)buf_, nres, checksum);
    }
    code = writer.write(buf_, nres);
    if (code != OK) {
      threadStats_.setErrorCode(code);
      return SEND_ABORT_CMD;
    }
  }
  if (writer.getTotalWritten() != blockDetails.dataSize) {
    // This can only happen if there are transmission errors
    // Write errors to disk are already taken care of above
    LOG(ERROR) << "could not read entire content for " << blockDetails.fileName
               << " port " << socket_.getPort();
    threadStats_.setErrorCode(SOCKET_READ_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  }
  writtenGuard.dismiss();
  VLOG(2) << "completed " << blockDetails.fileName << " off_: " << off_
          << " numRead_: " << numRead_;
  // Transfer of the file is complete here, mark the bytes effective
  WDT_CHECK(remainingData >= 0) << "Negative remainingData " << remainingData;
  if (remainingData > 0) {
    // if we need to read more anyway, let's move the data
    numRead_ = remainingData;
    if ((remainingData < Protocol::kMaxHeader) && (off_ > (bufferSize_ / 2))) {
      // rare so inneficient is ok
      VLOG(3) << "copying extra " << remainingData << " leftover bytes @ "
              << off_;
      memmove(/* dst      */ buf_,
              /* from     */ buf_ + off_,
              /* how much */ remainingData);
      off_ = 0;
    } else {
      // otherwise just continue from the offset
      VLOG(3) << "Using remaining extra " << remainingData
              << " leftover bytes starting @ " << off_;
    }
  } else {
    numRead_ = off_ = 0;
  }
  if (enableChecksum_) {
    // have to read footer cmd
    oldOffset_ = off_;
    numRead_ = readAtLeast(socket_, buf_ + off_, bufferSize_ - off_,
                           Protocol::kMinBufLength, numRead_);
    if (numRead_ < Protocol::kMinBufLength) {
      LOG(ERROR) << "socket read failure " << Protocol::kMinBufLength << " "
                 << numRead_;
      threadStats_.setErrorCode(SOCKET_READ_ERROR);
      return ACCEPT_WITH_TIMEOUT;
    }
    Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf_[off_++];
    if (cmd != Protocol::FOOTER_CMD) {
      LOG(ERROR) << "Expecting footer cmd, but received " << cmd;
      threadStats_.setErrorCode(PROTOCOL_ERROR);
      return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
    }
    int32_t receivedChecksum;
    bool success = Protocol::decodeFooter(
        buf_, off_, oldOffset_ + Protocol::kMaxFooter, receivedChecksum);
    if (!success) {
      LOG(ERROR) << "Unable to decode footer cmd";
      threadStats_.setErrorCode(PROTOCOL_ERROR);
      return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
    }
    if (checksum != receivedChecksum) {
      LOG(ERROR) << "Checksum mismatch " << checksum << " " << receivedChecksum
                 << " port " << socket_.getPort() << " file "
                 << blockDetails.fileName;
      threadStats_.setErrorCode(CHECKSUM_MISMATCH);
      return ACCEPT_WITH_TIMEOUT;
    }
    int64_t msgLen = off_ - oldOffset_;
    numRead_ -= msgLen;
  }
  auto &transferLogManager = metaData->getTransferLogManager();
  if (options.enable_download_resumption) {
    transferLogManager.addBlockWriteEntry(
        blockDetails.seqId, blockDetails.offset, blockDetails.dataSize);
  }
  threadStats_.addEffectiveBytes(headerBytes, blockDetails.dataSize);
  threadStats_.incrNumBlocks();
  return READ_NEXT_CMD;
}

ReceiverState ReceiverThread::processDoneCmd() {
  VLOG(1) << *this << " entered PROCESS_DONE_CMD state ";
  if (numRead_ != Protocol::kMinBufLength) {
    LOG(ERROR) << "Unexpected state for done command"
               << " off_: " << off_ << " numRead_: " << numRead_;
    threadStats_.setErrorCode(PROTOCOL_ERROR);
    return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
  }

  ErrorCode senderStatus = (ErrorCode)buf_[off_++];
  int64_t numBlocksSend;
  int64_t totalSenderBytes;
  bool success = Protocol::decodeDone(threadProtocolVersion_, buf_, off_,
                                      oldOffset_ + Protocol::kMaxDone,
                                      numBlocksSend, totalSenderBytes);
  if (!success) {
    LOG(ERROR) << "Unable to decode done cmd";
    threadStats_.setErrorCode(PROTOCOL_ERROR);
    return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
  }
  Receiver *metaData = getMetaData();
  metaData->setNumBlocksSend(numBlocksSend);
  metaData->setTotalSenderBytes(totalSenderBytes);
  threadStats_.setRemoteErrorCode(senderStatus);

  // received a valid command, applying pending checkpoint write update
  checkpointIndex_ = pendingCheckpointIndex_;
  return WAIT_FOR_FINISH_OR_NEW_CHECKPOINT;
}

ReceiverState ReceiverThread::processSizeCmd() {
  VLOG(1) << *this << " entered PROCESS_SIZE_CMD state ";
  int64_t totalSenderBytes;
  bool success = Protocol::decodeSize(
      buf_, off_, oldOffset_ + Protocol::kMaxSize, totalSenderBytes);
  if (!success) {
    LOG(ERROR) << "Unable to decode size cmd";
    threadStats_.setErrorCode(PROTOCOL_ERROR);
    return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
  }
  VLOG(1) << "Number of bytes to receive " << totalSenderBytes;
  Receiver *metaData = getMetaData();
  metaData->setTotalSenderBytes(totalSenderBytes);
  auto msgLen = off_ - oldOffset_;
  numRead_ -= msgLen;
  return READ_NEXT_CMD;
}

ReceiverState ReceiverThread::sendFileChunks() {
  LOG(INFO) << *this << " entered SEND_FILE_CHUNKS state ";
  ReceiverState state = SEND_FILE_CHUNKS;
  auto metaData = getMetaData();
  int toWrite;
  int written;
  WDT_CHECK(senderReadTimeout_ > 0);  // must have received settings
  int waitingTimeMillis = senderReadTimeout_ / kWaitTimeoutFactor;
  auto endFn = [&]() -> bool {
    buf_[0] = Protocol::ACK_CMD;
    toWrite = 1;
    written = socket_.write(buf_, toWrite);
    if (written != toWrite) {
      LOG(ERROR) << "Socket write error " << toWrite << " " << written;
      threadStats_.setErrorCode(SOCKET_READ_ERROR);
      state = ACCEPT_WITH_TIMEOUT;
      return false;
    }
    threadStats_.addHeaderBytes(toWrite);
    state = READ_NEXT_CMD;
    return true;
  };

  auto progressFn = [&]() -> bool {
    buf_[0] = Protocol::WAIT_CMD;
    toWrite = 1;
    written = socket_.write(buf_, toWrite);
    if (written != toWrite) {
      LOG(ERROR) << "Socket write error " << toWrite << " " << written;
      threadStats_.setErrorCode(SOCKET_READ_ERROR);
      state = ACCEPT_WITH_TIMEOUT;
      return false;
    }
    threadStats_.addHeaderBytes(toWrite);
    // self loop
    state = SEND_FILE_CHUNKS;
    return true;
  };

  auto startFn = [&]() -> bool {
    auto &transferLogManager = metaData->getTransferLogManager();
    const auto &parsedFileChunksInfo =
        transferLogManager.getParsedFileChunksInfo();
    int64_t off = 0;
    buf_[off++] = Protocol::CHUNKS_CMD;
    const int64_t numParsedChunksInfo = parsedFileChunksInfo.size();
    Protocol::encodeChunksCmd(buf_, off, bufferSize_, numParsedChunksInfo);
    written = socket_.write(buf_, off);
    if (written > 0) {
      threadStats_.addHeaderBytes(written);
    }
    if (written != off) {
      LOG(ERROR) << "Socket write error " << off << " " << written;
      threadStats_.setErrorCode(SOCKET_READ_ERROR);
      state = ACCEPT_WITH_TIMEOUT;
      return false;
    }
    int64_t numEntriesWritten = 0;
    // we try to encode as many chunks as possible in the buffer. If a
    // single
    // chunk can not fit in the buffer, it is ignored. Format of encoding :
    // <data-size><chunk1><chunk2>...
    while (numEntriesWritten < numParsedChunksInfo) {
      off = sizeof(int32_t);
      int64_t numEntriesEncoded = Protocol::encodeFileChunksInfoList(
          buf_, off, bufferSize_, numEntriesWritten, parsedFileChunksInfo);
      int32_t dataSize = folly::Endian::little(off - sizeof(int32_t));
      folly::storeUnaligned<int32_t>(buf_, dataSize);
      written = socket_.write(buf_, off);
      if (written > 0) {
        threadStats_.addHeaderBytes(written);
      }
      if (written != off) {
        break;
      }
      numEntriesWritten += numEntriesEncoded;
    }
    if (numEntriesWritten != numParsedChunksInfo) {
      LOG(ERROR) << "Could not write all the file chunks "
                 << numParsedChunksInfo << " " << numEntriesWritten;
      threadStats_.setErrorCode(SOCKET_WRITE_ERROR);
      state = ACCEPT_WITH_TIMEOUT;
      return false;
    }
    // try to read ack
    int64_t toRead = 1;
    int64_t numRead_ = socket_.read(buf_, toRead);
    if (numRead_ != toRead) {
      LOG(ERROR) << "Socket read error " << toRead << " " << numRead_;
      threadStats_.setErrorCode(SOCKET_READ_ERROR);
      state = ACCEPT_WITH_TIMEOUT;
      return false;
    }
    // sender is aware of previous transferred chunks. logging can now be
    // enabled
    transferLogManager.enableLogging();
    transferLogManager.addLogHeader();
    state = READ_NEXT_CMD;
    return true;
  };
  const std::string funnelName = "SEND_FILE_CHUNKS";
  auto execFunnel = controller_->getFunnel(funnelName);
  execFunnel->execute<decltype(startFn), decltype(progressFn), decltype(endFn)>(
      startFn, progressFn, endFn, waitingTimeMillis);
  return state;
}

ReceiverState ReceiverThread::sendGlobalCheckpoint() {
  LOG(INFO) << *this << " entered SEND_GLOBAL_CHECKPOINTS state ";
  buf_[0] = Protocol::ERR_CMD;
  off_ = 1;
  // leave space for length
  off_ += sizeof(int16_t);
  auto oldOffset_ = off_;
  Protocol::encodeCheckpoints(threadProtocolVersion_, buf_, off_, bufferSize_,
                              newCheckpoints_);
  int16_t length = off_ - oldOffset_;
  folly::storeUnaligned<int16_t>(buf_ + 1, folly::Endian::little(length));

  auto written = socket_.write(buf_, off_);
  if (written != off_) {
    LOG(ERROR) << "unable to write error checkpoints";
    threadStats_.setErrorCode(SOCKET_WRITE_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  } else {
    threadStats_.addHeaderBytes(off_);
    pendingCheckpointIndex_ = checkpointIndex_ + newCheckpoints_.size();
    numRead_ = off_ = 0;
    return READ_NEXT_CMD;
  }
}

ReceiverState ReceiverThread::sendAbortCmd() {
  LOG(INFO) << *this << " entered SEND_ABORT_CMD state ";
  int64_t offset = 0;
  buf_[offset++] = Protocol::ABORT_CMD;
  Protocol::encodeAbort(buf_, offset, threadProtocolVersion_,
                        threadStats_.getErrorCode(),
                        threadStats_.getNumFiles());
  socket_.write(buf_, offset);
  // No need to check if we were successful in sending ABORT
  // This thread will simply disconnect and sender thread on the
  // other side will timeout
  socket_.closeCurrentConnection();
  threadStats_.addHeaderBytes(offset);
  if (threadStats_.getErrorCode() == VERSION_MISMATCH) {
    // Receiver should try again expecting sender to have changed its version
    return ACCEPT_WITH_TIMEOUT;
  }
  return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
}

ReceiverState ReceiverThread::sendDoneCmd() {
  VLOG(1) << *this << " entered SEND_DONE_CMD state ";
  buf_[0] = Protocol::DONE_CMD;
  if (socket_.write(buf_, 1) != 1) {
    PLOG(ERROR) << "unable to send DONE " << threadIndex_;
    doneSendFailure_ = true;
    return ACCEPT_WITH_TIMEOUT;
  }

  threadStats_.addHeaderBytes(1);

  auto read = socket_.read(buf_, 1);
  if (read != 1 || buf_[0] != Protocol::DONE_CMD) {
    LOG(ERROR) << *this << " did not receive ack for DONE";
    doneSendFailure_ = true;
    return ACCEPT_WITH_TIMEOUT;
  }

  read = socket_.read(buf_, Protocol::kMinBufLength);
  if (read != 0) {
    LOG(ERROR) << *this << " EOF not found where expected";
    doneSendFailure_ = true;
    return ACCEPT_WITH_TIMEOUT;
  }
  socket_.closeCurrentConnection();
  LOG(INFO) << *this << " got ack for DONE. Transfer finished";
  return END;
}

ReceiverState ReceiverThread::waitForFinishWithThreadError() {
  LOG(INFO) << *this << " entered WAIT_FOR_FINISH_WITH_THREAD_ERROR state ";
  // should only be in this state if there is some error
  WDT_CHECK(threadStats_.getErrorCode() != OK);

  // close the socket, so that sender receives an error during connect
  socket_.closeAll();
  // post checkpoint in case of an error
  Checkpoint localCheckpoint(socket_.getPort(), threadStats_.getNumBlocks(),
                             curBlockWrittenBytes_);
  Receiver *receiver = getMetaData();
  receiver->addCheckpoint(localCheckpoint);
  controller_->markState(threadIndex_, FINISHED);
  return END;
}

ReceiverState ReceiverThread::waitForFinishOrNewCheckpoint() {
  LOG(INFO) << *this << " entered WAIT_FOR_FINISH_OR_NEW_CHECKPOINT state ";
  // should only be called if the are no errors
  WDT_CHECK(threadStats_.getErrorCode() == OK);
  Receiver *metaData = getMetaData();

  // we have to check for checkpoints before checking to see if session ended or
  // not. because if some checkpoints have not been sent back to the sender,
  // session should not end
  newCheckpoints_ = metaData->getNewCheckpoints(checkpointIndex_);
  if (!newCheckpoints_.empty()) {
    return SEND_GLOBAL_CHECKPOINTS;
  }
  controller_->markState(threadIndex_, WAITING);
  // we must send periodic wait cmd to keep the sender thread alive
  while (true) {
    WDT_CHECK(senderReadTimeout_ > 0);  // must have received settings
    int timeoutMillis = senderReadTimeout_ / kWaitTimeoutFactor;
    auto waitingTime = std::chrono::milliseconds(timeoutMillis);
    bool hasSessionFinished = false;
    auto checkPoints = metaData->getNewCheckpoints(checkpointIndex_);
    if (!checkPoints.empty()) {
      newCheckpoints_ = std::move(checkPoints);
      return SEND_GLOBAL_CHECKPOINTS;
    }
    bool isFound =
        (controller_->markIfAllThreads(threadIndex_, ~RUNNING, FINISHED));
    if (isFound) {
      return SEND_DONE_CMD;
    }
    // send WAIT cmd to keep sender thread alive
    buf_[0] = Protocol::WAIT_CMD;
    if (socket_.write(buf_, 1) != 1) {
      PLOG(ERROR) << *this << " unable to write WAIT ";
      threadStats_.setErrorCode(SOCKET_WRITE_ERROR);
      controller_->markState(threadIndex_, FAILED);
      return FAILED;
    }
    threadStats_.addHeaderBytes(1);
    std::this_thread::sleep_for(waitingTime);
  }
}

void ReceiverThread::startThread() {
  if (threadPtr_) {
    LOG(WARNING) << "There is a already a thread running. Trying to join the"
                 << "previous thread";
    finish();
  }
  threadPtr_.reset(new std::thread(&ReceiverThread::start, this));
}

void ReceiverThread::start() {
  INIT_PERF_STAT_REPORT
  auto metaData = getMetaData();
  auto guard = folly::makeGuard([&] {
    perfStatReport_ = *perfStatReport;
    LOG(INFO) << *this << threadStats_;
    controller_->deRegisterThread(threadIndex_);
    auto endFunc = [&]() { metaData->endCurGlobalSession(); };
    controller_->executeAtEnd<decltype(endFunc)>(endFunc);
  });
  if (!buf_) {
    LOG(ERROR) << "error allocating " << bufferSize_;
    threadStats_.setErrorCode(MEMORY_ALLOCATION_ERROR);
    return;
  }
  ReceiverState state = LISTEN;
  while (true) {
    ErrorCode abortCode = metaData->getCurAbortCode();
    if (abortCode != OK) {
      LOG(ERROR) << "Transfer aborted " << socket_.getPort() << " "
                 << errorCodeToStr(abortCode);
      threadStats_.setErrorCode(ABORT);
      break;
    }
    if (state == FAILED) {
      return;
    }
    if (state == END) {
      return;
    }
    state = (this->*stateMap_[state])();
  }
}

int32_t ReceiverThread::getPort() const {
  return socket_.getPort();
}

ErrorCode ReceiverThread::init() {
  int max_retries = WdtOptions::get().max_retries;
  for (int retries = 0; retries < max_retries; retries++) {
    if (socket_.listen() == OK) {
      break;
    }
  }
  if (socket_.listen() != OK) {
    LOG(ERROR) << *this << "Couldn't listen on port " << socket_.getPort();
    return ERROR;
  }
  LOG(INFO) << "Listening on port " << socket_.getPort();
  return OK;
}

ErrorCode ReceiverThread::finish() {
  if (!threadPtr_) {
    LOG(ERROR) << "Finish called on an instance while no thread has been "
               << " created to do any work";
    return ERROR;
  }
  threadPtr_->join();
  reset();
  threadPtr_.reset();
  return OK;
}

void ReceiverThread::reset() {
  numRead_ = off_ = 0;
  checkpointIndex_ = pendingCheckpointIndex_ = 0;
  doneSendFailure_ = false;
  senderReadTimeout_ = senderWriteTimeout_ = -1;
  threadStats_.reset();
}

ReceiverThread::~ReceiverThread() {
  if (threadPtr_) {
    LOG(INFO) << *this << " has an alive thread while the instance is being "
              << "destructed";
    finish();
  }
  delete[] buf_;
}
}
}