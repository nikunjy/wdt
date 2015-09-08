#include "SenderThread.h"
#include "ClientSocket.h"
#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/String.h>
#include <folly/Bits.h>
#include <folly/ScopeGuard.h>
#include <sys/stat.h>
#include <folly/Checksum.h>

namespace facebook {
namespace wdt {
const SenderThread::StateFunction SenderThread::stateMap_[] = {
    &SenderThread::connect,         &SenderThread::readLocalCheckPoint,
    &SenderThread::sendSettings,    &SenderThread::sendBlocks,
    &SenderThread::sendDoneCmd,     &SenderThread::sendSizeCmd,
    &SenderThread::checkForAbort,   &SenderThread::readFileChunks,
    &SenderThread::readReceiverCmd, &SenderThread::processDoneCmd,
    &SenderThread::processWaitCmd,  &SenderThread::processErrCmd,
    &SenderThread::processAbortCmd, &SenderThread::processVersionMismatch};

SenderState SenderThread::connect() {
  VLOG(1) << "entered CONNECT state " << threadIndex_;
  auto metaData = getMetaData();
  if (socket_) {
    socket_->close();
  }
  ErrorCode code;
  socket_ = metaData->connectToReceiver(port_, code);
  if (code == ABORT) {
    threadStats_.setErrorCode(ABORT);
    if (metaData->getCurAbortCode() == VERSION_MISMATCH) {
      return PROCESS_VERSION_MISMATCH;
    }
    return END;
  }
  if (code != OK) {
    threadStats_.setErrorCode(code);
    return END;
  }
  auto nextState =
      threadStats_.getErrorCode() == OK ? SEND_SETTINGS : READ_LOCAL_CHECKPOINT;
  // resetting the status of thread
  reset();
  return nextState;
}

SenderState SenderThread::readLocalCheckPoint() {
  LOG(INFO) << "entered READ_LOCAL_CHECKPOINT state " << threadIndex_;
  auto metaData = getMetaData();
  ThreadTransferHistory &transferHistory = getTransferHistory();
  std::vector<Checkpoint> checkpoints;
  int64_t decodeOffset = 0;
  int64_t numRead = socket_->read(buf_, Protocol::kMaxLocalCheckpoint);
  if (numRead != Protocol::kMaxLocalCheckpoint) {
    VLOG(1) << "read mismatch " << Protocol::kMaxLocalCheckpoint << " "
            << numRead << " port " << port_;
    threadStats_.setErrorCode(SOCKET_READ_ERROR);
    return CONNECT;
  }
  auto protocolVersion = metaData->getProtocolVersion();
  if (!Protocol::decodeCheckpoints(protocolVersion, buf_, decodeOffset,
                                   Protocol::kMaxLocalCheckpoint,
                                   checkpoints)) {
    LOG(ERROR) << "checkpoint decode failure "
               << folly::humanify(
                      std::string(buf_, Protocol::kMaxLocalCheckpoint));
    threadStats_.setErrorCode(PROTOCOL_ERROR);
    return END;
  }
  if (checkpoints.size() != 1 || checkpoints[0].port != port_) {
    LOG(ERROR) << "illegal local checkpoint "
               << folly::humanify(
                      std::string(buf_, Protocol::kMaxLocalCheckpoint));
    threadStats_.setErrorCode(PROTOCOL_ERROR);
    return END;
  }
  auto numBlocks = checkpoints[0].numBlocks;
  int64_t lastBlockReceivedBytes = checkpoints[0].lastBlockReceivedBytes;
  VLOG(1) << "received local checkpoint " << port_ << " " << numBlocks << " "
          << lastBlockReceivedBytes;

  if (numBlocks == -1) {
    // Receiver failed while sending DONE cmd
    return READ_RECEIVER_CMD;
  }

  auto numReturned = transferHistory.setCheckpointAndReturnToQueue(
      numBlocks, lastBlockReceivedBytes, false);
  if (numReturned == -1) {
    threadStats_.setErrorCode(PROTOCOL_ERROR);
    return END;
  }
  VLOG(1) << numRead << " number of source(s) returned to queue";
  return SEND_SETTINGS;
}

SenderState SenderThread::sendSettings() {
  VLOG(1) << "entered SEND_SETTINGS state " << threadIndex_;
  auto &options = WdtOptions::get();
  int64_t readTimeoutMillis = options.read_timeout_millis;
  int64_t writeTimeoutMillis = options.write_timeout_millis;
  int64_t off = 0;
  buf_[off++] = Protocol::SETTINGS_CMD;
  auto metaData = getMetaData();
  bool sendFileChunks = metaData->isReadFileChunks();
  Settings settings;
  settings.readTimeoutMillis = readTimeoutMillis;
  settings.writeTimeoutMillis = writeTimeoutMillis;
  settings.transferId = metaData->getTransferId();
  settings.enableChecksum = options.enable_checksum;
  settings.sendFileChunks = sendFileChunks;
  Protocol::encodeSettings(metaData->getProtocolVersion(), buf_, off,
                           Protocol::kMaxSettings, settings);
  int64_t toWrite = sendFileChunks ? Protocol::kMinBufLength : off;
  int64_t written = socket_->write(buf_, toWrite);
  if (written != toWrite) {
    LOG(ERROR) << "Socket write failure " << written << " " << toWrite;
    threadStats_.setErrorCode(SOCKET_WRITE_ERROR);
    return CONNECT;
  }
  threadStats_.addHeaderBytes(toWrite);
  return sendFileChunks ? READ_FILE_CHUNKS : SEND_BLOCKS;
}

SenderState SenderThread::sendBlocks() {
  VLOG(1) << "entered SEND_BLOCKS state " << threadIndex_;
  ThreadTransferHistory &transferHistory = getTransferHistory();
  auto metaData = getMetaData();
  if (metaData->getProtocolVersion() >=
          Protocol::RECEIVER_PROGRESS_REPORT_VERSION &&
      !totalSizeSent_ && dirQueue_->fileDiscoveryFinished()) {
    return SEND_SIZE_CMD;
  }
  ErrorCode transferStatus;
  std::unique_ptr<ByteSource> source = dirQueue_->getNextSource(transferStatus);
  if (!source) {
    return SEND_DONE_CMD;
  }
  WDT_CHECK(!source->hasError());
  TransferStats transferStats =
      metaData->sendOneByteSource(socket_, source, transferStatus);
  threadStats_ += transferStats;
  source->addTransferStats(transferStats);
  source->close();
  if (!transferHistory.addSource(source)) {
    // global checkpoint received for this thread. no point in
    // continuing
    LOG(ERROR) << "global checkpoint received, no point in continuing";
    threadStats_.setErrorCode(CONN_ERROR);
    return END;
  }
  if (transferStats.getErrorCode() != OK) {
    return CHECK_FOR_ABORT;
  }
  return SEND_BLOCKS;
}

SenderState SenderThread::sendSizeCmd() {
  VLOG(1) << "entered SEND_SIZE_CMD state " << threadIndex_;
  int64_t off = 0;
  buf_[off++] = Protocol::SIZE_CMD;

  Protocol::encodeSize(buf_, off, Protocol::kMaxSize,
                       dirQueue_->getTotalSize());
  int64_t written = socket_->write(buf_, off);
  if (written != off) {
    LOG(ERROR) << "Socket write error " << off << " " << written;
    threadStats_.setErrorCode(SOCKET_WRITE_ERROR);
    return CHECK_FOR_ABORT;
  }
  threadStats_.addHeaderBytes(off);
  totalSizeSent_ = true;
  return SEND_BLOCKS;
}

SenderState SenderThread::sendDoneCmd() {
  VLOG(1) << "entered SEND_DONE_CMD state " << threadIndex_;
  int64_t off = 0;
  buf_[off++] = Protocol::DONE_CMD;
  auto pair = dirQueue_->getNumBlocksAndStatus();
  int64_t numBlocksDiscovered = pair.first;
  ErrorCode transferStatus = pair.second;
  buf_[off++] = transferStatus;
  auto metaData = getMetaData();
  Protocol::encodeDone(metaData->getProtocolVersion(), buf_, off,
                       Protocol::kMaxDone, numBlocksDiscovered,
                       dirQueue_->getTotalSize());
  int toWrite = Protocol::kMinBufLength;
  int64_t written = socket_->write(buf_, toWrite);
  if (written != toWrite) {
    LOG(ERROR) << "Socket write failure " << written << " " << toWrite;
    threadStats_.setErrorCode(SOCKET_WRITE_ERROR);
    return CHECK_FOR_ABORT;
  }
  threadStats_.addHeaderBytes(toWrite);
  VLOG(1) << "Wrote done cmd on " << socket_->getFd()
          << " waiting for reply...";
  return READ_RECEIVER_CMD;
}

SenderState SenderThread::checkForAbort() {
  LOG(INFO) << "entered CHECK_FOR_ABORT state " << threadIndex_;
  auto numRead = socket_->read(buf_, 1);
  if (numRead != 1) {
    VLOG(1) << "No abort cmd found";
    return CONNECT;
  }
  Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf_[0];
  if (cmd != Protocol::ABORT_CMD) {
    VLOG(1) << "Unexpected result found while reading for abort " << buf_[0];
    return CONNECT;
  }
  threadStats_.addHeaderBytes(1);
  return PROCESS_ABORT_CMD;
}

SenderState SenderThread::readFileChunks() {
  LOG(INFO) << "entered READ_FILE_CHUNKS state " << threadIndex_;
  int64_t numRead = socket_->read(buf_, 1);
  auto metaData = getMetaData();
  if (numRead != 1) {
    LOG(ERROR) << "Socket read error 1 " << numRead;
    threadStats_.setErrorCode(SOCKET_READ_ERROR);
    return CHECK_FOR_ABORT;
  }
  threadStats_.addHeaderBytes(numRead);
  Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf_[0];
  if (cmd == Protocol::ABORT_CMD) {
    return PROCESS_ABORT_CMD;
  }
  if (cmd == Protocol::WAIT_CMD) {
    return READ_FILE_CHUNKS;
  }
  if (cmd == Protocol::ACK_CMD) {
    if (!metaData->isFileChunksReceived()) {
      LOG(ERROR) << "Sender has not yet received file chunks, but receiver "
                 << "thinks it has already sent it";
      threadStats_.setErrorCode(PROTOCOL_ERROR);
      return END;
    }
    return SEND_BLOCKS;
  }
  if (cmd != Protocol::CHUNKS_CMD) {
    LOG(ERROR) << "Unexpected cmd " << cmd;
    threadStats_.setErrorCode(PROTOCOL_ERROR);
    return END;
  }
  int64_t toRead = Protocol::kChunksCmdLen;
  numRead = socket_->read(buf_, toRead);
  if (numRead != toRead) {
    LOG(ERROR) << "Socket read error " << toRead << " " << numRead;
    threadStats_.setErrorCode(SOCKET_READ_ERROR);
    return CHECK_FOR_ABORT;
  }
  threadStats_.addHeaderBytes(numRead);
  int64_t off = 0;
  int64_t bufSize, numFiles;
  Protocol::decodeChunksCmd(buf_, off, bufSize, numFiles);
  LOG(INFO) << "File chunk list has " << numFiles
            << " entries and is broken in buffers of length " << bufSize;
  std::unique_ptr<char[]> chunkBuffer(new char[bufSize]);
  std::vector<FileChunksInfo> fileChunksInfoList;
  while (true) {
    int64_t numFileChunks = fileChunksInfoList.size();
    if (numFileChunks > numFiles) {
      // We should never be able to read more file chunks than mentioned in the
      // chunks cmd. Chunks cmd has buffer size used to transfer chunks and also
      // number of chunks. This chunks are read and parsed and added to
      // fileChunksInfoList. Number of chunks we decode should match with the
      // number mentioned in the Chunks cmd.
      LOG(ERROR) << "Number of file chunks received is more than the number "
                    "mentioned in CHUNKS_CMD "
                 << numFileChunks << " " << numFiles;
      threadStats_.setErrorCode(PROTOCOL_ERROR);
      return END;
    }
    if (numFileChunks == numFiles) {
      break;
    }
    toRead = sizeof(int32_t);
    numRead = socket_->read(buf_, toRead);
    if (numRead != toRead) {
      LOG(ERROR) << "Socket read error " << toRead << " " << numRead;
      threadStats_.setErrorCode(SOCKET_READ_ERROR);
      return CHECK_FOR_ABORT;
    }
    toRead = folly::loadUnaligned<int32_t>(buf_);
    toRead = folly::Endian::little(toRead);
    numRead = socket_->read(chunkBuffer.get(), toRead);
    if (numRead != toRead) {
      LOG(ERROR) << "Socket read error " << toRead << " " << numRead;
      threadStats_.setErrorCode(SOCKET_READ_ERROR);
      return CHECK_FOR_ABORT;
    }
    threadStats_.addHeaderBytes(numRead);
    off = 0;
    // decode function below adds decoded file chunks to fileChunksInfoList
    bool success = Protocol::decodeFileChunksInfoList(
        chunkBuffer.get(), off, toRead, fileChunksInfoList);
    if (!success) {
      LOG(ERROR) << "Unable to decode file chunks list";
      threadStats_.setErrorCode(PROTOCOL_ERROR);
      return END;
    }
  }
  metaData->setFileChunksInfo(fileChunksInfoList);
  // send ack for file chunks list
  buf_[0] = Protocol::ACK_CMD;
  int64_t toWrite = 1;
  int64_t written = socket_->write(buf_, toWrite);
  if (toWrite != written) {
    LOG(ERROR) << "Socket write error " << toWrite << " " << written;
    threadStats_.setErrorCode(SOCKET_WRITE_ERROR);
    return CHECK_FOR_ABORT;
  }
  threadStats_.addHeaderBytes(written);
  return SEND_BLOCKS;
}

SenderState SenderThread::readReceiverCmd() {
  VLOG(1) << "entered READ_RECEIVER_CMD state " << threadIndex_;
  int64_t numRead = socket_->read(buf_, 1);
  if (numRead != 1) {
    LOG(ERROR) << "READ unexpected " << numRead;
    threadStats_.setErrorCode(SOCKET_READ_ERROR);
    return CONNECT;
  }
  Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf_[0];
  if (cmd == Protocol::ERR_CMD) {
    return PROCESS_ERR_CMD;
  }
  if (cmd == Protocol::WAIT_CMD) {
    return PROCESS_WAIT_CMD;
  }
  if (cmd == Protocol::DONE_CMD) {
    return PROCESS_DONE_CMD;
  }
  if (cmd == Protocol::ABORT_CMD) {
    return PROCESS_ABORT_CMD;
  }
  threadStats_.setErrorCode(PROTOCOL_ERROR);
  return END;
}

SenderState SenderThread::processDoneCmd() {
  VLOG(1) << "entered PROCESS_DONE_CMD state " << threadIndex_;
  ThreadTransferHistory &transferHistory = getTransferHistory();
  transferHistory.markAllAcknowledged();

  // send ack for DONE
  buf_[0] = Protocol::DONE_CMD;
  socket_->write(buf_, 1);

  socket_->shutdown();
  auto numRead = socket_->read(buf_, Protocol::kMinBufLength);
  if (numRead != 0) {
    LOG(WARNING) << "EOF not found when expected";
    return END;
  }
  VLOG(1) << "done with transfer, port " << port_;
  return END;
}

SenderState SenderThread::processWaitCmd() {
  LOG(INFO) << "entered PROCESS_WAIT_CMD state " << threadIndex_;
  ThreadTransferHistory &transferHistory = getTransferHistory();
  VLOG(1) << "received WAIT_CMD, port " << port_;
  transferHistory.markAllAcknowledged();
  return READ_RECEIVER_CMD;
}

SenderState SenderThread::processErrCmd() {
  LOG(INFO) << "entered PROCESS_ERR_CMD state " << threadIndex_ << " port "
            << port_;
  ThreadTransferHistory &transferHistory = getTransferHistory();
  auto metaData = getMetaData();
  int64_t toRead = sizeof(int16_t);
  int64_t numRead = socket_->read(buf_, toRead);
  if (numRead != toRead) {
    LOG(ERROR) << "read unexpected " << toRead << " " << numRead;
    threadStats_.setErrorCode(SOCKET_READ_ERROR);
    return CONNECT;
  }

  int16_t checkpointsLen = folly::loadUnaligned<int16_t>(buf_);
  checkpointsLen = folly::Endian::little(checkpointsLen);
  char checkpointBuf[checkpointsLen];
  numRead = socket_->read(checkpointBuf, checkpointsLen);
  if (numRead != checkpointsLen) {
    LOG(ERROR) << "read unexpected " << checkpointsLen << " " << numRead;
    threadStats_.setErrorCode(SOCKET_READ_ERROR);
    return CONNECT;
  }

  std::vector<Checkpoint> checkpoints;
  int64_t decodeOffset = 0;
  if (!Protocol::decodeCheckpoints(metaData->getProtocolVersion(),
                                   checkpointBuf, decodeOffset, checkpointsLen,
                                   checkpoints)) {
    LOG(ERROR) << "checkpoint decode failure "
               << folly::humanify(std::string(checkpointBuf, checkpointsLen));
    threadStats_.setErrorCode(PROTOCOL_ERROR);
    return END;
  }
  transferHistory.markAllAcknowledged();
  for (auto &checkpoint : checkpoints) {
    transferHistoryController_->handleCheckpoint(checkpoint);
  }
  return SEND_BLOCKS;
}

SenderState SenderThread::processAbortCmd() {
  LOG(INFO) << "entered PROCESS_ABORT_CMD state " << threadIndex_;
  ThreadTransferHistory &transferHistory = getTransferHistory();
  auto metaData = getMetaData();
  threadStats_.setErrorCode(ABORT);
  int toRead = Protocol::kAbortLength;
  auto numRead = socket_->read(buf_, toRead);
  if (numRead != toRead) {
    // can not read checkpoint, but still must exit because of ABORT
    LOG(ERROR) << "Error while trying to read ABORT cmd " << numRead << " "
               << toRead;
    return END;
  }
  int64_t offset = 0;
  int32_t negotiatedProtocol;
  ErrorCode remoteError;
  int64_t checkpoint;
  Protocol::decodeAbort(buf_, offset, negotiatedProtocol, remoteError,
                        checkpoint);
  threadStats_.setRemoteErrorCode(remoteError);
  std::string failedFileName = transferHistory.getSourceId(checkpoint);
  LOG(WARNING) << "Received abort on " << threadIndex_
               << " remote protocol version " << negotiatedProtocol
               << " remote error code " << errorCodeToStr(remoteError)
               << " file " << failedFileName << " checkpoint " << checkpoint;
  metaData->abort(remoteError);
  if (remoteError == VERSION_MISMATCH) {
    if (Protocol::negotiateProtocol(negotiatedProtocol,
                                    metaData->getProtocolVersion()) ==
        negotiatedProtocol) {
      // sender can support this negotiated version
      negotiatedProtocol_ = negotiatedProtocol;
      return PROCESS_VERSION_MISMATCH;
    } else {
      LOG(ERROR) << "Sender can not support receiver version "
                 << negotiatedProtocol;
      threadStats_.setRemoteErrorCode(VERSION_INCOMPATIBLE);
    }
  }
  return END;
}

SenderState SenderThread::processVersionMismatch() {
  LOG(INFO) << "entered PROCESS_VERSION_MISMATCH state " << threadIndex_;
  WDT_CHECK(threadStats_.getErrorCode() == ABORT);
  auto metaData = getMetaData();
  const auto &options = WdtOptions::get();
  auto negotiationStatus = metaData->getNegotiationStatus();
  switch (negotiationStatus) {
    case V_MISMATCH_FAILED:
      LOG(WARNING) << "Protocol version already negotiated, but transfer still "
                   << "aborted due to version mismatch, port " << port_;
      return END;
    case V_MISMATCH_RESOLVED:
      threadStats_.setRemoteErrorCode(OK);
      return CONNECT;
    case V_MISMATCH_WAIT:
      break;
  };

  // Need a barrier here to make sure all the negotiated protocol versions
  // have been collected
  const std::string barrierName = "PROCESS_VERSION_MISMATCH_BARRIER";
  auto barrier = controller_->getBarrier(barrierName);
  barrier->execute();

  SenderState state = PROCESS_VERSION_MISMATCH;
  auto progressFn = [&]() -> bool {
    // do nothing
    return true;
  };

  auto endFn = [&]() -> bool { return true; };

  auto startFn = [&]() -> bool {
    metaData->setProtoNegotiationStatus(V_MISMATCH_FAILED);
    if (metaData->verifyVersionMismatchStats() != OK) {
      state = END;
      return true;
    }
    int negotiatedProtocol = 0;
    for (int protocolVersion : metaData->getNegotiatedProtocols()) {
      if (protocolVersion > 0) {
        if (negotiatedProtocol > 0 && negotiatedProtocol != protocolVersion) {
          LOG(ERROR) << "Different threads negotiated different protocols "
                     << negotiatedProtocol << " " << protocolVersion;
          state = END;
          return true;
        }
        negotiatedProtocol = protocolVersion;
      }
    }
    WDT_CHECK_GT(negotiatedProtocol, 0);
    auto protocolVersion = metaData->getProtocolVersion();
    LOG_IF(INFO, negotiatedProtocol != protocolVersion)
        << "Changing protocol version to " << negotiatedProtocol
        << ", previous version " << protocolVersion;
    metaData->setProtocolVersion(negotiatedProtocol);
    threadStats_.setRemoteErrorCode(OK);
    metaData->setProtoNegotiationStatus(V_MISMATCH_RESOLVED);
    metaData->clearAbort();
    state = CONNECT;
    return true;
  };
  const std::string funnelName = "PROCESS_VERSION_MISMATCH";
  auto execFunnel = controller_->getFunnel(funnelName);
  execFunnel->execute<decltype(startFn), decltype(progressFn), decltype(endFn)>(
      startFn, progressFn, endFn);
  return state;
}

void SenderThread::startThread() {
  if (threadPtr_) {
    LOG(WARNING) << "There is a already a thread running. Trying to join the"
                 << "previous thread";
    finish();
  }
  threadPtr_.reset(new std::thread(&SenderThread::start, this));
}

void SenderThread::start() {
  INIT_PERF_STAT_REPORT
  Clock::time_point startTime = Clock::now();
  auto metaData = getMetaData();
  auto completionGuard = folly::makeGuard([&] {
    controller_->deRegisterThread(threadIndex_);
    auto endFunc = [&]() { metaData->endCurTransfer(); };
    controller_->executeAtEnd<decltype(endFunc)>(endFunc);
  });
  metaData->startNewTransfer();
  SenderState state = CONNECT;
  while (state != END) {
    ErrorCode abortCode = metaData->getCurAbortCode();
    if (abortCode != OK) {
      LOG(ERROR) << "Transfer aborted " << port_ << " "
                 << errorCodeToStr(abortCode);
      threadStats_.setErrorCode(ABORT);
      if (abortCode == VERSION_MISMATCH) {
        state = PROCESS_VERSION_MISMATCH;
      } else {
        break;
      }
    }
    state = (this->*stateMap_[state])();
  }

  double totalTime = durationSeconds(Clock::now() - startTime);
  LOG(INFO) << "Port " << port_ << " done. " << threadStats_
            << " Total throughput = "
            << threadStats_.getEffectiveTotalBytes() / totalTime / kMbToB
            << " Mbytes/sec";
  perfReport_ = *perfStatReport;
  return;
}

PerfStatReport SenderThread::getPerfReport() const {
  return perfReport_;
}

const TransferStats &SenderThread::getTransferStats() const {
  return threadStats_;
}

int SenderThread::getNegotiatedProtocol() const {
  return negotiatedProtocol_;
}

TransferStats SenderThread::finish() {
  if (!threadPtr_) {
    LOG(ERROR) << "Finish called on an instance while no thread has been "
               << " created to do any work";
    return std::move(threadStats_);
  }
  threadPtr_->join();
  threadPtr_.reset();
  return std::move(threadStats_);
}

SenderThread::~SenderThread() {
  if (threadPtr_) {
    LOG(INFO) << threadIndex_
              << " has an alive thread while the instance is being "
              << "destructed";
    finish();
  }
}

void SenderThread::reset() {
  totalSizeSent_ = false;
  threadStats_.setErrorCode(OK);
}

Sender *SenderThread::getMetaData() const {
  return (Sender *)controller_->getMetaData();
}
}
}
