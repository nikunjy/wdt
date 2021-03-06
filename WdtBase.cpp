/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "WdtBase.h"
#include "SocketUtils.h"
#include <folly/Conv.h>
#include <folly/Range.h>
#include <ctime>
#include <random>
using namespace std;
using folly::StringPiece;
namespace facebook {
namespace wdt {
WdtUri::WdtUri(const string& url) {
  errorCode_ = process(url);
}

void WdtUri::setHostName(const string& hostName) {
  hostName_ = hostName;
  if (hostName.find(":") != string::npos) {
    // Having a : means its an ipv6 address
    hostName_ = "[" + hostName + "]";
  }
}

void WdtUri::setPort(int32_t port) {
  port_ = std::to_string(port);
}

void WdtUri::setQueryParam(const string& key, const string& value) {
  queryParams_[key] = value;
}

ErrorCode WdtUri::getErrorCode() const {
  return errorCode_;
}

string WdtUri::generateUrl() const {
  string url;
  folly::toAppend(WDT_URL_PREFIX, hostName_, &url);
  if (getPort() > 0) {
    folly::toAppend(":", port_, &url);
  }
  char prefix = '?';
  for (const auto& pair : queryParams_) {
    folly::toAppend(prefix, pair.first, "=", pair.second, &url);
    prefix = '&';
  }
  return url;
}

ErrorCode WdtUri::process(const string& url) {
  if (url.size() < WDT_URL_PREFIX.size()) {
    LOG(ERROR) << "Url doesn't specify wdt protocol";
    return URI_PARSE_ERROR;
  }
  StringPiece urlPiece(url, 0, WDT_URL_PREFIX.size());
  StringPiece wdtPrefix(WDT_URL_PREFIX);
  if (urlPiece != wdtPrefix) {
    LOG(ERROR) << "Url does not specify wdt protocol " << url;
    return URI_PARSE_ERROR;
  }
  urlPiece = StringPiece(url, WDT_URL_PREFIX.size());
  size_t paramsIndex = urlPiece.find("?");
  if (paramsIndex == string::npos) {
    paramsIndex = urlPiece.size();
  }
  ErrorCode status = OK;
  StringPiece hostNamePiece(urlPiece, 0, paramsIndex);
  if (hostNamePiece.empty()) {
    LOG(ERROR) << "URL doesn't have a valid host name " << url;
    status = URI_PARSE_ERROR;
  } else {
    bool isIpv6 = false;
    size_t addrEndIndex = hostNamePiece.find("]");
    if (addrEndIndex != string::npos) {
      isIpv6 = true;
      ++addrEndIndex;
    } else {
      size_t portStartIndex = hostNamePiece.find(":");
      if (portStartIndex != string::npos) {
        addrEndIndex = portStartIndex;
      } else {
        addrEndIndex = hostNamePiece.size();
      }
    }
    hostName_.assign(hostNamePiece.data(), addrEndIndex);
    if (isIpv6) {
      if (hostName_.find("[") == string::npos) {
        LOG(ERROR) << "Malformed ipv6 address " << hostName_;
        hostName_ = "";
        status = URI_PARSE_ERROR;
      }
      if (hostName_.find(":") == string::npos) {
        LOG(ERROR) << "Malformed ipv6 address " << hostName_;
        hostName_ = "";
        status = URI_PARSE_ERROR;
      }
    } else {
      if (hostName_.find(":") != string::npos) {
        LOG(ERROR) << "ipv6 address found without enclosing []" << hostName_;
        hostName_ = "";
        status = URI_PARSE_ERROR;
      }
    }
    if (addrEndIndex < hostNamePiece.size() - 1 &&
        hostNamePiece[addrEndIndex] == ':') {
      ++addrEndIndex;
      port_.assign(hostNamePiece.data(), addrEndIndex,
                   hostNamePiece.size() - addrEndIndex);
    }
  }
  urlPiece.advance(paramsIndex + (paramsIndex < urlPiece.size()));
  while (!urlPiece.empty()) {
    StringPiece keyValuePair = urlPiece.split_step('&');
    if (keyValuePair.empty()) {
      // Last key value pair
      keyValuePair = urlPiece;
      urlPiece.advance(urlPiece.size());
    }
    StringPiece key = keyValuePair.split_step('=');
    StringPiece value = keyValuePair;
    if (key.empty()) {
      // Value can be empty but key can't be empty
      LOG(ERROR) << "Errors parsing params, url = " << url;
      status = URI_PARSE_ERROR;
      break;
    }
    queryParams_[key.toString()] = value.toString();
  }
  return status;
}

string WdtUri::getHostName() const {
  string hostName = hostName_;
  if (hostName.size() > 0 && hostName[0] == '[' &&
      hostName[hostName.size() - 1] == ']') {
    hostName = hostName.substr(1, hostName.size() - 2);
  }
  return hostName;
}

int32_t WdtUri::getPort() const {
  int32_t port = -1;
  try {
    port = folly::to<int32_t>(port_);
  } catch (std::exception& e) {
    LOG(ERROR) << "Invalid port in the uri " << port_;
  }
  return port;
}

string WdtUri::getQueryParam(const string& key) const {
  auto it = queryParams_.find(key);
  if (it == queryParams_.end()) {
    VLOG(1) << "Couldn't find query param " << key;
    return "";
  }
  return it->second;
}

const unordered_map<string, string>& WdtUri::getQueryParams() const {
  return queryParams_;
}

void WdtUri::clear() {
  hostName_.clear();
  port_.clear();
  queryParams_.clear();
}

WdtUri& WdtUri::operator=(const string& url) {
  clear();
  errorCode_ = process(url);
  return *this;
}

const string WdtTransferRequest::TRANSFER_ID_PARAM{"id"};
const string WdtTransferRequest::PROTOCOL_VERSION_PARAM{"protocol"};
const string WdtTransferRequest::DIRECTORY_PARAM{"dir"};
const string WdtTransferRequest::PORTS_PARAM{"ports"};
const string WdtTransferRequest::START_PORT_PARAM{"start_port"};
const string WdtTransferRequest::NUM_PORTS_PARAM{"num_ports"};

WdtTransferRequest::WdtTransferRequest(const vector<int32_t> &ports) {
  this->ports = ports;
  // Sort the ports so that if they are a sequence then you
  // can detect that
  sort(this->ports.begin(), this->ports.end());
}

WdtTransferRequest::WdtTransferRequest(int startPort, int numPorts,
                                       const string& directory) {
  this->directory = directory;
  int portNum = startPort;
  for (int i = 0; i < numPorts; i++) {
    ports.push_back(portNum);
    if (startPort) {
      ++portNum;
    }
  }
}

WdtTransferRequest::WdtTransferRequest(const string& uriString) {
  WdtUri wdtUri(uriString);
  errorCode = wdtUri.getErrorCode();
  hostName = wdtUri.getHostName();
  transferId = wdtUri.getQueryParam(TRANSFER_ID_PARAM);
  directory = wdtUri.getQueryParam(DIRECTORY_PARAM);
  try {
    protocolVersion =
        folly::to<int64_t>(wdtUri.getQueryParam(PROTOCOL_VERSION_PARAM));
  } catch (std::exception& e) {
    LOG(ERROR) << "Error parsing protocol version "
               << wdtUri.getQueryParam(PROTOCOL_VERSION_PARAM);
    errorCode = URI_PARSE_ERROR;
  }
  string portsStr(wdtUri.getQueryParam(PORTS_PARAM));
  StringPiece portsList(portsStr);  // pointers into portsStr
  do {
    StringPiece portNum = portsList.split_step(',');
    int port;
    if (!portNum.empty()) {
      try {
        port = folly::to<int32_t>(portNum);
        ports.push_back(port);
      } catch (std::exception& e) {
        LOG(ERROR) << "Couldn't convert " << portNum << " to valid port number";
        errorCode = URI_PARSE_ERROR;
      }
    }
  } while (!portsList.empty());
  if (!ports.empty()) {
    return;
  }
  string startPortStr = wdtUri.getQueryParam(START_PORT_PARAM);
  string numPortsStr = wdtUri.getQueryParam(NUM_PORTS_PARAM);
  const auto& options = WdtOptions::get();
  int32_t startPort = wdtUri.getPort();
  if (startPort < 0) {
    startPort = options.start_port;
    if (!startPortStr.empty()) {
      try {
        startPort = folly::to<int32_t>(startPortStr);
      } catch(std::exception& e) {
        LOG(ERROR) << "Couldn't convert start port " << startPortStr;
      }
    } 
  }
  int numPorts = options.num_ports;
  if (!numPortsStr.empty()) {
      try {
        numPorts = folly::to<int32_t>(numPortsStr);
      } catch(std::exception& e) {
        LOG(ERROR) << "Couldn't convert num ports " << numPortsStr;
      }
  }
  ports = std::move(WdtBase::genPortsVector(startPort, numPorts));
}

string WdtTransferRequest::generateUrl(bool genFull) const {
  if (errorCode == ERROR || errorCode == URI_PARSE_ERROR) {
    LOG(ERROR) << "Transfer request has errors present ";
    return errorCodeToStr(errorCode);
  }
  WdtUri wdtUri;
  wdtUri.setHostName(hostName);
  wdtUri.setQueryParam(TRANSFER_ID_PARAM, transferId);
  wdtUri.setQueryParam(PROTOCOL_VERSION_PARAM,
                       folly::to<string>(protocolVersion));
  serializePorts(wdtUri);
  if (genFull) {
    wdtUri.setQueryParam(DIRECTORY_PARAM, directory);
  }
  return wdtUri.generateUrl();
}

void WdtTransferRequest::serializePorts(WdtUri& wdtUri) const {
  if (ports.size() == 0) {
    return;
  }
  int32_t startPort = ports[0];
  bool hasHoles = false;
  for (size_t i = 0; i < ports.size(); i++) {
    if (ports[i] != startPort + i) {
      hasHoles = true;
      break;
    }
  }
  if (hasHoles) {
    wdtUri.setQueryParam(PORTS_PARAM, getSerializedPortsList());
  } else {
    wdtUri.setPort(startPort);
    wdtUri.setQueryParam(NUM_PORTS_PARAM, folly::to<string>(ports.size()));
  }
}

string WdtTransferRequest::getSerializedPortsList() const {
  string portsList = "";
  for (size_t i = 0; i < ports.size(); i++) {
    if (i != 0) {
      folly::toAppend(",", &portsList);
    }
    auto port = ports[i];
    folly::toAppend(port, &portsList);
  }
  return portsList;
}

bool WdtTransferRequest::operator==(const WdtTransferRequest& that) const {
  bool result = true;
  result &= (transferId == that.transferId);
  result &= (protocolVersion == that.protocolVersion);
  result &= (directory == that.directory);
  result &= (hostName == that.hostName);
  result &= (ports == that.ports);
  // No need to check the file info, simply checking whether two objects
  // are same with respect to the wdt settings
  return result;
}

WdtBase::WdtBase() : abortCheckerCallback_(this) {
}

WdtBase::~WdtBase() {
  abortChecker_ = nullptr;
}

std::vector<int32_t> WdtBase::genPortsVector(int32_t startPort,
                                             int32_t numPorts) {
  std::vector<int32_t> ports;
  for (int32_t i = 0; i < numPorts; i++) {
    ports.push_back(startPort + i);
  }
  return ports;
}

void WdtBase::abort(const ErrorCode abortCode) {
  folly::RWSpinLock::WriteHolder guard(abortCodeLock_);
  if (abortCode == VERSION_MISMATCH && abortCode_ != OK) {
    // VERSION_MISMATCH is the lowest priority abort code. If the abort code is
    // anything other than OK, we should not override it
    return;
  }
  LOG(WARNING) << "Setting the abort code " << abortCode;
  abortCode_ = abortCode;
}

void WdtBase::clearAbort() {
  folly::RWSpinLock::WriteHolder guard(abortCodeLock_);
  if (abortCode_ != VERSION_MISMATCH) {
    // We do no clear abort code unless it is VERSION_MISMATCH
    return;
  }
  LOG(WARNING) << "Clearing the abort code";
  abortCode_ = OK;
}

void WdtBase::setAbortChecker(const std::shared_ptr<IAbortChecker>& checker) {
  abortChecker_ = checker;
}

ErrorCode WdtBase::getCurAbortCode() {
  // external check, if any:
  if (abortChecker_ && abortChecker_->shouldAbort()) {
    return ABORTED_BY_APPLICATION;
  }
  folly::RWSpinLock::ReadHolder guard(abortCodeLock_);
  // internal check:
  return abortCode_;
}

void WdtBase::setProgressReporter(
    std::unique_ptr<ProgressReporter>& progressReporter) {
  progressReporter_ = std::move(progressReporter);
}

void WdtBase::setThrottler(std::shared_ptr<Throttler> throttler) {
  VLOG(2) << "Setting an external throttler";
  throttler_ = throttler;
}

void WdtBase::setTransferId(const std::string& transferId) {
  transferId_ = transferId;
  LOG(INFO) << "Setting transfer id " << transferId_;
}

void WdtBase::setProtocolVersion(int64_t protocolVersion) {
  WDT_CHECK(protocolVersion > 0) << "Protocol version can't be <= 0 "
                                 << protocolVersion;
  WDT_CHECK(Protocol::negotiateProtocol(protocolVersion) == protocolVersion)
      << "Can not support wdt version " << protocolVersion;
  protocolVersion_ = protocolVersion;
  LOG(INFO) << "using wdt protocol version " << protocolVersion_;
}

std::string WdtBase::getTransferId() {
  return transferId_;
}

void WdtBase::configureThrottler() {
  WDT_CHECK(!throttler_);
  VLOG(1) << "Configuring throttler options";
  const auto& options = WdtOptions::get();
  double avgRateBytesPerSec = options.avg_mbytes_per_sec * kMbToB;
  double peakRateBytesPerSec = options.max_mbytes_per_sec * kMbToB;
  double bucketLimitBytes = options.throttler_bucket_limit * kMbToB;
  throttler_ = Throttler::makeThrottler(avgRateBytesPerSec, peakRateBytesPerSec,
                                        bucketLimitBytes,
                                        options.throttler_log_time_millis);
  if (throttler_) {
    LOG(INFO) << "Enabling throttling " << *throttler_;
  } else {
    LOG(INFO) << "Throttling not enabled";
  }
}

string WdtBase::generateTransferId() {
  static std::default_random_engine randomEngine{std::random_device()()};
  static std::mutex mutex;
  string transferId;
  {
    std::lock_guard<std::mutex> lock(mutex);
    transferId = to_string(randomEngine());
  }
  LOG(INFO) << "Generated a transfer id " << transferId;
  return transferId;
}
}
}  // namespace facebook::wdt
