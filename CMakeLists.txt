# CMake build file - use CMake 3.1 or later to build WDT and its dependencies.
#
# mkdir build; cd build; cmake .. -DBUILD_TESTING=on; make -j
# omit -DBUILD_TESTING=on if you don't want the extra dependencies for
# testing (but testing is good !)
# (at fb:
# cd local; mkdir wdt_build; cd wdt_build
# cmake31 ~/fbcode/wdt -DFOLLY_SOURCE_DIR=$HOME/fbcode -DBUILD_TESTING=on
# make -j
# )
#
#  Copywright (c) 2014, Facebook, Inc.
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree. An additional grant
#  of patent rights can be found in the PATENTS file in the same directory.
#

cmake_minimum_required(VERSION 3.1)

# There is no C per se in WDT but if you use CXX only here many checks fail
project("WDT" C CXX)

# On MacOS this requires the latest (master) CMake (and/or CMake 3.1.1/3.2)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED on)

# somehow 'option' for this doesn't seeem to work/I don't know how to make it
set(BUILD_SHARED_LIBS on CACHE Bool "build shared libs")

# Optimized by default
# TODO: This doesn't seem to work / sets default to "" instead of Release...
# set(CMAKE_BUILD_TYPE Release CACHE String "build type")
# So hardcoding for now:
set(CMAKE_BUILD_TYPE Release)


# Check that we have the Folly source tree
set(FOLLY_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/deps/folly CACHE path
    "Folly source tree (folly/ThreadLocal.h should be reachable from there")
# Check for folly - TODO: this doesn't work well for relative paths
# (because of relative to build dir vs relative to source tree for -I)
if(NOT EXISTS "${FOLLY_SOURCE_DIR}/folly/ThreadLocal.h")
  MESSAGE(FATAL_ERROR "${FOLLY_SOURCE_DIR}/folly/ThreadLocal.h not found
Fix using:
mkdir deps; cd deps; git clone https://github.com/facebook/folly.git
or change FOLLY_SOURCE_DIR (use ccmake or -DFOLLY_SOURCE_DIR=...)
")
endif()


# The part of folly that isn't pure .h and we use:
set (FOLLY_CPP_SRC
${FOLLY_SOURCE_DIR}/folly/Conv.cpp
${FOLLY_SOURCE_DIR}/folly/Demangle.cpp
${FOLLY_SOURCE_DIR}/folly/Malloc.cpp
)

# WDT's library proper - comes from: ls -1 *.cpp | grep -iv test
add_library(wdtlib_min
ClientSocket.cpp
DirectorySourceQueue.cpp
FileByteSource.cpp
FileCreator.cpp
Protocol.cpp
Receiver.cpp
Reporting.cpp
Sender.cpp
ServerSocket.cpp
SocketUtils.cpp
Throttler.cpp
WdtOptions.cpp
)
add_library(wdtlib
WdtFlags.cpp
)
target_link_libraries(wdtlib wdtlib_min)
# Folly uses boost system
#set(Boost_USE_STATIC_LIBS on)
find_package(Boost COMPONENTS system REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

# We use std:: threads
find_package(Threads) # this will set ${CMAKE_THREAD_LIBS_INIT} (ie pthreads)

# Glog
find_path(GLOG_INCLUDE_DIR glog/logging.h)
find_library(GLOG_LIBRARY glog)
# Gflags
find_path(GFLAGS_INCLUDE_DIR gflags/gflags.h)
find_library(GFLAGS_LIBRARY gflags)

# You can also add jemalloc to the list if you have it/want it
target_link_libraries(wdtlib
  folly4wdt
  ${GLOG_LIBRARY}
  ${GFLAGS_LIBRARY}
  ${Boost_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT} # Must be last to avoid link errors
)

# What we need to build the part of folly we use:

include(CheckIncludeFileCXX)
include(CheckFunctionExists)
include(CheckLibraryExists)
include(CheckCXXSourceCompiles)
# C based check (which fail with the c++ setting thereafter...)
check_library_exists(rt clock_gettime "" FOLLY_HAVE_CLOCK_GETTIME)
set(SAVE_CMRL ${CMAKE_REQUIRED_LIBRARIES}) #globals are evil/ugly
set(CMAKE_REQUIRED_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
check_function_exists(pthread_atfork FOLLY_HAVE_PTHREAD_ATFORK)
set(CMAKE_REQUIRED_LIBRARIES ${SAVE_CMRL}) #globals are evil/ugly
# Needed until Cmake issue #15361 is addressed
set(CMAKE_REQUIRED_DEFINITIONS ${CMAKE_CXX11_STANDARD_COMPILE_OPTION})
check_include_file_cxx(malloc.h FOLLY_HAVE_MALLOC_H)
check_include_file_cxx(features.h FOLLY_HAVE_FEATURES_H)
check_include_file_cxx(bits/c++config.h FOLLY_HAVE_BITS_CXXCONFIG_H)
check_include_file_cxx(bits/functexcept.h FOLLY_HAVE_BITS_FUNCTEXCEPT)
#check_function_exists(clock_gettime FOLLY_HAVE_CLOCK_GETTIME)
check_cxx_source_compiles("#include <type_traits>
      #if !_LIBCPP_VERSION
      #error No libc++
      #endif
      int main() {return 0;}" FOLLY_USE_LIBCPP)
check_cxx_source_compiles(
"extern \"C\" void cmkcheckweak() __attribute__((weak));
int main(int argc, char** argv) {
  return (cmkcheckweak) ? 1 : 0 ;
}" FOLLY_HAVE_WEAK_SYMBOLS)
# Now record all this :
configure_file(folly-config.h.in folly/folly-config.h)

# Malloc stuff  tied to not supporting weaksympbols
if (NOT FOLLY_HAVE_WEAK_SYMBOLS)
  list(APPEND FOLLY_CPP_SRC ${FOLLY_SOURCE_DIR}/folly/detail/MallocImpl.cpp)
  message(STATUS "no weak symbols, adding MallocImpl to folly src")
endif()
# For missing __throw_logic_error:
if (NOT FOLLY_HAVE_BITS_FUNCTEXCEPT)
  list(APPEND FOLLY_CPP_SRC ${FOLLY_SOURCE_DIR}/folly/detail/FunctionalExcept.cpp)
  message(STATUS "no bits/functexcept.h, adding FunctionalExcept to folly src")
endif()

add_library(folly4wdt ${FOLLY_CPP_SRC})

# Order is important - inside fb we want the above
# folly-config.h to be picked up instead of the fbcode one
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${FOLLY_SOURCE_DIR})
include_directories(${GLOG_INCLUDE_DIR})
include_directories(${GFLAGS_INCLUDE_DIR})

add_executable(wdt wdtCmdLine.cpp)

target_link_libraries(wdt wdtlib)

### Install rules

install(TARGETS wdt wdtlib wdtlib_min folly4wdt CONFIGURATIONS Release
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
  )
# wcp script
install(PROGRAMS wcp.sh DESTINATION bin RENAME wcp)


### Everything below is about testing (and not required to create wdt/wdtlib)

if (BUILD_TESTING)

  enable_testing()

  # Extra code hat we use in tests
  add_library(wdt4tests
    ${FOLLY_SOURCE_DIR}/folly/FileUtil.cpp # used by Random used by tests
    ${FOLLY_SOURCE_DIR}/folly/Random.cpp # used indirectly by tests
  )


  include(ExternalProject)

  # GMock
  set(GMOCK_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gmock")
  externalproject_add(
    gmock
    SVN_REPOSITORY https://googlemock.googlecode.com/svn/trunk/
    INSTALL_COMMAND "" # Disable install step
    UPDATE_COMMAND "" # Doesn't change often
    PREFIX "${GMOCK_PREFIX}"
    #CONFIGURE_COMMAND "" # skip
    #BUILD_COMMAND "" # skip
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON
  )
  # Specify include dir for both gmock and gtest
  externalproject_get_property(gmock SOURCE_DIR)
  include_directories(${SOURCE_DIR}/include ${SOURCE_DIR}/gtest/include)

  externalproject_get_property(gmock BINARY_DIR)

  #  add_library(gmock_all STATIC EXCLUDE_FROM_ALL
  #   ${GMOCK_PREFIX}/src/gmock/gtest/src/gtest-all.cc
  #   ${GMOCK_PREFIX}/src/gmock/gmock-all.cc
  #   ${GMOCK_PREFIX}/src/gmock/gmock_main.cc)


  # ${BINARY_DIR}/libgmock.a works everywhere except xcode...
# so ugly weird hack generating warnings about unknown dir for now:
  target_link_libraries(wdt4tests
    "-L ${BINARY_DIR} -L ${BINARY_DIR}/Debug -lgmock"
    wdtlib
  )


  add_executable(protocol_test ProtocolTest.cpp)
  target_link_libraries(protocol_test wdt4tests)
  add_test(AllTestsInProtocolTest protocol_test)

  add_executable(sender_test SenderTest.cpp)
  target_link_libraries(sender_test wdt4tests)
  add_test(AllTestsInSenderTest sender_test)

endif(BUILD_TESTING)