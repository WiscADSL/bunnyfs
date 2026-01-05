#ifndef CFS_INCLUDE_FSPROC_TLS_H_
#define CFS_INCLUDE_FSPROC_TLS_H_

#include <string>

class FsProcWorkerMaster;
class FsProcWorkerServant;

class FsProcTLS {
 private:
  thread_local static int wid_;
  static void SetWid(int wid) {
    FsProcTLS::wid_ = wid;
    std::string name = "Worker-" + std::to_string(wid);
    pthread_setname_np(pthread_self(), name.c_str());
  }

 public:
  // NOTE: while convenient, please use sparingly as I have not measured how
  // much is the cost of accessing a thread local variable.
  static int GetWid() { return FsProcTLS::wid_; }

  // Only workers can call SetTLWid
  friend class FsProcWorkerMaster;
  friend class FsProcWorkerServant;
};

#endif
