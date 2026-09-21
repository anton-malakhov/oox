#pragma once

#include <functional>
#include <thread>

struct StlThreadEnvironment {
  // EnvThread constructor must start the thread,
  // destructor must join the thread.
  class EnvThread {
  public:
    template <typename F> EnvThread(F &&f) : thr_(std::forward<F>(f)) {}
    ~EnvThread() { thr_.join(); }
    // This function is called when the threadpool is cancelled.
    void OnCancel() {}

  private:
    std::thread thr_;
  };

  EnvThread *CreateThread(std::function<void()> f) {
    return new EnvThread(std::move(f));
  }
};
