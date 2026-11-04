// SPDX-License-Identifier: Apache-2.0
#include <atomic>
#include <functional>
namespace pending_observer { inline std::function<void()> acquired; }
#define OOX_EIGEN_TEST_LAZY_TASK_ACQUIRED() \
  do { if(pending_observer::acquired)pending_observer::acquired(); } while(false)
#include <oox/eigen/rapid_auto.h>
#include "eigen_test_wait.h"
#include <gtest/gtest.h>
#include <array>
#include <chrono>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>

namespace {
using namespace oox::detail::eigen_pool;
namespace pd=partitioner_detail;
using namespace std::chrono_literals;
template<class P> bool Until(P ready,std::chrono::steady_clock::duration timeout=2s) {
  const auto deadline=std::chrono::steady_clock::now()+timeout;
  while(!ready()) {
    if(std::chrono::steady_clock::now()>=deadline)return false;
    std::this_thread::yield();
  }
  return true;
}
template<class F> void Published(ThreadPool &pool,F &body,size_t begin,size_t end,size_t grain) {
  using State=partitioning::LazyAutoPartitionState;using Work=pd::Work<F,State>;
  auto *region=NewSmallObject<pd::Region>(pool,nullptr,true);
  {
    pd::Branch<Work,true> branch(nullptr);
    pd::ActiveRange<F,State> range(*region,&body,{begin,end,grain},State(pool.NumThreads()),branch);
    const bool published=pool.PublishStealableRange(range);EXPECT_TRUE(published);
    if(published) {
      size_t chunks=0;
      try {range.Run(chunks);}catch(...){region->Fail(std::current_exception());}
      range.Close();
    }
  }
  pool.Wait([&]{return region->IsComplete();});region->CloseAndWait();
  std::exception_ptr error;
  try{region->Rethrow();}catch(...){error=std::current_exception();}
  region->TaskComplete();if(error)std::rethrow_exception(error);
}
struct Blocker {
  explicit Blocker(ThreadPool &pool) {
    pool.RunOnThread(MakeTask([this,&pool]{
      worker=pool.CurrentThreadId();thread=std::this_thread::get_id();started=true;
      if(!Until([&]{return release.load();},10s))eigen_test_support::FailWait("pending-range helper blocker");
    }),1);
    eigen_test_support::WaitUntil([&]{return started.load();},"pending helper started");
  }
  ~Blocker(){release=true;}
  std::atomic<bool> started{false},release{false};size_t worker=size_t(-1);std::thread::id thread;
};
enum class Inner { Generic,Named };
void FastLeaf(ThreadPool &pool,rapid::RapidStartGroup group,Inner path,bool fail=false) {
  const auto context=rapid::CurrentResidentState();const bool executing=pool.IsExecutingTask();
  unsigned calls=0;
  auto body=[&](size_t i){EXPECT_EQ(i,0u);++calls;if(fail)throw std::runtime_error("caught inner fastleaf");};
  try {
    if(path==Inner::Named)rapid::ParallelForAuto(group,0,1,body,8);
    else ParallelFor(pool,0,1,body,8);
  }catch(...) {
    EXPECT_EQ(calls,1u);EXPECT_EQ(rapid::CurrentResidentState(),context);EXPECT_EQ(pool.IsExecutingTask(),executing);throw;
  }
  EXPECT_EQ(calls,1u);EXPECT_EQ(rapid::CurrentResidentState(),context);EXPECT_EQ(pool.IsExecutingTask(),executing);
}
struct GrownResult {bool progress=false,tail=false;unsigned mask=0;};
GrownResult Grown(Inner path,bool repeated=false,bool owner_throw=false,bool near_max=false) {
  constexpr size_t count=65;
  const size_t begin=near_max?std::numeric_limits<size_t>::max()-count:0;
  std::array<std::atomic<unsigned>,count> visits{};
  std::array<std::atomic<std::uint64_t>,count> values{};
  std::atomic<unsigned> private_mask{0},active{0};
  std::atomic<bool> inside{false},foreign_tail{false},running_moved{false},closed{false},after_return{false};
  ThreadPool pool(2,true,true,WorkerIdleMode::ResidentBusy);pool.SetResidentLimit(0);
  rapid::RapidDomainState state(pool);rapid::RapidStartGroup group{&state,{0,2}};
  Blocker helper(pool);const auto owner=std::this_thread::get_id();
  EXPECT_EQ(helper.worker,1u);EXPECT_NE(helper.thread,owner);
  GrownResult result;unsigned caught=0;
  auto body=[&](size_t index) {
    if(closed.load())after_return=true;
    const size_t offset=index-begin;
    if(index<begin || offset>=count){ADD_FAILURE()<<"index outside grown range";return;}
    struct Guard{std::atomic<unsigned>&active;Guard(std::atomic<unsigned>&a):active(a){++active;}~Guard(){--active;}} guard(active);
    ++visits[offset];values[offset].fetch_add(static_cast<std::uint64_t>(index)*31+11);
    const bool foreign=std::this_thread::get_id()!=owner;
    if(foreign && offset==3)running_moved=true;
    if(foreign && inside.load()) {
      if(offset>=4 && offset<=6)private_mask.fetch_or(1u<<(offset-4));
      if(offset>=7)foreign_tail=true;
    }
    if(offset==3) {
      // With grain1 and N65, the actual owner has grown [0,1),[1,3),
      // [3,7). Only callback3 is running;4..6 are pending private work.
      inside=true;
      FastLeaf(pool,group,path);
      if(repeated) {
        try {FastLeaf(pool,group,path,true);}catch(const std::runtime_error &){++caught;}
        FastLeaf(pool,group,path);
      }
      helper.release=true; // late helper: availability is after nested entry
      result.progress=Until([&]{return private_mask.load()==7 && foreign_tail.load();});
      result.mask=private_mask.load();result.tail=foreign_tail.load();
      if(repeated)FastLeaf(pool,group,path);
      inside=false;
      if(owner_throw)throw std::runtime_error("owner callback after pending trim");
    }
  };
  if(owner_throw)EXPECT_THROW(Published(pool,body,begin,begin+count,1),std::runtime_error);
  else Published(pool,body,begin,begin+count,1);
  closed=true;
  EXPECT_TRUE(result.progress);EXPECT_EQ(result.mask,7u);EXPECT_TRUE(result.tail);
  EXPECT_FALSE(running_moved.load());EXPECT_FALSE(after_return.load());EXPECT_EQ(active.load(),0u);
  if(repeated)EXPECT_EQ(caught,1u);
  for(size_t i=0;i<count;++i) {
    const unsigned actual=visits[i].load();
    if(owner_throw)EXPECT_LE(actual,1u);else EXPECT_EQ(actual,1u);
    EXPECT_EQ(values[i].load(),actual*(static_cast<std::uint64_t>(begin+i)*31+11));
  }
  std::array<std::atomic<unsigned>,19> reuse{};
  auto again=[&](size_t i){++reuse[i];};ParallelFor(pool,0,reuse.size(),again);
  for(auto &value:reuse)EXPECT_EQ(value.load(),1u);
  return result;
}

TEST(EigenNestedPending, GrownBatchReleasesFourThroughSixBeforeRunningThreeReturns) {
  for(auto path:{Inner::Generic,Inner::Named}) {
    const auto result=Grown(path);
    std::cout<<"{\"event\":\"pending_gate\",\"path\":\""<<(path==Inner::Named?"named_auto":"generic")
      <<"\",\"reserved_begin\":3,\"reserved_end\":7,\"foreign_pending_mask\":"<<result.mask
      <<",\"foreign_unreserved_tail\":"<<(result.tail?"true":"false")
      <<",\"progress_before_callback_return\":"<<(result.progress?"true":"false")<<"}\n";
  }
}
TEST(EigenNestedPending, RepeatedEntriesCaughtInnerThrowAndReturnRaceAreExactOnce) {
  for(unsigned round=0;round<24;++round) {
    SCOPED_TRACE(round);Grown(round%2?Inner::Named:Inner::Generic,true,false,round%3==0);
  }
}
TEST(EigenNestedPending, OwnerThrowAfterTrimJoinsReleasedCallbacksAndAllowsReuse) {
  Grown(Inner::Named,true,true);Grown(Inner::Generic,true,true,true);
}

TEST(EigenNestedPending, FastLeafNotifiesWaitingHelperForWholeTerminalTail) {
  for(size_t grain:{1u,4u})for(bool near_max:{false,true}) {
    const size_t begin=near_max?std::numeric_limits<size_t>::max()-2:0;
    std::array<std::atomic<unsigned>,2> visits{};
    std::atomic<bool> foreign{false},moved_running{false};
    ThreadPool pool(2,false,true,WorkerIdleMode::ResidentBusy);pool.SetResidentLimit(0);
    rapid::RapidDomainState state(pool);rapid::RapidStartGroup group{&state,{0,2}};
    Blocker helper(pool);
    const auto owner=std::this_thread::get_id();bool parked=false,progress=false;
    EXPECT_EQ(helper.worker,1u);EXPECT_NE(helper.thread,owner);
    auto body=[&](size_t index) {
      const size_t offset=index-begin;
      if(index<begin || offset>=2){ADD_FAILURE()<<"terminal range index outside oracle";return;}
      ++visits[offset];
      if(std::this_thread::get_id()!=owner) {
        if(offset==0)moved_running=true;else foreign=true;
      }
      if(offset==0) {
        // Block theft before the first reservation. Once released, the
        // helper sees only an empty/<=grain tail and registers its wait again.
        helper.release=true;
        parked=Until([&]{return pool.HasWaitingHelper();});
        EXPECT_FALSE(foreign.load());
        FastLeaf(pool,group,Inner::Named);
        progress=Until([&]{return foreign.load();});
      }
    };
    Published(pool,body,begin,begin+2,grain);
    std::cout<<"{\"event\":\"terminal_gate\",\"grain\":"<<grain
      <<",\"near_size_max\":"<<(near_max?"true":"false")<<",\"waiter_registered_before_nested\":"<<(parked?"true":"false")
      <<",\"foreign_before_running_callback_return\":"<<(progress?"true":"false")<<"}\n";
    EXPECT_TRUE(parked);EXPECT_TRUE(progress);EXPECT_FALSE(moved_running.load());
    EXPECT_EQ(visits[0].load(),1u);EXPECT_EQ(visits[1].load(),1u);
  }
}

TEST(EigenNestedPending, DelayedWholeTerminalTaskJoinsOrRejectsExpiredCallbackAfterCancel) {
  for(bool cancel:{false,true}) {
    std::array<std::atomic<unsigned>,2> visits{};
    std::atomic<bool> acquired{false},release{false},left{false},finished{false},expired{false},after_return{false};
    std::atomic<bool> early{false},returned_before_admission{false};
    auto workers=std::make_unique<ThreadPool>(2,true,true,WorkerIdleMode::ResidentBusy);
    auto &pool=*workers;pool.SetResidentLimit(0);
    rapid::RapidDomainState state(pool);rapid::RapidStartGroup group{&state,{0,2}};
    Blocker helper(pool);const auto owner=std::this_thread::get_id();bool reached=false;
    pending_observer::acquired=[&] {
      if(std::this_thread::get_id()==owner || acquired.exchange(true))return;
      if(!Until([&]{return release.load();},5s))eigen_test_support::FailWait("terminal acquired task admission");
    };
    std::thread controller([&] {
      if(Until([&]{return left.load();},5s)) {
        if(cancel)returned_before_admission=Until([&]{return finished.load();},2s);
        else {for(unsigned i=0;i<10000;++i)std::this_thread::yield();early=finished.load();}
      }
      release=true;
    });
    const auto call_with_stack_callback=[&] {
      auto body=[&](size_t index) {
        if(index>=2){ADD_FAILURE()<<"delayed terminal index outside oracle";return;}
        if(expired.load())after_return=true;++visits[index];
        if(index==0) {
          FastLeaf(pool,group,Inner::Named);helper.release=true;
          reached=Until([&]{return acquired.load();});
          if(cancel)pool.Cancel();left=true;
        }
      };
      Published(pool,body,0,2,4);
    };
    call_with_stack_callback();expired=true;finished=true;controller.join();
    workers.reset();pending_observer::acquired={};
    EXPECT_TRUE(reached);EXPECT_TRUE(acquired.load());EXPECT_FALSE(early.load());EXPECT_FALSE(after_return.load());
    EXPECT_EQ(visits[0].load(),1u);EXPECT_EQ(visits[1].load(),cancel?0u:1u);
    if(cancel)EXPECT_TRUE(returned_before_admission.load());
  }
}
} // namespace
int main(int argc,char **argv){testing::InitGoogleTest(&argc,argv);return RUN_ALL_TESTS();}
