// SPDX-License-Identifier: Apache-2.0
// Diagnostic-only TU: existing trace counters prove that the real idle worker
// attempted the exhausted top range while the owner was inside its callback.
#define OOX_SCHEDULER_TRACE 1
#include <atomic>
#include <functional>
namespace ancestor_observer { inline std::function<void()> acquired; }
#define OOX_EIGEN_TEST_LAZY_TASK_ACQUIRED() \
  do { if (ancestor_observer::acquired) ancestor_observer::acquired(); } while (false)
#include <oox/eigen/rapid_auto.h>
#include "eigen_test_wait.h"
#include <gtest/gtest.h>
#include <array>
#include <chrono>
#include <memory>
#include <iostream>
#include <stdexcept>

namespace ancestor_trace {
inline std::atomic<bool> armed{false};
inline std::atomic<unsigned> empty{0}, grain{0};
inline std::atomic<std::uint64_t> next{1};
inline thread_local int worker=-1;
void Reset() { armed=false;empty=0;grain=0; }
}
namespace oox::scheduler_trace {
Id NextId() noexcept { return ancestor_trace::next.fetch_add(1); }
Id Current() noexcept { return 0; }
void Worker(int worker) noexcept { ancestor_trace::worker=worker; }
void Count(Counter counter,std::uint64_t amount) noexcept {
  if (!ancestor_trace::armed.load() || ancestor_trace::worker!=1) return;
  if (counter==Counter::RangeRejectedEmpty) ancestor_trace::empty.fetch_add(amount);
  if (counter==Counter::RangeRejectedGrain) ancestor_trace::grain.fetch_add(amount);
}
bool Emit(const char *,Id,Id,Id,Id,char) noexcept { return false; }
Scope::Scope(const char *kind,Id id,Id,Id) noexcept
    :kind_(kind),id_(id),previous_(0),recorded_(false) {}
Scope::~Scope()=default;
}

namespace {
using namespace oox::detail::eigen_pool;
namespace pd=partitioner_detail;
using namespace std::chrono_literals;

template<class Predicate>
bool Bounded(Predicate ready,std::chrono::steady_clock::duration timeout=2s) {
  const auto end=std::chrono::steady_clock::now()+timeout;
  while (!ready()) {
    if (std::chrono::steady_clock::now()>=end) return false;
    std::this_thread::yield();
  }
  return true;
}

struct SentinelTask final : Task {
  void operator()() override {}
  void Discard() noexcept override {}
};
struct Candidate : StealableRange {
  Task *Steal() noexcept override { ++calls;return offer; }
  Task *offer=nullptr;
  std::atomic<unsigned> calls{0};
};

TEST(EigenNestedAncestor, FivePublishedRangesPreferTopThenRestoreEveryParent) {
  std::atomic<uintptr_t> slot{0};
  SentinelTask task;
  std::array<Candidate,5> ranges;
  for (auto &range:ranges) range.Publish(slot);
  ranges.front().offer=&task;
  EXPECT_EQ(StealableRange::TrySteal(slot),&task);
  for (auto &range:ranges) EXPECT_EQ(range.calls.load(),1u);
  ranges.back().offer=&task;
  EXPECT_EQ(StealableRange::TrySteal(slot),&task);
  EXPECT_EQ(ranges.front().calls.load(),1u); // deepest work has priority
  EXPECT_EQ(ranges.back().calls.load(),2u);
  for (size_t depth=5;depth!=0;--depth) {
    ranges[depth-1].Unpublish();
    if (depth!=1) EXPECT_EQ(StealableRange::TrySteal(slot),&task);
  }
  EXPECT_EQ(slot.load(),0u);
  EXPECT_EQ(StealableRange::TrySteal(slot),nullptr);
}

TEST(EigenNestedAncestor, ClosingTopJoinsAThiefPinnedInAnAncestor) {
  std::atomic<uintptr_t> slot{0};
  std::atomic<bool> entered{false},release{false},closing{false},closed{false},timed_out{false};
  SentinelTask task;
  struct Held final : StealableRange {
    Held(Task &task,std::atomic<bool> &entered,std::atomic<bool> &release,std::atomic<bool> &timed_out)
        :task(task),entered(entered),release(release),timed_out(timed_out) {}
    Task *Steal() noexcept override {
      entered=true;
      if (!Bounded([&]{return release.load();},5s)) timed_out=true;
      return &task;
    }
    Task &task;std::atomic<bool> &entered,&release,&timed_out;
  } outer(task,entered,release,timed_out);
  std::array<Candidate,4> inner;
  outer.Publish(slot);for(auto &range:inner)range.Publish(slot);
  Task *stolen=nullptr;
  std::thread thief([&]{stolen=StealableRange::TrySteal(slot);});
  const bool reached=Bounded([&]{return entered.load();});
  EXPECT_TRUE(reached);
  if (reached) {
    std::thread closer([&]{closing=true;inner.back().Unpublish();closed=true;});
    eigen_test_support::WaitUntil([&]{return closing.load();},"begin top close");
    for(unsigned i=0;i<1000;++i)std::this_thread::yield();
    EXPECT_FALSE(closed.load());
    release=true;closer.join();
  } else { release=true;inner.back().Unpublish(); }
  thief.join();
  EXPECT_EQ(stolen,&task);EXPECT_FALSE(timed_out.load());
  for(size_t depth=3;depth!=0;--depth)inner[depth-1].Unpublish();
  outer.Unpublish();EXPECT_EQ(slot.load(),0u);
}

// This helper uses real ActiveRange/Branch/Region ownership, but controls
// publication without eager prefix tasks. Its inline owner does not emulate
// Process/WorkLease admission; donated Work uses the real admission protocol.
// Every index zero stays on the owner, so five stack ranges coexist.
template<class F>
void Active(ThreadPool &pool,F &body,size_t begin,size_t end,size_t grain,
            std::atomic<unsigned> *published_count=nullptr) {
  using State=partitioning::LazyAutoPartitionState;
  using Work=pd::Work<F,State>;
  auto *region=NewSmallObject<pd::Region>(pool,nullptr,true);
  {
    pd::Branch<Work,true> branch(nullptr);
    pd::ActiveRange<F,State> range(*region,&body,{begin,end,grain},State(pool.NumThreads()),branch);
    const bool published=pool.PublishStealableRange(range);
    EXPECT_TRUE(published);
    if(published) {
      if(published_count)++*published_count;
      size_t chunks=0;
      try { range.Run(chunks); }
      catch (...) { region->Fail(std::current_exception()); }
      range.Close();
      if(published_count)--*published_count;
    }
  }
  pool.Wait([&]{return region->IsComplete();});
  region->CloseAndWait();
  std::exception_ptr error;
  try { region->Rethrow(); } catch (...) { error=std::current_exception(); }
  region->TaskComplete();
  if(error)std::rethrow_exception(error);
}

struct ParkHelper {
  explicit ParkHelper(ThreadPool &pool):pool(pool) {
    pool.RunOnThread(MakeTask([this]{
      worker=this->pool.CurrentThreadId();os_thread=std::this_thread::get_id();started=true;
      if(!Bounded([&]{return release.load();},10s)) eigen_test_support::FailWait("release ordinary helper blocker");
      exited=true;
    }),1);
    eigen_test_support::WaitUntil([&]{return started.load();},"helper blocker started");
  }
  ~ParkHelper() { release=true; }
  ThreadPool &pool;
  std::atomic<bool> started{false},release{false},exited{false};
  size_t worker=size_t(-1);
  std::thread::id os_thread;
};

struct Hierarchy {
  static constexpr unsigned depth=5,width=65;
  explicit Hierarchy(ThreadPool &pool,ParkHelper &helper):pool(pool),helper(helper),owner(std::this_thread::get_id()) {}
  void Run(unsigned level=0) {
    const size_t count=level+1==depth?2:width;
    auto body=[&,level](size_t index) {
      if(returned[level].load())after_return=true;
      visits[level][index].fetch_add(1);
      values[level][index].fetch_add((level+1)*1009+index*17+3);
      if(std::this_thread::get_id()!=owner && inside.load()) {
        foreign.fetch_or(1u<<level);
        if(throw_on_foreign.exchange(false)) {
          thrown=true;throw std::runtime_error("stolen ancestor callback");
        }
      }
      if(index==0 && level+1!=depth)Run(level+1);
      if(index==1 && level+1==depth) {
        published_at_gate=published_count.load();
        inside=true;ancestor_trace::armed=true;helper.release=true;
        if(delayed) {
          progress=Bounded([&]{return acquired.load();});
          left_inner=true;
          if(cancel)pool.Cancel();
        } else {
          progress=Bounded([&]{return throw_case?thrown.load():foreign.load()==15;});
        }
        empty_attempts=ancestor_trace::empty.load();
        ancestor_trace::armed=false;inside=false;
      }
    };
    try { Active(pool,body,0,count,1,&published_count); }
    catch (...) { returned[level]=true;throw; }
    returned[level]=true;
  }
  ThreadPool &pool;ParkHelper &helper;const std::thread::id owner;
  std::array<std::array<std::atomic<unsigned>,width>,depth> visits{},values{};
  std::array<std::atomic<bool>,depth> returned{};
  std::atomic<unsigned> foreign{0};
  std::atomic<unsigned> published_count{0};
  std::atomic<bool> inside{false},after_return{false},acquired{false},left_inner{false};
  std::atomic<bool> throw_on_foreign{false},thrown{false};
  bool delayed=false,cancel=false,progress=false,throw_case=false;
  unsigned empty_attempts=0,published_at_gate=0;
};

TEST(EigenNestedAncestor, FiveActiveLevelsExposeAllUnreservedAncestorTails) {
  ancestor_trace::Reset();
  ThreadPool pool(2,true,true,WorkerIdleMode::ResidentBusy);
  ParkHelper helper(pool);
  EXPECT_EQ(pool.CurrentThreadId(),0u);EXPECT_EQ(helper.worker,1u);
  EXPECT_NE(helper.os_thread,std::this_thread::get_id());
  Hierarchy run(pool,helper);run.Run();
  std::cout<<"{\"event\":\"ancestor_gate\",\"published_levels\":"<<run.published_at_gate
    <<",\"helper_worker\":"<<helper.worker<<",\"foreign_ancestor_mask\":"<<run.foreign.load()
    <<",\"top_empty_rejections\":"<<run.empty_attempts
    <<",\"progress_before_inner_release\":"<<(run.progress?"true":"false")<<"}\n";
  EXPECT_EQ(run.published_at_gate,5u);EXPECT_EQ(run.published_count.load(),0u);
  EXPECT_TRUE(run.progress) << "helper attempted the exhausted deepest range but could not reach ancestors";
  EXPECT_GT(run.empty_attempts,0u);EXPECT_EQ(run.foreign.load(),15u);
  EXPECT_FALSE(run.after_return.load());
  for(unsigned level=0;level<Hierarchy::depth;++level)
    for(unsigned index=0;index<(level+1==Hierarchy::depth?2:Hierarchy::width);++index) {
      EXPECT_EQ(run.visits[level][index].load(),1u)<<level<<':'<<index;
      EXPECT_EQ(run.values[level][index].load(),(level+1)*1009+index*17+3)<<level<<':'<<index;
    }
  // A fresh publication on the same registered slot proves all five popped.
  std::array<std::atomic<unsigned>,17> reused{};
  auto body=[&](size_t i){++reused[i];};Active(pool,body,0,reused.size(),1);
  for(auto &value:reused)EXPECT_EQ(value.load(),1u);
}

TEST(EigenNestedAncestor, PrivateBatchTwoThroughFourStaysOnOwnerAndGrainIsSeparate) {
  for(bool grain_hidden:{false,true}) {
    SCOPED_TRACE(grain_hidden?"unreserved remainder <=grain":"stealable tail beyond private [2,5)");
    ancestor_trace::Reset();
    ThreadPool pool(2,true,true,WorkerIdleMode::ResidentBusy);
    ParkHelper helper(pool);
    const auto owner=std::this_thread::get_id();
    const size_t begin=2,count=grain_hidden?6:65;
    std::array<std::atomic<unsigned>,65> visits{},values{};
    std::atomic<bool> foreign_tail{false},reserved_moved{false};
    bool reached=false;
    auto body=[&](size_t index) {
      ++visits[index-begin];values[index-begin].fetch_add(unsigned(index*19+7));
      const bool foreign=std::this_thread::get_id()!=owner;
      if(foreign && index<=4)reserved_moved=true;
      if(foreign && index>=5)foreign_tail=true;
      if(index==begin) {
        ancestor_trace::armed=true;helper.release=true;
        reached=Bounded([&]{return grain_hidden?ancestor_trace::grain.load()>0:foreign_tail.load();});
        EXPECT_EQ(visits[1].load(),0u);EXPECT_EQ(visits[2].load(),0u);
        ancestor_trace::armed=false;
      }
    };
    Active(pool,body,begin,begin+count,3);
    EXPECT_TRUE(reached);EXPECT_FALSE(reserved_moved.load());
    EXPECT_EQ(foreign_tail.load(),!grain_hidden);
    for(size_t i=0;i<count;++i) {
      EXPECT_EQ(visits[i].load(),1u);EXPECT_EQ(values[i].load(),unsigned((begin+i)*19+7));
    }
  }
}

TEST(EigenNestedAncestor, DelayedAncestorAdmissionJoinsBeforeCallbackFrameReturns) {
  for(bool cancel:{false,true}) {
    SCOPED_TRACE(cancel?"cancel before acquired ancestor admission":"normal delayed ancestor admission");
    ancestor_trace::Reset();
    auto workers=std::make_unique<ThreadPool>(2,true,true,WorkerIdleMode::ResidentBusy);
    auto &pool=*workers;
    ParkHelper helper(pool);Hierarchy run(pool,helper);run.delayed=true;run.cancel=cancel;
    std::atomic<bool> release{false},finished{false},early_return{false},returned_before_admission{false};
    struct Reset { ~Reset(){ancestor_observer::acquired={};} } reset;
    ancestor_observer::acquired=[&]{
      if(std::this_thread::get_id()==run.owner || run.acquired.exchange(true))return;
      if(!Bounded([&]{return release.load();},5s))eigen_test_support::FailWait("delayed ancestor task admission");
    };
    std::thread controller([&]{
      if(!Bounded([&]{return run.left_inner.load();},5s)) {
        release=true;return;
      }
      if(cancel) {
        // Cancellation may close admission and return with an acquired but
        // not admitted task still retained. Force that ordering, then let the
        // real WorkLease reject the now-expired callback frame.
        returned_before_admission=Bounded([&]{return finished.load();},2s);
      } else {
        for(unsigned i=0;i<10000;++i)std::this_thread::yield();
        early_return=finished.load();
      }
      release=true;
    });
    run.Run();finished=true;controller.join();
    if(cancel)EXPECT_TRUE(pool.IsCancelled());
    else {
      unsigned sum=0;auto body=[&](size_t){++sum;};Active(pool,body,0,1,1);EXPECT_EQ(sum,1u);
    }
    // Join the actual worker before releasing the hook, captured flags or run
    // storage. In cancelled mode Region completion alone need not join it.
    workers.reset();ancestor_observer::acquired={};
    EXPECT_TRUE(run.progress);EXPECT_TRUE(run.acquired.load());EXPECT_FALSE(early_return.load());
    EXPECT_FALSE(run.after_return.load());
    for(auto &value:run.returned)EXPECT_TRUE(value.load());
    if(cancel) {
      EXPECT_TRUE(returned_before_admission.load());
      for(unsigned level=0;level+1<Hierarchy::depth;++level)
        for(unsigned index=1;index<Hierarchy::width;++index)EXPECT_EQ(run.visits[level][index].load(),0u);
    }
  }
}

TEST(EigenNestedAncestor, StolenAncestorExceptionUnwindsAllFiveFramesAndAllowsReuse) {
  ancestor_trace::Reset();
  ThreadPool pool(2,true,true,WorkerIdleMode::ResidentBusy);
  ParkHelper helper(pool);Hierarchy run(pool,helper);run.throw_on_foreign=true;run.throw_case=true;
  EXPECT_THROW(run.Run(),std::runtime_error);
  EXPECT_TRUE(run.progress);EXPECT_TRUE(run.thrown.load());EXPECT_GT(run.empty_attempts,0u);
  EXPECT_FALSE(run.after_return.load());
  for(auto &value:run.returned)EXPECT_TRUE(value.load());
  for(unsigned level=0;level<Hierarchy::depth;++level)
    for(unsigned index=0;index<(level+1==Hierarchy::depth?2:Hierarchy::width);++index) {
      const unsigned count=run.visits[level][index].load();
      EXPECT_LE(count,1u);
      EXPECT_EQ(run.values[level][index].load(),count*((level+1)*1009+index*17+3));
    }
  std::array<std::atomic<unsigned>,37> visits{};
  auto body=[&](size_t i){++visits[i];};Active(pool,body,0,visits.size(),1);
  for(auto &value:visits)EXPECT_EQ(value.load(),1u);
}

TEST(EigenNestedAncestor, CancelledDivisibleTopDoesNotHideAnotherLiveRegion) {
  ThreadPool pool(1,false,true,WorkerIdleMode::ResidentBusy);
  using State=partitioning::LazyAutoPartitionState;
  std::array<std::atomic<unsigned>,17> visits{};
  auto outer_body=[&](size_t i){++visits[i];};
  auto inner_body=[&](size_t){ADD_FAILURE()<<"cancelled inner callback admitted";};
  using Outer=pd::Work<decltype(outer_body),State>;
  using Inner=pd::Work<decltype(inner_body),State>;
  auto *outer_region=NewSmallObject<pd::Region>(pool,nullptr,true);
  auto *inner_region=NewSmallObject<pd::Region>(pool,nullptr,true);
  std::atomic<uintptr_t> slot{0};
  {
    pd::Branch<Outer,true> outer_branch(nullptr);
    pd::Branch<Inner,true> inner_branch(nullptr);
    pd::ActiveRange<decltype(outer_body),State> outer(*outer_region,&outer_body,{0,17,1},State(1),outer_branch);
    pd::ActiveRange<decltype(inner_body),State> inner(*inner_region,&inner_body,{0,17,1},State(1),inner_branch);
    outer.Publish(slot);inner.Publish(slot);
    inner_region->Fail(std::make_exception_ptr(std::runtime_error("inner cancelled")));
    Task *stolen=StealableRange::TrySteal(slot);
    EXPECT_NE(stolen,nullptr);
    if(stolen)(*stolen)();
    inner.Close();
    size_t chunks=0;outer.Run(chunks);outer.Close();
  }
  for(auto *region:{outer_region,inner_region}) {
    pool.Wait([&]{return region->IsComplete();});region->CloseAndWait();region->TaskComplete();
  }
  EXPECT_EQ(slot.load(),0u);
  for(auto &value:visits)EXPECT_EQ(value.load(),1u);
}

TEST(EigenNestedAncestor, NamedAutoMixedDepthsMatchSerialAndRestoreAfterThrow) {
  using namespace rapid;
  for(unsigned workers:{1u,2u,4u})for(unsigned depth:{1u,3u,5u}) {
    SCOPED_TRACE(::testing::Message()<<"workers="<<workers<<" depth="<<depth);
    ThreadPool pool(workers,true,true,WorkerIdleMode::ResidentBusy);
    RapidDomainState state(pool);RapidStartGroup group{&state,{0,workers}};
    size_t leaves=1;for(unsigned i=0;i<depth;++i)leaves*=3;
    std::vector<std::atomic<unsigned>> visits(leaves),values(leaves);
    std::function<void(unsigned,size_t)> walk;
    walk=[&](unsigned level,size_t prefix) {
      ParallelForAuto(group,0,3,[&](size_t i){
        const size_t index=prefix*3+i;
        if(level+1==depth) {++visits[index];values[index].fetch_add(unsigned(index*31+11));}
        else walk(level+1,index);
      });
    };
    pool.RunInlineWork([&]{walk(0,0);});
    for(size_t i=0;i<leaves;++i) {EXPECT_EQ(visits[i].load(),1u);EXPECT_EQ(values[i].load(),unsigned(i*31+11));}
    EXPECT_FALSE(pool.IsExecutingTask());EXPECT_EQ(CurrentResidentState(),nullptr);
    std::atomic<unsigned> active{0};
    std::function<void(unsigned)> fail;
    fail=[&](unsigned level) {
      ParallelForAuto(group,0,17,[&](size_t i){
        struct Guard { std::atomic<unsigned> &active;Guard(std::atomic<unsigned> &a):active(a){++active;}~Guard(){--active;} } guard(active);
        if(i==0) {
          if(level+1==depth)throw std::runtime_error("nested ancestor throw");
          fail(level+1);
        }
      });
    };
    EXPECT_THROW(pool.RunInlineWork([&]{fail(0);}),std::runtime_error);
    EXPECT_EQ(active.load(),0u);EXPECT_FALSE(pool.IsExecutingTask());EXPECT_EQ(CurrentResidentState(),nullptr);
    std::array<std::atomic<unsigned>,31> reuse{};
    ParallelForAuto(group,0,reuse.size(),[&](size_t i){++reuse[i];});
    for(auto &value:reuse)EXPECT_EQ(value.load(),1u);
  }
}
} // namespace

int main(int argc,char **argv) {
  testing::InitGoogleTest(&argc,argv);return RUN_ALL_TESTS();
}
