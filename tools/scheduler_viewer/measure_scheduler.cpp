// SPDX-License-Identifier: Apache-2.0
// Non-instrumented, warm-call measurements. No trace counters in timed bodies.
#include "common.h"
#include "oox/eigen/run_queue.h"
#include <atomic>
#include <iomanip>
#include <memory>

struct Case { const char *name; std::size_t n, work; int pattern; };
constexpr Case cases[] = {
    {"loop64",64,0,0}, {"loop4096",4096,0,0},
    {"balanced64_100",64,100,0}, {"balanced64_1000",64,1000,0},
    {"tail64_100",64,100,1}, {"tail64_1000",64,1000,1},
    {"cluster64_1000",64,1000,2}, {"single64_1000",64,1000,3},
    {"balanced4096_100",4096,100,0}, {"tail4096_100",4096,100,1}};

template <typename F> void RunMeasured(std::size_t n, F &&f) {
#if defined(TBB_MODE) && TBB_MODE == TBB_AUTO
  tbb::parallel_for(tbb::blocked_range<std::size_t>(0,n,2),
      [&](const auto &range) { for(auto i=range.begin();i<range.end();++i) f(i); },
      tbb::auto_partitioner{});
#else
  ParallelFor(0,n,std::forward<F>(f),2);
#endif
}

std::size_t Units(const Case &c,std::size_t i) {
  if(c.pattern==1 && i>=c.n*3/4) return c.work*16;
  if(c.pattern==2 && ((i>=c.n/8&&i<c.n/4)||(i>=c.n*5/8&&i<c.n*3/4))) return c.work*6;
  if(c.pattern==3 && i==c.n-1) return c.work*32;
  return c.work;
}

std::uint64_t Arithmetic(std::size_t i,std::size_t units) {
  std::uint64_t x=i+1;
  // Keep the deterministic input opaque to constant folding on every compiler.
  asm volatile("" : "+r"(x));
  for(std::size_t j=0;j<units;++j) {
    x+=UINT64_C(0x9e3779b97f4a7c15);
    x=(x^(x>>30))*UINT64_C(0xbf58476d1ce4e5b9);
    x=(x^(x>>27))*UINT64_C(0x94d049bb133111eb);
    x^=x>>31;
  }
  return x;
}

template <typename F> double BatchNs(F &&body, std::size_t count) {
  const auto begin=std::chrono::steady_clock::now();
  for(std::size_t i=0;i<count;++i) body(i);
  return std::chrono::duration<double,std::nano>(std::chrono::steady_clock::now()-begin).count()/count;
}

void Primitives() {
  using Queue=oox::detail::eigen_pool::RunQueue<std::uintptr_t,1024>;
  Queue q;
  std::atomic<std::size_t> head{0};
  constexpr std::size_t rounds=4000,batch=256;
  double push=0,pop=0,back=0;
  for(std::size_t r=0;r<rounds;++r) {
    push+=BatchNs([&](std::size_t i){if(!q.PushFront(i+1))std::abort();},batch);
    pop+=BatchNs([&](std::size_t){if(!q.PopFront())std::abort();},batch);
    for(std::size_t i=0;i<batch;++i)if(!q.PushFront(i+1))std::abort();
    back+=BatchNs([&](std::size_t){if(!q.PopBack())std::abort();},batch);
  }
  const auto empty=BatchNs([&](std::size_t){if(q.PopBack())std::abort();},rounds*batch);
  const auto reserve=BatchNs([&](std::size_t i){
    head.store(i,std::memory_order_seq_cst);
    auto observed=head.load(std::memory_order_acquire);
    // Escape the local atomic to the compiler: otherwise Clang may eliminate
    // an unobserved, provably thread-local atomic loop entirely.
    asm volatile("" : "+r"(observed) : "r"(&head) : "memory");
  },rounds*batch);
  std::cout<<"\"primitives_ns\":{\"push_front_hot\":"<<push/rounds
           <<",\"pop_front_hot\":"<<pop/rounds<<",\"pop_back_uncontended_same_thread\":"<<back/rounds
           <<",\"empty_pop_back\":"<<empty<<",\"sc_store_acquire_load\":"<<reserve<<"},";
}

int main(int argc,char **argv) {
  try {
    using namespace scheduler_eval;
    const auto minimum=Argument(argc,argv,"--min-ms",40);
    if(minimum<5||minimum>1000)throw std::invalid_argument("min-ms must be 5..1000");
    std::string kernel="cpu_relax";
    for(int i=1;i<argc;++i)if(std::string_view(argv[i])=="--kernel") {
      if(++i==argc)throw std::invalid_argument("missing kernel");
      kernel=argv[i];
    }
    if(kernel!="cpu_relax"&&kernel!="arithmetic_v1")throw std::invalid_argument("unknown kernel");
    if(Arithmetic(0,0)!=UINT64_C(0x1)||Arithmetic(0,1)!=UINT64_C(0x910a2dec89025cc1)||
       Arithmetic(1,2)!=UINT64_C(0x64684c4f0fd784b4)||Arithmetic(7,100)!=UINT64_C(0x15baf02e43887e81))
      throw std::runtime_error("arithmetic reference vectors failed");
    const bool arithmetic=kernel=="arithmetic_v1";
    Initialize();
    std::cout<<std::setprecision(12)<<"{\"mode\":\""<<ModeName()<<"\",\"workers\":"<<ExecutionThreads()
             <<",\"instrumented\":false,\"grain\":2,\"suite_version\":2,\"kernel\":\""<<kernel<<"\",";
#ifdef SERIAL
    Primitives();
#endif
    std::cout<<"\"cases\":[";
    bool first=true;
    for(const auto &c:cases) {
      auto visits=std::make_unique<std::atomic<unsigned>[]>(c.n);
      RunMeasured(c.n,[&](std::size_t i){visits[i].fetch_add(1,std::memory_order_relaxed);});
      for(std::size_t i=0;i<c.n;++i)if(visits[i].load()!=1)throw std::runtime_error("visitation oracle failed");
      const auto run=[&]{
        if(arithmetic)RunMeasured(c.n,[&](std::size_t i){
          auto value=Arithmetic(i,Units(c,i));
          asm volatile("" : "+r"(value) : : "memory");
        });
        else RunMeasured(c.n,[&](std::size_t i){for(std::size_t j=0,units=Units(c,i);j<units;++j)CpuRelax();});
      };
      for(int i=0;i<64;++i)run();
      const auto begin=std::chrono::steady_clock::now();
      std::size_t calls=0;
      double elapsed=0;
      do {
        for(int i=0;i<16;++i)run();
        calls+=16;
        elapsed=std::chrono::duration<double,std::nano>(std::chrono::steady_clock::now()-begin).count();
      } while(elapsed<minimum*1e6);
      if(!first)std::cout<<',';first=false;
      std::cout<<"{\"case\":\""<<c.name<<"\",\"n\":"<<c.n<<",\"work\":"<<c.work
               <<",\"pattern\":"<<c.pattern<<",\"calls\":"<<calls<<",\"ns\":"<<elapsed/calls<<'}';
    }
    std::cout<<"],\"oracle_ok\":true}\n";
  } catch(const std::exception &e){std::cerr<<e.what()<<'\n';return 1;}
}
