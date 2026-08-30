# Estimating Rapid Start and choosing scheduler parameters

This document connects published scheduling models to the scheduler policies and
measurements in this directory. The objective is not to attach a smooth curve to
every result. It is to make each major source of time explicit, fit only terms the
current experiment can identify, and expose the measurements still needed for a
causal model.

The companion `model.py` is the first executable version of that model. Its
coefficients are diagnostics for one result directory, not universal properties of
a scheduler. This article also separates what earlier sources actually established
from the estimation and tuning methodology proposed for OOX.

## Research lineage and terminology

The historical sources use several names for related ideas. They must not be cited
as though they were one published algorithm:

| Source | What it contributes | Publication status |
| --- | --- | --- |
| Malakhov and Fiksman, [*Pushing the limits of work-stealing*](https://community.intel.com/legacyfs/online/drupal_files/managed/9d/48/ConfAnton-Pushing-the-limits-of-work-stealing-approved.pdf) ([readable mirror](https://manuals.plus/m/1aedec7a8f34f492f5565f406d382747add2403573aca2a4395576213d655cef.pdf)), 2013 | Mailboxing, uneven mailbox distribution, delayed sharing, and body-based granularity motivation | Conference presentation, not a peer-reviewed article |
| [Hydra 2022: *Fusing Efficient Parallel For Loops with a Composable Task Scheduler*](https://hydraconf.com/archive/2022/talks/50468720b63246e7a1389a8c100eba72/) ([video](https://youtu.be/ZpHgf8aVAiY), [slides](https://squidex.jugru.team/api/assets/srm/2502f3b5-36bf-4e6d-88f9-8a6170b688e2/hydra22-fusing-anton-malakhov-1-.pdf)) | Presents Rapid Start group publication as the route beyond the older timespan compromise | Industry talk, no proceedings |
| [Denis Vorkozhokov's 2023 bachelor thesis](https://ctlab.itmo.ru/~vaksenov/diplomas/2023-vorkozhokov-bachelors.pdf) ([source at the revision pinned by OOX](https://github.com/lejabque/composable-parallel-scheduler-thesis/blob/f58844b4fb6f0a1394968771c1a932c8cf2c75d4/thesis/bachelor-thesis.tex#L537-L545)) | Targeted tree work sharing, delayed balancing, p99 timespan tuner, and evaluation | Bachelor thesis, in Russian |
| Vorkozhokov, Aksenov, and Malakhov, [*Hybrid work distribution for parallel programs* (pinned repository copy)](https://github.com/lejabque/composable-parallel-scheduler-thesis/blob/f58844b4fb6f0a1394968771c1a932c8cf2c75d4/paper/paper.pdf) ([hosted copy](https://ctlab.itmo.ru/~vaksenov/diplomas/2023-vorkozhokov-draft.pdf)) | Concise English description of the p99 delay and adaptive grain idea | Authored unpublished manuscript; its PL'18 template metadata is placeholder material |
| Anton Malakhov and Vitaly Aksenov, [C++ Russia 2024: *How to Fit in What Can't Be Fitted in, or Let's Parallelise in Harmony!*](https://cppconf.ru/en/archive/2024/talks/20003526/) ([slides](https://squidex.jugru.team/api/assets/srm/5aad9e85-8f42-4b35-a981-6948052a1bbc/anton-malahov-composability24.pdf)) | Later presentation of the composability and throughput motivation | Industry talk |
| [Historical prototype, pinned commit](https://github.com/lejabque/composable-parallel-scheduler-thesis/tree/f58844b4fb6f0a1394968771c1a932c8cf2c75d4) ([tuner](https://github.com/lejabque/composable-parallel-scheduler-thesis/blob/f58844b4fb6f0a1394968771c1a932c8cf2c75d4/timespan_tuner/tuner.cpp), [Rapid Start code](https://github.com/lejabque/composable-parallel-scheduler-thesis/blob/f58844b4fb6f0a1394968771c1a932c8cf2c75d4/include/rapid_start.h)) | Code for the timespan policy, tuner, benchmarks, and bitmask Rapid Start experiment | Research artifact; no license file at that revision |

The phrase *Fast work distribution for composable task scheduling engines* is a
project description/English rendering of the thesis topic, not a verified indexed
paper title with a DOI. The closest English written source is the unpublished
*Hybrid work distribution* manuscript above. The pinned copy names Denis
Vorkozhokov, Vitaly Aksenov, and Anton Malakhov as authors. Its placeholder
conference metadata must not be used as evidence that the manuscript appeared in
those proceedings.

The historical measurements in this document predate the pool-backed Rapid
Start rewrite. References to persistent trappers, one bitmask descriptor, and
the 64-worker limit describe the preserved research artifact and old result
files, not the current `RAPID_START` executable.

Two mechanisms in OOX are also distinct:

- **Rapid Start** now uses independent rapid regions, inherited worker domains,
  and a hierarchical activation/completion tree on the fixed Eigen pool.
- **Eigen sharing-stealing** is the historical hybrid method: it first distributes
  ranges through targeted mailboxes, waits for a calibrated timespan before exposing
  balancing work, and derives a grain from iterations completed during that time.

The historical p99 rule tunes the second mechanism's balancing delay. The same
measurement pattern is useful for describing Rapid Start activation latency, but
Rapid Start currently has no `INIT_TIME` parameter. This distinction is important
throughout the rest of the article.

## 1. The quantity we need to predict

For the OOX-specific equations, let:

- $P$ be the configured worker count;
- $N$ be the number of loop iterations;
- $q$ be the number of parallel-loop invocations over which a worker pool or
  Rapid Start group is reused;
- $w_i$ be a structural estimate of iteration $i$'s useful work;
- $W=\sum_i w_i$ be total useful work;
- $\kappa w_i$ be iteration $i$'s estimated service time on a reference worker;
- $s_j$ be worker $j$'s speed relative to that reference worker;
- $A_j$ be the iterations statically assigned to participating slot $j$, and
  $L_j=\sum_{i\in A_j}w_i$ its structural load;
- $S_{\text{pub}}$ be publisher-side time strictly before the descriptor or root
  task becomes visible;
- $a_j$ be the post-visibility delay until slot $j$ begins useful work; and
- $J$ be completion detection/join time.

Equations quoted from prior papers retain those papers' notation. In particular,
their $W$ is normally work measured in unit tasks or processor-time; OOX's
structural $W$ requires the fitted conversion $\kappa$ before it can be added to
time-valued costs.

Cold construction and warm invocation are different experiments:

$$
T_{\text{total},m}(q)
=I_m(P)+\sum_{r=1}^{q}T_{\text{call},m}(x_r,P).
$$

$I_m$ includes thread/pool creation, Rapid Start registration, and warm-up. A
warm microbenchmark deliberately excludes most of it. Its amortized contribution
is $I_m/q$, which is why the model writes initialization to a separate table.

For a known static assignment $A_j$, the most literal per-call model is:

$$
T_{\text{static}}
=S_{\text{pub}}+\max_j\left(a_j+\frac{\kappa L_j}{s_j}\right)+J,
$$

where $S_{\text{pub}}$ is publication and $J$ is completion detection. This is
the right starting point for Rapid Start: the publisher executes slot 0 and each
trapper present in the mask executes another slot. Every participating slot
derives one contiguous range from the descriptor, and no later balancing occurs.

## 2. What the published work-stealing models say

### Work and span

Blumofe and Leiserson represent a computation as a DAG with total work $T_1$
and longest dependence path $T_\infty$. For fully strict computations on $P$
dedicated homogeneous processors, randomized work stealing gives:

$$
\mathbb E[T_P]=\frac{T_1}{P}+O(T_\infty),
\qquad
\mathbb E[\text{steal attempts}]=O(PT_\infty).
$$

This is a powerful scalability result, but the asymptotic term hides abstract
scheduler operations and steal contention. OS wakeups, allocation policy, and
cache/coherence effects require separate measurement or modeling. A fixed
implementation has a definite execution DAG and span, but the application-level
flat loop alone does not determine them: its scheduler's task-generation tree is
part of that DAG.

Primary source: Blumofe and Leiserson, *Scheduling Multithreaded
Computations by Work Stealing*, JACM 46(5), 1999
([DOI](https://doi.org/10.1145/324133.324234),
[full paper](https://www.cs.utexas.edu/~venkatar/sys_perf_analysis/ws_theory.pdf)).

Arora, Blumofe, and Plaxton make worker availability explicit. If the runtime
owns $P$ processes but the OS supplies an average of $P_A$ processors, their
nonblocking scheduler satisfies:

$$
\mathbb E[T]
=O\left(\frac{T_1}{P_A}+T_\infty\frac{P}{P_A}\right).
$$

This distinction matters on the current Mac: 16 distinct runtime worker IDs do
not prove that 16 workers ran simultaneously, at equal speed, or on the same core
class. Arora, Blumofe, and Plaxton, *Thread Scheduling for Multiprogrammed
Multiprocessors*, Theory of Computing Systems 34, 2001
([DOI](https://doi.org/10.1007/s00224-001-0004-z),
[full paper](https://www.cs.cmu.edu/~guyb/paralg/papers/AroraBlumofePlaxton01.pdf)).

### An exact accounting identity for decentralized stealing

For independent unit tasks, Tchiboukdjian, Gast, and Trystram use:

$$
C_{\max}=\frac{W}{P}+\frac{R}{P},
$$

because every processor-time slot is either useful work or a steal request. They
track queue imbalance with the potential

$$
\Phi_t=\sum_{j=1}^{P}\left(w_j(t)-\frac{w(t)}P\right)^2.
$$

The expected potential contracts as thieves contact nonempty victims. The paper's
refined constant does not come from that quadratic potential unchanged: it uses
$\sum_j w_j^\nu$ and numerically optimizes $\nu\approx2.94$. The resulting
unit-task bound is

$$
\mathbb E[C_{\max}]
\le \frac WP+3.24\log_2W+2.59,
$$

while their weighted-task result is

$$
\mathbb E[C_{\max}]
\le \frac WP+\frac{P-1}{P}p_{\max}+3.24\log_2n+2.59.
$$

The constant 3.24 is an upper-bound constant, not a value to copy into an OOX
fit. This gives a particularly useful interpretation of OOX:

- Eigen stealing starts with highly concentrated ready work and a large
  $\Phi_0$;
- targeted mailbox sharing accelerates the early decrease of $\Phi_t$, or creates
  a smaller effective potential after its publication phase;
- Rapid Start bypasses the ready-queue ramp, but retains its static load tail;
- grain size controls both scheduler work and $p_{\max}$.

[Open full paper](https://infoscience.epfl.ch/server/api/core/bitstreams/161cea31-ba33-4ca6-ad4f-ecc3c495b0be/content),
[journal DOI](https://doi.org/10.1007/s10479-012-1149-7).

Gast, Khatiri, Trystram, and Wagner extend the model with a one-way request
latency $\lambda$. A direct request-count accounting is

$$
C_{\max}\le\frac WP+\frac{2\lambda R}{P},
$$

for the paper's discrete model, because a request occupies a processor for a
round trip (unfinished terminal activity makes this an upper bound). The final
peer-reviewed paper's finite-size theorem gives

$$
\mathbb E[C_{\max}]
\le \frac WP+16.12\lambda\log_2\!\left(\frac{W}{2\lambda}\right)
   +3\lambda,
$$

using a bounded potential-decay constant. Its conclusion also states the simpler
asymptotic form proportional to $16.12\lambda\log_2(W/\lambda)$, while simulations
fit approximately
$W/P+3.8\lambda\log_2(W/\lambda)$. The DOI below identifies the final article;
the linked 2018 preprint is supplied as the openly readable version.
In a shared-memory OOX experiment, $\lambda$ should represent per-attempt
communication/service latency—approximately half a measured request round trip—
while retries remain represented by $R$. Queue atomics and cache-coherence traffic
may contribute to that latency. Gast et al., *Analysis of Work Stealing with
latency*, JPDC 153, 2021 ([DOI](https://doi.org/10.1016/j.jpdc.2021.03.010),
[preprint](https://arxiv.org/abs/1805.00857)).

### Cache locality is a separate term

Work/span alone does not predict memory behavior. Acar, Blelloch, and Blumofe
show that, for the nested-parallel class in their cache model, the additional
misses caused by $S$ successful steals obey:

$$
M_P(C)-M_1(C)\le 2CS,
$$

where $C$ is cache capacity in blocks. They also introduce locality-guided
stealing: a task has a normal deque reference and a reference in its preferred
worker's mailbox. This is a conceptually analogous predecessor to mailbox affinity
here; the paper does not establish an implementation lineage.
OOX's foreign-mailbox consumption is a further mechanism: it may reduce idle
time, but may weaken affinity and increase the analogue of their "drifted"
execution and its cache cost.

Acar, Blelloch, and Blumofe, *The Data Locality of Work Stealing*, Theory of
Computing Systems 35(3), 2002
([journal DOI](https://doi.org/10.1007/s00224-002-1057-3),
[full paper](https://www.cs.cmu.edu/~guyb/papers/ABB02.pdf); the earlier SPAA
version has [DOI 10.1145/341800.341801](https://doi.org/10.1145/341800.341801)).

## 3. What the published work-sharing models say

The hybrid manuscript cites Eager, Lazowska, and Zahorjan's *Adaptive Load
Sharing in Homogeneous Distributed Systems* as work-sharing background
([DOI](https://doi.org/10.1109/TSE.1986.6312961),
[full paper](https://www.cs.usask.ca/faculty/eager/loadsharing.pdf)). It studies
sender/receiver policies in a distributed queueing model, not lock-free targeted
mailbox publication in shared memory, so it is useful conceptual background rather
than a cost model that can be transferred directly.

Hagerup's model is the most useful accounting framework for a parallel loop.
Introducing $B$ for the number of batches and $I_{\text{idle}}$ for total idle
processor-time gives the following OOX accounting identity when obtaining one
batch costs $h$:

$$
PT=W+Bh+I_{\text{idle}},
\qquad
T=\frac WP+\frac{Bh}{P}+\frac{I_{\text{idle}}}{P}.
$$

Large batches reduce $Bh$ but increase the terminal tail; small batches do the
opposite. Hagerup, *Allocating Independent Tasks to Parallel Processors: An
Experimental Study*, JPDC 47(2), 1997
([DOI](https://doi.org/10.1006/jpdc.1997.1411),
[full paper](https://cseweb.ucsd.edu/classes/sp99/cse160/programming/prog3/hagerup97.pdf)).

Several classical schedules instantiate the same tradeoff:

- Static block scheduling creates roughly $P$ batches and minimizes allocation
  overhead, but preserves the maximum assigned load.
- Self scheduling uses grain one. Applying Graham's list bound to jobs that OOX
  augments with cost $h$ gives the derived inequality
  $$
  T\le\frac{W+nh}{P}+(1-1/P)(w_{\max}+h).
  $$
  This augmentation is ours, not a theorem stated in Graham's paper.
  [Graham, 1966](https://doi.org/10.1002/j.1538-7305.1966.tb01709.x).
- Under IID task times, homogeneous processors, fixed allocation cost,
  known standard deviation $\sigma$, and the asymptotic conditions of large
  $n,k$ with $k\gg\ln P$, Kruskal and Weiss's fixed-size chunking analysis gives
  the core approximation
  $$
  T(k)\approx \frac{n\mu}{P}+\frac{nh}{Pk}
  +\sigma\sqrt{2k\ln P},
  $$
  whose minimizing grain is
  $$
  k_{\mathrm{FSC}}=
  \left(\frac{\sqrt2nh}{\sigma P\sqrt{\ln P}}\right)^{2/3}.
  $$
  [Kruskal and Weiss, 1985](https://doi.org/10.1109/TSE.1985.231547). OOX adds
  separate startup $S_{\text{start}}$ and join $J_{\text{join}}$ terms when
  applying this approximation to an end-to-end launch. The real-valued seed must
  be rounded and clamped and requires $P>1$ and $\sigma>0$; as $\sigma\to0$, the
  model favors the largest admissible, static-like chunk.
- Guided self scheduling chooses $k=\lceil R/P\rceil$, producing about
  $P\ln(n/P)$ allocations before the terminal region, ignoring rounding and the
  final chunks.
  [Polychronopoulos and Kuck, 1987](https://doi.org/10.1109/TC.1987.5009495).
- Simplified factoring gives $P$ chunks of $R/(2P)$ per round, halves the
  remaining work, and uses about $P\log_2(n/P)$ allocations, again ignoring
  rounding and terminal effects.
  [Hummel, Schonberg, and Flynn, 1992](https://doi.org/10.1145/135226.135232).

These stochastic formulas assume independent iteration times. The ordered
hyperbolic and triangular matrices here violate that assumption. Their exact
contiguous range loads are more informative than only $\mu$ and $\sigma$.

### Published granularity-control predecessors

The closest rigorous predecessors to "execute useful work before exposing more
parallelism" are not Rapid Start papers:

- *Lazy Task Creation* exposes latent work only when another worker needs it,
  establishing the historical idea that task creation can be deferred
  ([Mohr, Kranz, and Halstead, 1991](https://doi.org/10.1109/71.86103)).
- Cilk's work-first principle moves scheduling overhead away from the work term
  and onto the critical path
  ([Frigo, Leiserson, and Randall, 1998](https://doi.org/10.1145/277650.277725)).
- *Heartbeat Scheduling* promotes the oldest promotable parallel frame after
  every $N$ sequential abstract-machine transitions. With sequential work $w$,
  span $s$, and fixed promotion cost $\tau_h$ in the same abstract cost units, it
  proves

  $$
  W\le(1+\tau_h/N)w,
  \qquad
  S\le(1+N/\tau_h)s.
  $$

  Thus $N=k\tau_h$ bounds work inflation by $1+1/k$ while increasing the span
  term by $O(k)$ ([Acar et al., 2018](https://doi.org/10.1145/3192366.3192391),
  [full paper](https://www.chargueraud.org/research/2018/heartbeat/heartbeat.pdf)).
  Here $\tau_h$ is not Eigen's elapsed-time gate.
- *Provably and Practically Efficient Granularity Control* combines a symbolic
  cost $N$ with a learned machine constant and a sequentialization threshold
  $\kappa$. Its practical method calibrates $\kappa$ on one core and then tunes
  the estimator's multiplicative growth/slack factor $\alpha$ on all cores
  ([Acar et al., 2019](https://doi.org/10.1145/3293883.3295725),
  [full paper](https://ctlab.itmo.ru/~vaksenov/papers/2019-granularity-control.pdf)).
  That is an empirical machine-tuning procedure on the paper's reduction
  benchmark, not a proof that its selected values globally minimize OOX time.
- Oracle-guided scheduling likewise profiles architecture-specific constants and
  predicts whether a task is too small to parallelize
  ([Acar, Chargueraud, and Rainey, 2016](https://doi.org/10.1017/S0956796816000101),
  [full paper](https://www.andrew.cmu.edu/user/mrainey/papers/jfp-oracle-guided.pdf)).

These papers justify delayed or coarsened task exposure and machine calibration.
They do not derive Rapid Start's worker-group publication time or prove that a p99
gate is optimal. OOX must estimate those quantities from its own traces.

## 4. Mapping those models to the exact OOX modes

| Mode | Initial distribution | Grain and later balancing |
| --- | --- | --- |
| `RAPID_START` | Hierarchical region activation through rapid inboxes with ordinary-queue overflow fallback | Proportional contiguous worker/data subtrees with inherited nested domains |
| `EIGEN_STEALING` | One root range | Binary splitting to a caller-supplied fixed grain (1 in this benchmark), then deque stealing |
| `EIGEN_SHARING` | `K_SPLIT=2` targeted mailbox tree | Binary splitting to a caller-supplied fixed grain (1 here), then stealing |
| `EIGEN_STEALING_GRAINSIZE` | One root range | Root measures a timespan-derived grain, then chunk stealing |
| `EIGEN_SHARING_STEALING` | Targeted mailbox tree | Each sharing-tree task publishes children, measures a local grain on its retained range, then enables chunk stealing |

Two details are easy to miss:

1. Eigen's `Balancing::STATIC` means a fixed grain, not immutable static worker
   blocks. The range is still recursively split and can be stolen.
2. A rapid publication normally uses one targeted inbox ticket. If the bounded
   rapid path fills, its embedded ordinary-queue ticket preserves progress.

With enough iterations, the Eigen sharing tree performs at most
$\min(P,N)-1$ targeted publications. For fixed `K_SPLIT=2`, sufficient work at
every node, and approximately constant edge latency, its critical depth is
$O(\log P)$. With publication latency $\ell_{\text{pub}}$, a first-order
critical-path term is therefore $O(\ell_{\text{pub}}\log P)$. Rapid Start
instead publishes one descriptor: the publisher handles slot 0 and each trapper
present in the mask handles a further slot, all calculating partitions locally.
Its critical cost is descriptor visibility plus the slowest participating slot's
observation/start delay.

## 5. Estimating initialization and rapid publication time

"Start time" is ambiguous unless the measurement boundary is named. OOX needs
five separate quantities:

1. **Cold initialization** $I_m(P)$: create the pool/group, register workers,
   configure affinity, and warm the runtime.
2. **Warm publisher cost** $S_{\text{pub},m}$: enter the launch and make its
   descriptor or root task visible.
3. **Post-visibility activation horizon** $D_{m,P}$: from that visibility point
   until every required participating slot has begun useful work.
4. **End-to-end team-start horizon** $G_{m,P}=S_{\text{pub},m}+D_{m,P}$: from
   immediately before launch to the last required first entry.
5. **Empty-loop makespan** $H_m(N,P)$: launch $N$ empty callbacks and wait for
   completion. This includes activation, scheduling, and join; it is not a direct
   task-allocation counter.

For measured launch $r$, record $b_r$ immediately before publication, $v_r$ when
the root/descriptor becomes visible, and first useful entry $t_{r,j}$ for required
slot $j$. Define

$$
S_{\text{pub},r}=v_r-b_r,
\qquad
a_{r,j}=t_{r,j}-v_r,
\qquad
D_{r,P}=\max_j a_{r,j},
\qquad
G_{r,P}=\max_j(t_{r,j}-b_r),
$$

and $C_r=|\{j:t_{r,j}\text{ was observed}\}|$. This boundary makes the
$S_{\text{pub}}+\max_j(a_j+\kappa L_j/s_j)$ model additive rather than counting
publication twice. Counting callbacks without slot/worker identities is
insufficient: several callbacks may run on one worker while another never starts.

### What the historical p99 tuner estimates

The thesis and hybrid manuscript repeatedly invoke a loop with one blocking item
per worker. Their timer starts immediately before `ParallelFor`, so the measured
maximum callback-entry time corresponds to $G$, not the post-visibility $D$ above.
For mode $m$ and an explicitly recorded execution regime $z$ (topology, affinity,
power state, nesting, contention, and system load), the rule is

$$
\tau_{\text{gate}}(m,P,z)
=Q_{1-\varepsilon}(G_{m,P,z}),
\qquad \varepsilon=0.01.
$$

The 2023 thesis describes 10,000 measured launches; the English draft says
$10^5$. OOX's [current tuner](timespan_tuner.cpp) defaults to 10,000 after ten
warm-ups. It uses the non-adaptive Eigen baseline, so it estimates
$G_{\text{Eigen},P,z}$. The rule is an empirical service-level heuristic: under a
stationary repeat of the calibrated regime, about 99% of launches are expected to
activate the required team before the gate. It is neither a hard guarantee nor a
proof of globally optimal elapsed time.

The p99 has a concrete cost interpretation. Let $c_{\text{early}}$ and
$c_{\text{late}}$ be linear penalty rates per unit time for exposing balancing
work before distribution completes and waiting after it completes. Consider

$$
J(\tau)=c_{\text{early}}\,\mathbb E[(G-\tau)_+]
       +c_{\text{late}}\,\mathbb E[(\tau-G)_+].
$$

Writing $q_c=c_{\text{early}}/(c_{\text{early}}+c_{\text{late}})$, a minimizer
satisfies the general discrete-quantile condition

$$
F_G(\tau^{*}-)\le q_c\le F_G(\tau^*).
$$

For a continuous strictly increasing CDF this becomes $F_G(\tau^*)=q_c$. This is
an OOX scheduling synthesis of the standard quantile-loss result, not a claim in
the historical draft. Choosing p99 implicitly values an early release about 99
times more heavily than an equally long late release under this linear loss. The
quantile-loss foundation is Koenker and Bassett, *Regression Quantiles*,
Econometrica 46(1),
1978 ([DOI](https://doi.org/10.2307/1913643)). OOX should measure the two costs
rather than assume that ratio forever.

### Reproducible calibration protocol

For each machine topology, affinity policy, worker count, scheduler mode, and
power policy:

1. Record the clock source and unit. Convert `RDTSC` or `CNTVCT_EL0` through a
   measured/documented counter frequency before comparing with nanoseconds.
2. Measure $I_m(P)$ in fresh processes; do not mix it into warm samples.
3. Warm the pool, then collect $S_{\text{pub},r}$, $D_{r,P}$, $G_{r,P}$, and
   $C_r$ with participating-slot and worker identities.
4. Treat $C_r<P$ at timeout as a failed activation or right-censored tail sample,
   and report its probability; discarding it would bias p99 downward. Discard only
   runs proven to have invalid instrumentation.
5. Randomize or counterbalance scheduler order and repeat across fresh processes,
   not only many calls inside one process.
6. State the empirical-quantile convention, sample count, warm-up count, and a
   confidence/tolerance interval for the selected quantile.
7. Freeze the selected value, then report failure and exceedance rates on held-out
   launches.
8. Repeat after changing mode, $P$, affinity, CPU, power/load state, scheduler
   code, nesting, body duration, or memory-contention regime. The historical
   prototype treats the gate as application-independent; OOX must test that
   transfer assumption rather than adopt it.

This follows the general guidance to separate uncertainty at different repetition
levels and report effect-size uncertainty
([Kalibera and Jones, 2013](https://kar.kent.ac.uk/33611/)) and to document parallel
benchmark topology and variability
([Hoefler and Belli, 2015](https://doi.org/10.1145/2807591.2807644)).

For Rapid Start, separately measured $D_{\text{Rapid},P,z}$ estimates the activation
term inside $a_j$ and should be reported alongside group registration and reuse.
For Eigen's hybrid policy, $\tau_{\text{gate}}$ is an input to `INIT_TIME`. The
same probe design is reusable, but Rapid and Eigen have different publication
paths and therefore require separate distributions; neither mode's p99 transfers
to the other without validation.

## 6. Choosing worker, tree, gate, and grain parameters

There is no machine-independent tuple of "optimal Rapid Start parameters." First
fix the application scenario $z$: group/pool lifetime $q$, machine topology,
affinity, power policy, and workload mixture. Treating $q$ as a free variable
without a lifetime or resource cost would make the optimizer choose its largest
allowed value trivially. For a latency objective, define the applicable candidate
vector $\vartheta=(m,P,K,\tau,g)$ and, for example,

$$
\vartheta^*_{\text{lat}}(z)=\arg\min_{\vartheta\in\mathcal C(z)}
\sum_x\omega_x\left[
\operatorname{median}T(x;\vartheta,z)
+\lambda_{\text{tail}} Q_{0.99}(T(x;\vartheta,z))
\right],
$$

where $m$ is the mode, $K$ is sharing-tree fanout, $\tau$ is the Eigen
publication gate, $g$ is grain, and $\omega_x$ describes the declared workload
mixture. Setting $\lambda_{\text{tail}}=0$ minimizes weighted median latency, not
throughput. A throughput study should maximize completed useful work per total
steady-state elapsed time; minimizing mean latency is an equivalent proxy only
for a single sequential caller with fixed work per call. A latency service may
instead minimize p99 subject to a throughput constraint.

The parameters do not all belong to every mode:

| Parameter | Rapid Start | Eigen hybrid | How to seed it |
| --- | --- | --- | --- |
| Worker count $P$ | Yes | Yes | Enumerate topology-aware choices; include one-worker and performance-core-only baselines |
| Reuse $q$ / lifetime | Exogenous: process-static runtime today | Exogenous pool lifetime | Report $I(P)/q$ for realistic application lifetimes; do not optimize it without a lifetime/resource cost |
| Membership/topology | Immutable inherited domains; optional whole-subtree leases | Mailbox targets are implementation-defined | Enumerate balanced topology subtrees and measure before adding affinity policy |
| Tree fanout $K$ | No | Yes, but compile-time today | Rebuild or make it runtime-configurable, then enumerate small integers |
| Gate $\tau$ | No | Yes, but `INIT_TIME` is compile-time today | Rebuild or parameterize; seed from an upper-confidence p99 of mode-specific $G_{m,P,z}$ |
| Grain $g$ | No: ranges follow $N/P$ and membership | Caller-supplied fixed or timespan-derived | Seed Eigen from observed work during $\tau$, then test multiplicative neighbors |

### Analytic seeds, not final answers

The current Eigen code executes iterations until elapsed time exceeds
`INIT_TIME`, then uses roughly the number completed as the grain:

$$
g_{\text{time}}\approx\widehat r_{\text{local}}\tau,
$$

so a balancing chunk is intended to contain about one gate interval of useful
work. This adapts to body cost, but it is a local noisy estimate and must be logged.
An independent overhead constraint gives

$$
g_{\text{overhead}}=\left\lceil\frac{h}{\delta c}\right\rceil,
$$

where $h$ is measured task-publication/acquisition cost, $c$ is time per
iteration, and $\delta$ bounds scheduler overhead relative to useful-work time:
$h/(gc)\le\delta$. If $\delta$ instead means overhead as a fraction of total
chunk time, the seed is $\lceil h(1-\delta)/(\delta c)\rceil$. The FSC formula in
Section 3 is another seed when its IID assumptions are credible.

For a sharing node that keeps one part and publishes at most $K$ child subtrees, a
useful empirical surrogate is

$$
d_K(1)=0,
\qquad
d_K(P)=1+d_K\!\left(\left\lceil\frac{P-1}{K}\right\rceil\right),
\qquad
D_K(P)\approx d_K(P)(c_{\text{node}}+Kc_{\text{pub}}).
$$

This expression is derived for OOX; it is not a theorem from the cited papers.
For $K\ge2$, $d_K(P)=O(\log_K P)$; for $K=1$, $d_1(P)=P-1$. The per-level cost is
conservative because the last levels may publish fewer than $K$ children. Larger
$K$ reduces depth but serializes more publications per node and may increase
coherence pressure, so `K_SPLIT=2` is a starting point, not a demonstrated optimum.

For Rapid Start, choose $P$ by evaluating the actual static placement:

$$
\widehat T_{\text{Rapid}}(P,q)
=\frac{I_{\text{Rapid}}(P)}q
+S_{\text{pub}}(P)
+\max_j\left(a_j(P)+\frac{\kappa L_j(P)}{s_j}\right)
+J_{\text{join}}(P).
$$

On heterogeneous cores, increasing $P$ can increase $a_j$, join latency, or the
static tail even while aggregate nominal capacity rises. Enumerating the available
topology-aware $P$ values is safer than differentiating a homogeneous-core model.

### Search and validation procedure

1. **Machine calibration:** measure timer conversion, worker speeds, $I(P)$,
   activation distributions, join cost, and task/request overhead.
2. **Analytic seeding:** form candidates around p90/p95/p99/p99.9 gates, small
   fanouts, the body-derived grain, the overhead grain, and the classical FSC seed.
3. **Controlled search:** evaluate this finite candidate set in randomized order on
   balanced, skewed, short, long, nested, and repeated-call training workloads.
4. **Model fit:** fit interpretable event/cost terms, not an unconstrained curve;
   include interactions such as mode-by-body-cost and $P$-by-topology.
5. **Selection:** choose the simplest candidate whose confidence interval is not
   practically worse than the measured minimum. Preserve a Pareto set if throughput,
   tail latency, and energy disagree.
6. **Holdout:** report predictions and chosen parameters on unseen widths, work
   orders, fresh runs, and at least one different machine.

General-purpose autotuning frameworks support larger search spaces
([Ansel et al., *OpenTuner*, 2014](https://doi.org/10.1145/2628071.2628092)),
but the current OOX space is small enough that an exhaustive, counterbalanced grid
is easier to audit. The result is an optimum only for the declared candidate set,
objective, workload distribution, and machine configuration.

## 7. The executable first-order OOX model

For SpMV family $k$ and mode $m$, `model.py` fits the warm-call quantity

$$
\boxed{
\widehat T^{\text{warm}}_{m,k}
=H_m(N)
+\rho_mN
+\kappa_k\left[
\phi_m\frac WP
+\theta_m\left(L_{\text{static}}-\frac WP\right)
\right]
}
$$

Initialization is observed separately rather than fitted:

$$
\widehat T^{\text{amortized}}_{m,k}(q)
=\frac{I_m}{q}+\widehat T^{\text{warm}}_{m,k}.
$$

The prediction CSV contains the warm term; the initialization-amortization CSV
contains $I_m/q$. They must be added only when the application actually constructs
or registers the runtime within the modeled lifetime.

The terms have operational meanings:

- $I_m/q$: worker/group construction amortized over $q$ calls;
- $H_m(N)$: end-to-end empty-body `Launch(N)`/join cost, represented by the
  empirical surrogate $a+b(N/N_{\max})^\gamma$; the implementation searches
  $\gamma\in[0.2,1.6]$, so this is not an asymptotic-complexity claim;
- $\rho_mN$: scheduler/body interaction per row that an empty body cannot
  observe, including task/body overlap and migration effects;
- $\kappa_k$: Rapid-anchored microseconds per structural work unit for one
  matrix family;
- $\phi_m$: fitted relative useful-work coefficient against the Rapid anchor;
- $L_{\text{static}}$: the exact largest contiguous Rapid partition;
- $\theta_m$: residual coefficient on the static excess tail.

For Rapid, $\rho=0,\phi=1,\theta=1$ define the calibration anchor. A small fitted
$\theta$ means this regression attributes little remaining time to the static-tail
regressor; it does not by itself prove that the scheduler removed that tail.
$\theta$ is not a probability and $\phi$ is not purely a cache multiplier; the
current measurements cannot separate cache, topology, and scheduler work.

Define the speed-weighted static load ratio

$$
r_s=\frac{\sum_j s_j}{W}\max_j\frac{L_j}{s_j}.
$$

For homogeneous workers it reduces to $L_{\text{static}}/(W/P)$. Define Rapid's
post-visibility excess over the ideal speed-aware useful-work lower bound as

$$
E_R=\max_j\left(a^R_j+\frac{\kappa L_j}{s_j}\right)
    -\frac{\kappa W}{\sum_j s_j}.
$$

Let $E_D=T_{\text{work},D}-\kappa W/\sum_j s_j$ be the corresponding measured or
modeled dynamic excess, including its activation, residual imbalance, scheduling,
and cache/migration effects. Dynamic scheduling wins under this decomposition when

$$
E_R-E_D>
\frac{I_D-I_R}{q}
+(S_{\text{pub},D}-S_{\text{pub},R})
+(J_D-J_R).
$$

Only if activation is equal or separately absorbed does
$E_R\approx(r_s-1)\kappa W/\sum_j s_j$. That expression is an available
static-imbalance opportunity, not guaranteed time saved by a real dynamic
scheduler. Balanced short loops tend to remain on Rapid's side of the crossover;
long skewed loops tend to move to the dynamic side.

Scan requires a separate structural model because, for the power-of-two sizes in
this suite, it performs $2\log_2N$ parallel invocations and the timespan-derived
grain depends on the task body:

$$
T_{\text{scan},m}
=\alpha_m(2\log_2N)
+\beta_m\left(2\sum_{j=1}^{\log_2N}
\left\lceil\frac{N/2^j}{P}\right\rceil\right).
$$

$\alpha$ is the effective hot call/barrier cost, while $\beta$ is time per
critical-worker iteration slot, including incremental body-dependent scheduling.
This model is intentionally
not obtained by blindly summing empty-body `Launch` values: doing that already
overpredicts some Eigen scan cases.

## 8. What the current full run says

The local run `20260729T201647Z_Kirills-MacBook-Pro-3.local` used 16 workers,
Release Clang 19, and ten repetitions. It is a development snapshot under the
ignored `results/` tree, not an immutable published artifact; its exact raw data
must be archived with a commit and checksum before these numbers are cited in a
paper.

Empty-body launch time exposes end-to-end publication/scheduling/join scaling:

| Mode | `Launch(64)` | `Launch(262144)` |
| --- | ---: | ---: |
| Rapid Start | 51.0 us | 54.3 us |
| Eigen stealing | 50.2 us | 2875.0 us |
| Eigen sharing | 14.7 us | 2335.7 us |
| Eigen sharing-stealing | 9.8 us | 467.4 us |

Rapid reuses its registered trappers and one descriptor rather than building a
task frontier, so its curve is nearly flat. The grain-one Eigen modes create
$O(N)$ task objects or leaf ranges, although that is not literally one queued task
per iteration at every instant. Tree sharing creates the frontier in parallel;
timespan-derived grains reduce its size substantially.

At the largest SpMV width, exact static imbalance and observed time correspond
directionally, not quantitatively:

| Shape | $L_{\text{static}}/(W/P)$ | Rapid | Eigen stealing |
| --- | ---: | ---: | ---: |
| Balanced | 1.001 | 16.438 ms | 16.108 ms |
| Hyperbolic | 7.875 | 30.027 ms | 8.988 ms |
| Triangle | 1.911 | 13.729 ms | 11.037 ms |

The latest fitted diagnostics are:

| Mode | $\rho$, us/row | $\phi$ | $\theta$ | Training MAPE |
| --- | ---: | ---: | ---: | ---: |
| Rapid Start | 0.000 | 1.000 | 1.000 | 8.3% |
| Eigen stealing | 0.117 | 0.960 | 0.167 | 18.0% |
| Eigen sharing | 0.247 | 1.149 | 0.202 | 19.1% |
| Eigen sharing-stealing | 0.198 | 1.015 | 0.241 | 28.0% |

The defensible conclusion is qualitative: on these sampled skewed cases the Eigen
policies mitigate the observed cost associated with Rapid's static placement. The
regression's small $\theta$ values are associations among confounded terms, not a
causal measurement of how much imbalance was removed, and the fitted fractions
are not stable enough for a paper claim.

The `scheduling_dist` plot is a **task-start spread**
$\max(t_{\text{entry}})-\min(t_{\text{entry}})$, not the publication-to-last-entry
$D$ or $G$ defined in Section 5. In the 16-task spin probe, Rapid uses all 16
workers exactly once, whereas median distinct workers are only 7 for stealing,
7.5 for sharing, and 8.5 for the hybrid. A small spread can therefore mean
"finished on a subset," not "activated the full team quickly." Estimating $G$
requires origin-to-maximum timing plus required-slot coverage, as in the blocking
tuner probe.

## 9. Why these coefficients are not publishable yet

1. The host has 12 performance and 4 efficiency cores. macOS pinning is currently
   a no-op, so equal iteration counts do not imply equal execution time.
2. There is only one full $P=16$ run in the main fit. Worker availability,
   publication depth, and heterogeneity cannot be separated at one $P$.
3. The two complete same-commit runs disagree materially. For the overlapping
   cases, Rapid is stable, but several Eigen medians change by much more than 20%.
   The mode order is fixed and recorded system load was very high.
4. The full run omits `EIGEN_STEALING_GRAINSIZE`. Without it, initial sharing and
   adaptive grain selection cannot be isolated.
5. `Launch` is not a universally transferable scheduler term. In a timespan mode,
   the body determines the measured grain and hence the number of generated tasks.
6. The three SpMV families need different $\kappa_k$ values. One nonzero is not
   a universal time unit because reuse and memory traffic differ by shape.
7. Current traces expose externally visible task execution, but not mailbox pops,
   failed probes, proxy wins, generated grains, cache misses, or worker core class.

There is also a timespan-unit mismatch to resolve. On AArch64,
[`Now()`](../eigen/util.h) returns raw `CNTVCT_EL0` ticks and
[`INIT_TIME=1800`](../eigen/timespan_partitioner.h) is therefore a tick
count. The tuner emits wall-clock nanoseconds. This run reports a 24 MHz counter,
making 1800 ticks about 75 us, while the tuner p99 is 20.8955 ms. Those values
cannot be substituted for one another without reading `CNTFRQ_EL0` and converting
units. The tuner's all-worker barrier is itself heavily affected by the unpinned
heterogeneous host.

## 10. How to run and read the model

After a complete non-smoke evaluation containing Rapid Start, at least three
`Launch` sizes, and the SpMV families:

```sh
python3 benchmarks/scheduler_eval/model.py \
  results/scheduler_eval/<result-directory>
```

It writes:

- `summaries/model_parameters.json`: fitted parameters and startup diagnostics;
- `summaries/model_predictions.csv`: every observed/predicted case and time
  decomposition;
- `summaries/model_initialization_amortization.csv`: $I/q$ for several reuse
  counts;
- `summaries/model.md`: compact fit report;
- `plots/model_spmv_observed_vs_predicted.svg`: measured solid curves and fitted
  dashed curves.

Use residuals as evidence. A systematic residual is a missing mechanism; it is
not an invitation to add arbitrary polynomial terms.

## 11. Measurements that turn this into a causal model

The next evaluation should add, in this order:

1. Run all four Eigen policies, especially `EIGEN_STEALING_GRAINSIZE`.
2. Randomize or counterbalance process/mode order and repeat complete runs in
   fresh processes.
3. Sweep $P=1,2,4,8,12,16$, and compare performance cores only with all cores.
4. Record worker registration, runnable, start, idle, and completion times, plus
   core identity or calibrated per-worker speed.
5. Count task allocations, range splits, targeted publications, own-mailbox pops,
   foreign-mailbox pops, successful steals, and failed probes.
6. Record every derived grain and split the current timespan into separate
   publication-gate and desired-grain-duration parameters.
7. Add serial $T_1$, static-fork, and one-worker baselines for every workload.
8. Add hardware counters for cycles, instructions, cache misses, and migrations
   where the platform permits them.
9. Run the same weighted loop in sorted, randomly shuffled, and cyclically shifted
   order. These have the same $n,\mu,\sigma$, but different contiguous loads and
   therefore distinguish stochastic chunking theory from spatial structure.
10. Fit on selected widths/runs and report error on held-out widths, runs, and
    machines.

With those counters, a candidate conjectural event model for the fitted residual
is:

$$
T-\widehat T
\approx
\frac{\tau_a A+\tau_s S+\mu_{\text{miss}}\Delta M}{P_{\text{eff}}},
$$

where $A$ is probe/attempt count, $S$ is successful migration count, and
$\Delta M$ is extra cache misses. Dividing every term by $P_{\text{eff}}$ assumes
perfect overlap and is a hypothesis to test, not an established identity.
Ultimately the event simulator should predict each worker's start, queue/mailbox
acquisition, chunk execution, and finish time; the makespan is the latest finish
plus join cost.

## 12. What is inherited and what OOX still has to establish

The citation boundary for a future paper should be explicit:

- **Inherited from prior systems work:** work/span bounds, request accounting,
  latency-aware stealing, cache effects of steals, list-scheduling tails, classical
  chunk formulas, lazy/heartbeat task exposure, and architecture-calibrated
  granularity control.
- **Inherited from the historical prototype:** targeted tree publication, the p99
  publication-gate heuristic, body-derived grain, the benchmark families, and the
  bitmask Rapid Start experiment. These sources are a thesis, draft, talks, and
  code artifact, not a peer-reviewed Rapid Start paper.
- **Current OOX implementation:** reproducible native benchmarks, explicit cold
  versus warm measurements, worker-coverage reports, the Rapid adapter, four Eigen
  modes, and the first diagnostic time decomposition.
- **Proposed OOX research contribution:** a unit-correct, topology-conditioned
  activation estimator; a cost-weighted choice of gate quantile; a validated search
  over worker count, reuse, fanout, and grain; an event-instrumented crossover model;
  and held-out evidence of where Rapid Start outperforms stealing or mailbox sharing.

No cited paper supplies that complete optimizer. Until OOX records the missing
events and validates selected parameters on held-out runs and machines, the current
coefficients explain observations but do not establish universal optimal settings.
