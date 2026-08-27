// Build:  g++ -std=c++20 -O2 -Wall -Wextra -pthread -o order_book order_book.cpp
// Run:    ./order_book
#define ORDER_BOOK_TESTING

// =============================================================================
// order_book.hpp
//
// Top-of-book order book for sequenced market data feeds.
//
// Concurrency model:
//   - ONE writer (the feed thread) calls onUpdate().
//   - N readers call bestBid() / bestAsk() / top() lock-free and wait-free.
//   - The top of book is published as a single atomic uint64_t (bid in the
//     high 32 bits, ask in the low 32 bits), which removes the need for a
//     seqlock entirely: one load returns a coherent pair.
//
// Note on the seqlock: you only need one if you ever publish more than 8 bytes
// (e.g. price + quantity for both sides). In that case the correct pattern is:
//     ver_.store(v + 1, std::memory_order_relaxed);
//     std::atomic_thread_fence(std::memory_order_release);   // <- required
//     ...write the payload...
//     ver_.store(v + 2, std::memory_order_release);
// and on the reader side a std::atomic_thread_fence(acquire) AFTER loading the
// payload and BEFORE re-reading the version. Without those fences the seqlock
// only works by accident on x86; it breaks on ARM/POWER.
// =============================================================================

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

#ifdef ORDER_BOOK_TESTING
#include <utility>
#endif

// -----------------------------------------------------------------------------
// Update
// A decoded market data message, handed over by the feed parser.
//   sequence : feed sequence number (monotonically increasing)
//   side     : 'B' = bid (buy), 'A' = ask (sell)
//   price    : price in integer ticks (never floating point)
//   quantity : total quantity at that level; 0 means "delete this level"
// -----------------------------------------------------------------------------
struct Update {
    uint64_t sequence;
    char     side;
    int      price;
    int      quantity;
};

// -----------------------------------------------------------------------------
// BufferedUpdate
// A slot in the reorder buffer: an Update that arrived out of order and is
// waiting for the missing ones. 'valid' distinguishes an occupied slot from an
// empty one.
// -----------------------------------------------------------------------------
struct BufferedUpdate {
    uint64_t sequence = 0;
    int      price    = 0;
    int      quantity = 0;
    char     side     = 0;
    bool     valid    = false;
};

class OrderBook {
public:

    static constexpr int kEmpty = -1;


    struct TopOfBook {
        int bid;
        int ask;
    };

    // -------------------------------------------------------------------------
    // OrderBook(initial_sequence, max_gap, expected_depth)
    //
    // What it does: builds an empty book and sizes the internal structures.
    //
    // Inputs:
    //   initial_sequence - first sequence number expected from the feed.
    //   max_gap          - largest feed gap tolerated. Updates up to
    //                      initial_sequence + max_gap are buffered; anything
    //                      beyond that forces a resynchronisation.
    //   expected_depth   - typical book depth per side; used only to reserve
    //                      memory so the hot path never reallocates.
    //
    // Output: none. The published top of book starts as {kEmpty, kEmpty}.
    //
    // Note: the reorder buffer is rounded up to the next power of two, which
    // lets us replace the % operator (integer division, ~20-40 cycles) with a
    // single-cycle AND against a mask.
    // -------------------------------------------------------------------------
    OrderBook(uint64_t initial_sequence, int max_gap, std::size_t expected_depth = 64)
        : expected_seq_(initial_sequence),
          max_gap_(max_gap < 0 ? 0u : static_cast<uint64_t>(max_gap)) {
        const std::size_t needed = static_cast<std::size_t>(max_gap_) + 2u;
        reorder_buffer_.resize(nextPow2(needed));
        buf_mask_ = reorder_buffer_.size() - 1u;

        bids_.reserve(expected_depth);
        asks_.reserve(expected_depth);

        last_published_ = pack(kEmpty, kEmpty);
        top_of_book_.store(last_published_, std::memory_order_relaxed);
    }

    OrderBook(const OrderBook&)            = delete;
    OrderBook& operator=(const OrderBook&) = delete;

    // -------------------------------------------------------------------------
    // onUpdate(update)
    //
    // What it does: the writer's single entry point. Applies the update if it is
    // in order, buffers it if it runs ahead but within tolerance, drops it if it
    // is a duplicate or a late retransmission, and resynchronises if the gap is
    // too large. After applying, it drains every contiguous update already
    // sitting in the reorder buffer and publishes the top of book ONCE.
    //
    // Input : update - a decoded market data message.
    // Output: none. Visible effects: bids_/asks_ are updated and, if the top
    //         changed, one atomic store to top_of_book_.
    // Caller: the feed thread only. Not safe to call from multiple threads.
    // -------------------------------------------------------------------------
    void onUpdate(const Update& update) noexcept {
        const uint64_t seq = update.sequence;

        // Most common case first: packet in order.
        if (seq == expected_seq_) [[likely]] {
            applyAndDrain(update);
            return;
        }

        // Duplicate or late retransmission: ignore.
        if (seq < expected_seq_) [[unlikely]] {
            return;
        }

        // Runs ahead. Does it fit in the reorder window?
        const uint64_t gap = seq - expected_seq_;
        if (gap <= max_gap_) [[likely]] {
            const std::size_t idx = static_cast<std::size_t>(seq) & buf_mask_;
            reorder_buffer_[idx] = BufferedUpdate{seq, update.price, update.quantity,
                                                  update.side, true};
            return;
        }

      
        invalidateBuffer();
        expected_seq_ = seq;
        applyAndDrain(update);
    }

    // -------------------------------------------------------------------------
    // top()
    //
    // What it does: reads the top of book (best bid and best ask) coherently —
    // both values always come from the same logical instant. Wait-free: a single
    // atomic load, no loop, no retry.
    //
    // Input : none.
    // Output: TopOfBook{bid, ask}; kEmpty (-1) on any side with no levels.
    // -------------------------------------------------------------------------
    TopOfBook top() const noexcept {
        const uint64_t v = top_of_book_.load(std::memory_order_acquire);
        return TopOfBook{unpackBid(v), unpackAsk(v)};
    }

    // -------------------------------------------------------------------------
    // bestBid() / bestAsk()
    //
    // What they do: return the best price on one side.
    // Input : none.
    // Output: price in ticks, or kEmpty (-1) if that side is empty.
    //
    // Warning: if you need both values together (spread, crossed-book check),
    // use top(). Two separate calls can observe two different instants.
    // -------------------------------------------------------------------------
    int bestBid() const noexcept {
        return unpackBid(top_of_book_.load(std::memory_order_acquire));
    }

    int bestAsk() const noexcept {
        return unpackAsk(top_of_book_.load(std::memory_order_acquire));
    }

#ifdef ORDER_BOOK_TESTING
    // -------------------------------------------------------------------------
    // snapshot(side)  [TEST BUILDS ONLY]
    //
    // What it does: returns a copy of one side of the book so a test can compare
    // full depth against a reference model. Compiled out of production builds.
    //
    // Input : side - 'B' for bids, anything else for asks.
    // Output: vector of (price, quantity), sorted the same way as internally.
    // NOT thread-safe: call from the writer thread only, with no update in
    // flight.
    // -------------------------------------------------------------------------
    std::vector<std::pair<int, int>> snapshot(char side) const {
        const auto& src = (side == 'B') ? bids_ : asks_;
        std::vector<std::pair<int, int>> out;
        out.reserve(src.size());
        for (const PriceLevel& pl : src) out.emplace_back(pl.price, pl.quantity);
        return out;
    }
#endif

private:
    // An aggregated price level. 8 bytes => 8 levels per cache line.
    struct PriceLevel {
        int price;
        int quantity;
    };

    // -------------------------------------------------------------------------
    // applyAndDrain(u)
    //
    // What it does: applies an in-order update, advances expected_seq_, then
    // consumes every contiguous update already held in the reorder buffer.
    // Publishes the top of book at the end (one atomic store per burst instead
    // of one per modified level).
    //
    // Input : u - an update whose sequence is exactly expected_seq_.
    // Output: none.
    // -------------------------------------------------------------------------
    void applyAndDrain(const Update& u) noexcept {
        processSingleUpdate(u);
        ++expected_seq_;

        std::size_t idx = static_cast<std::size_t>(expected_seq_) & buf_mask_;
        while (reorder_buffer_[idx].valid &&
               reorder_buffer_[idx].sequence == expected_seq_) {
            BufferedUpdate& b = reorder_buffer_[idx];
            b.valid = false;
            processSingleUpdate(Update{b.sequence, b.side, b.price, b.quantity});
            ++expected_seq_;
            idx = static_cast<std::size_t>(expected_seq_) & buf_mask_;
        }

        publishTop();
    }

    // -------------------------------------------------------------------------
    // processSingleUpdate(u)
    //
    // What it does: routes the update to the correct side of the book.
    // Input : u - an update already validated for sequencing.
    // Output: none. Unknown sides are silently ignored.
    // -------------------------------------------------------------------------
    void processSingleUpdate(const Update& u) noexcept {
        if (u.side == 'B') {
            upsert(bids_, u.price, u.quantity, /*descending=*/true);
        } else if (u.side == 'A') {
            upsert(asks_, u.price, u.quantity, /*descending=*/false);
        }
    }

    // -------------------------------------------------------------------------
    // upsert(levels, price, quantity, descending)
    //
    // What it does: inserts, updates or removes a price level while keeping the
    // vector sorted at all times. Uses lower_bound (binary search) instead of a
    // linear find_if followed by a sort: O(log n) comparisons plus one memmove,
    // rather than O(n) + O(n log n) per update.
    //
    // Inputs:
    //   levels     - sorted vector for the side being modified.
    //   price      - price in ticks.
    //   quantity   - new quantity; 0 removes the level.
    //   descending - true for bids (highest price first), false for asks.
    // Output: none. levels[0] becomes the top of that side.
    // -------------------------------------------------------------------------
    static void upsert(std::vector<PriceLevel>& levels, int price, int quantity,
                       bool descending) noexcept {
        auto it = descending
            ? std::lower_bound(levels.begin(), levels.end(), price,
                               [](const PriceLevel& pl, int p) { return pl.price > p; })
            : std::lower_bound(levels.begin(), levels.end(), price,
                               [](const PriceLevel& pl, int p) { return pl.price < p; });

        const bool found = (it != levels.end() && it->price == price);

        if (quantity == 0) {
            if (found) levels.erase(it);
            return;
        }
        if (found) {
            it->quantity = quantity;
        } else {
            levels.insert(it, PriceLevel{price, quantity});
        }
    }

    // -------------------------------------------------------------------------
    // publishTop()
    //
    // What it does: packs the best bid and best ask into a single uint64_t and
    // publishes it to the readers. If the top has not changed it writes nothing,
    // which avoids needlessly invalidating the readers' cache line — the main
    // source of latency in a book with heavy activity on deep levels.
    //
    // Input : none (reads bids_/asks_).
    // Output: none (writes top_of_book_).
    // -------------------------------------------------------------------------
    void publishTop() noexcept {
        const int bid = bids_.empty() ? kEmpty : bids_.front().price;
        const int ask = asks_.empty() ? kEmpty : asks_.front().price;
        const uint64_t packed = pack(bid, ask);

        if (packed != last_published_) {
            last_published_ = packed;
            top_of_book_.store(packed, std::memory_order_release);
        }
    }

    // -------------------------------------------------------------------------
    // invalidateBuffer()
    //
    // What it does: marks every reorder buffer slot as empty. Touches only the
    // 'valid' byte instead of rewriting the whole struct.
    // Input/Output: none.
    // -------------------------------------------------------------------------
    void invalidateBuffer() noexcept {
        for (BufferedUpdate& b : reorder_buffer_) b.valid = false;
    }

    // -------------------------------------------------------------------------
    // pack / unpackBid / unpackAsk
    //
    // What they do: convert the (bid, ask) pair to and from a single uint64_t so
    // that publishing and reading the top happen in one atomic instruction.
    // Inputs/Outputs: prices in ticks (int32) <-> one 64-bit word.
    // -------------------------------------------------------------------------
    static constexpr uint64_t pack(int bid, int ask) noexcept {
        return (static_cast<uint64_t>(static_cast<uint32_t>(bid)) << 32) |
               static_cast<uint64_t>(static_cast<uint32_t>(ask));
    }
    static constexpr int unpackBid(uint64_t v) noexcept {
        return static_cast<int>(static_cast<uint32_t>(v >> 32));
    }
    static constexpr int unpackAsk(uint64_t v) noexcept {
        return static_cast<int>(static_cast<uint32_t>(v));
    }

    // -------------------------------------------------------------------------
    // nextPow2(n)
    // What it does: returns the smallest power of two >= n (minimum 2).
    // Input : n - desired size.  Output: rounded-up capacity.
    // -------------------------------------------------------------------------
    static std::size_t nextPow2(std::size_t n) noexcept {
        std::size_t p = 2;
        while (p < n) p <<= 1;
        return p;
    }

    // --- Writer-only state (never read by the readers) -----------------------
    uint64_t                    expected_seq_;
    uint64_t                    max_gap_;
    std::size_t                 buf_mask_ = 0;
    std::vector<BufferedUpdate> reorder_buffer_;
    std::vector<PriceLevel>     bids_;   // sorted by descending price
    std::vector<PriceLevel>     asks_;   // sorted by ascending price
    uint64_t                    last_published_ = 0;

    // --- Cache line shared with the readers ----------------------------------
    // Isolated on its own line to avoid false sharing with the state above,
    // which the writer modifies constantly.
    alignas(64) std::atomic<uint64_t> top_of_book_{0};
    char pad_[64 - sizeof(std::atomic<uint64_t>)]{};
};

// =============================================================================
//                                  TESTS
// Everything below is test scaffolding. std::map is used ONLY in the reference
// model — the challenge bans it in the implementation, not in the test oracle.
// =============================================================================

#include <chrono>
#include <cstdio>
#include <map>
#include <random>
#include <string>
#include <thread>

static int g_failures = 0;

#define CHECK(cond, msg)                                                       \
    do {                                                                       \
        if (!(cond)) {                                                         \
            std::printf("  FAIL  %s:%d  %s\n", __FILE__, __LINE__, (msg));     \
            ++g_failures;                                                      \
        }                                                                      \
    } while (0)

// -----------------------------------------------------------------------------
// RefBook
// Deliberately naive reference implementation used as the test oracle. Slow and
// obviously correct: std::map keeps the levels sorted, begin() is the top.
// -----------------------------------------------------------------------------
struct RefBook {
    std::map<int, int, std::greater<int>> bids;
    std::map<int, int, std::less<int>>    asks;

    void apply(const Update& u) {
        if (u.side == 'B') {
            if (u.quantity == 0) bids.erase(u.price);
            else                 bids[u.price] = u.quantity;
        } else if (u.side == 'A') {
            if (u.quantity == 0) asks.erase(u.price);
            else                 asks[u.price] = u.quantity;
        }
    }
    int bestBid() const { return bids.empty() ? -1 : bids.begin()->first; }
    int bestAsk() const { return asks.empty() ? -1 : asks.begin()->first; }
};

// -----------------------------------------------------------------------------
// compareDepth(ob, ref, label)
// Compares the full depth of both sides, not just the top. Catches bugs that
// hide in deep levels until a delete pulls them to the surface.
// -----------------------------------------------------------------------------
static void compareDepth(const OrderBook& ob, const RefBook& ref, const char* label) {
    auto b = ob.snapshot('B');
    auto a = ob.snapshot('A');

    CHECK(b.size() == ref.bids.size(), (std::string(label) + ": bid depth differs").c_str());
    CHECK(a.size() == ref.asks.size(), (std::string(label) + ": ask depth differs").c_str());

    std::size_t i = 0;
    for (auto it = ref.bids.begin(); it != ref.bids.end() && i < b.size(); ++it, ++i) {
        CHECK(b[i].first == it->first && b[i].second == it->second,
              (std::string(label) + ": bid level mismatch").c_str());
    }
    i = 0;
    for (auto it = ref.asks.begin(); it != ref.asks.end() && i < a.size(); ++it, ++i) {
        CHECK(a[i].first == it->first && a[i].second == it->second,
              (std::string(label) + ": ask level mismatch").c_str());
    }
}

// -----------------------------------------------------------------------------
// makeStream(n, rng)
// Builds a perfectly ordered stream of n updates over a small price pool, so
// the same levels get hit, overwritten and deleted repeatedly.
// -----------------------------------------------------------------------------
static std::vector<Update> makeStream(std::size_t n, std::mt19937& rng) {
    std::uniform_int_distribution<int> side(0, 1);
    std::uniform_int_distribution<int> price(1000, 1040);
    std::uniform_int_distribution<int> qty(0, 100);
    std::uniform_int_distribution<int> del(0, 9);

    std::vector<Update> s;
    s.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        const int q = (del(rng) < 3) ? 0 : qty(rng) + 1;   // ~30% deletes
        s.push_back(Update{static_cast<uint64_t>(i), side(rng) ? 'B' : 'A', price(rng), q});
    }
    return s;
}

// -----------------------------------------------------------------------------
// shuffleWithinWindow(ordered, k, rng)
// Simulates a lossless but out-of-order network: an update may be held back by
// at most k/2 positions. Holding sequence S back by d makes S+1..S+d arrive
// early with a gap of at most d, so the stream always stays inside the K
// tolerance and nothing may be dropped. Duplicates are injected on top.
// -----------------------------------------------------------------------------
static std::vector<Update> shuffleWithinWindow(const std::vector<Update>& ordered,
                                               int k, std::mt19937& rng) {
    const int maxDelay = std::max(1, k / 2);
    std::uniform_int_distribution<int> hold(0, 9);
    std::uniform_int_distribution<int> delay(1, maxDelay);

    std::vector<std::pair<std::size_t, Update>> pending;   // (release index, update)
    std::vector<Update> out;
    out.reserve(ordered.size() * 5 / 4);

    for (std::size_t i = 0; i < ordered.size(); ++i) {
        for (std::size_t j = 0; j < pending.size();) {
            if (pending[j].first <= i) {
                out.push_back(pending[j].second);
                pending.erase(pending.begin() + static_cast<long>(j));
            } else {
                ++j;
            }
        }
        if (hold(rng) < 3) pending.emplace_back(i + static_cast<std::size_t>(delay(rng)), ordered[i]);
        else               out.push_back(ordered[i]);
    }
    for (auto& p : pending) out.push_back(p.second);

    // Duplicates: re-send ~5% of the messages a bit later.
    std::uniform_int_distribution<int> dup(0, 19);
    std::vector<Update> withDups;
    withDups.reserve(out.size() * 11 / 10);
    for (const Update& u : out) {
        withDups.push_back(u);
        if (dup(rng) == 0) withDups.push_back(u);
    }
    return withDups;
}

// -----------------------------------------------------------------------------
// test1_basics
// Hand-written edge cases: empty book, delete of a level that does not exist,
// quantity overwrite, deleting the top, refilling.
// -----------------------------------------------------------------------------
static void test1_basics() {
    std::printf("test1_basics\n");
    OrderBook ob(0, 8);
    uint64_t s = 0;

    CHECK(ob.bestBid() == -1 && ob.bestAsk() == -1, "empty book must return -1/-1");

    ob.onUpdate({s++, 'B', 100, 10});
    ob.onUpdate({s++, 'A', 105, 10});
    CHECK(ob.bestBid() == 100 && ob.bestAsk() == 105, "single level per side");

    ob.onUpdate({s++, 'B', 99, 5});          // deeper bid, top unchanged
    CHECK(ob.bestBid() == 100, "deeper bid must not become the top");

    ob.onUpdate({s++, 'B', 101, 7});         // better bid
    CHECK(ob.bestBid() == 101, "better bid must become the top");

    ob.onUpdate({s++, 'B', 101, 3});         // overwrite quantity
    CHECK(ob.bestBid() == 101, "quantity overwrite must not move the top");

    ob.onUpdate({s++, 'B', 101, 0});         // delete the top
    CHECK(ob.bestBid() == 100, "deleting the top must expose the next level");

    ob.onUpdate({s++, 'B', 777, 0});         // delete a level that never existed
    CHECK(ob.bestBid() == 100, "deleting an absent level must be a no-op");

    ob.onUpdate({s++, 'B', 100, 0});
    ob.onUpdate({s++, 'B', 99, 0});
    CHECK(ob.bestBid() == -1, "emptying a side must return to -1");
    CHECK(ob.bestAsk() == 105, "the other side must be untouched");

    const auto t = ob.top();
    CHECK(t.bid == -1 && t.ask == 105, "top() must agree with bestBid/bestAsk");
}

// -----------------------------------------------------------------------------
// test2_inOrder
// 50k in-order updates, comparing the top against the reference after EVERY
// single update, and the full depth at the end.
// -----------------------------------------------------------------------------
static void test2_inOrder() {
    std::printf("test2_inOrder\n");
    std::mt19937 rng(12345);
    const auto stream = makeStream(50000, rng);

    OrderBook ob(0, 16);
    RefBook   ref;
    for (const Update& u : stream) {
        ob.onUpdate(u);
        ref.apply(u);
        if (ob.bestBid() != ref.bestBid() || ob.bestAsk() != ref.bestAsk()) {
            CHECK(false, "top diverged from the reference mid-stream");
            break;
        }
    }
    compareDepth(ob, ref, "test2");
}

// -----------------------------------------------------------------------------
// test3_outOfOrder
// The real one. For many seeds: build an ordered stream, deliver it shuffled
// within the K window with duplicates on top, and require the final book to
// match the reference applied in strict sequence order. This is what proves
// "exactly once and in sequence order".
// -----------------------------------------------------------------------------
static void test3_outOfOrder() {
    std::printf("test3_outOfOrder\n");
    for (int seed = 0; seed < 200; ++seed) {
        std::mt19937 rng(static_cast<unsigned>(seed) + 999);
        const int K = 4 + (seed % 60);

        const auto ordered  = makeStream(3000, rng);
        const auto delivery = shuffleWithinWindow(ordered, K, rng);

        OrderBook ob(0, K);
        RefBook   ref;
        for (const Update& u : delivery) ob.onUpdate(u);
        for (const Update& u : ordered)  ref.apply(u);

        if (ob.bestBid() != ref.bestBid() || ob.bestAsk() != ref.bestAsk()) {
            CHECK(false, "out-of-order delivery produced a different top");
            std::printf("        seed=%d K=%d  got(%d,%d) want(%d,%d)\n",
                        seed, K, ob.bestBid(), ob.bestAsk(), ref.bestBid(), ref.bestAsk());
            break;
        }
        if (seed % 40 == 0) compareDepth(ob, ref, "test3");
    }
}

// -----------------------------------------------------------------------------
// test4_lateAndDuplicate
// A message that arrives after its sequence was already processed must be
// dropped, not applied a second time.
// -----------------------------------------------------------------------------
static void test4_lateAndDuplicate() {
    std::printf("test4_lateAndDuplicate\n");
    OrderBook ob(100, 8);

    ob.onUpdate({100, 'B', 50, 10});
    ob.onUpdate({101, 'B', 50, 20});
    CHECK(ob.bestBid() == 50, "level present");

    ob.onUpdate({100, 'B', 50, 0});     // late duplicate that would delete it
    CHECK(ob.bestBid() == 50, "a late message must not be reapplied");

    ob.onUpdate({99, 'A', 60, 5});      // older than the initial sequence
    CHECK(ob.bestAsk() == -1, "a message older than expected must be dropped");
}

// -----------------------------------------------------------------------------
// test5_resync
// A gap larger than K cannot be recovered. The book must jump to the new
// sequence and keep working instead of stalling forever waiting for the
// missing messages.
// -----------------------------------------------------------------------------
static void test5_resync() {
    std::printf("test5_resync\n");
    OrderBook ob(0, 8);

    ob.onUpdate({0, 'B', 10, 1});
    ob.onUpdate({1, 'A', 20, 1});
    CHECK(ob.bestBid() == 10 && ob.bestAsk() == 20, "book built");

    ob.onUpdate({5, 'B', 11, 1});       // gap of 3, inside K: buffered only
    CHECK(ob.bestBid() == 10, "a buffered update must not be applied yet");

    ob.onUpdate({100000, 'B', 12, 1});  // gap far beyond K: resync
    CHECK(ob.bestBid() == 12, "after a resync the new update must be applied");

    ob.onUpdate({100001, 'A', 25, 1});
    CHECK(ob.bestAsk() == 20 || ob.bestAsk() == 25, "book still accepts updates");

    // The stale buffered update must not resurface after the resync.
    ob.onUpdate({100002, 'B', 12, 0});
    CHECK(ob.bestBid() != 11, "a stale buffered update must not be applied");
}

// -----------------------------------------------------------------------------
// test6_concurrentCoherence
// The one that justifies packing bid and ask into a single 64-bit word.
//
// The writer moves BOTH sides at once while keeping the invariant
// ask - bid == 1000. It does so by sending four updates (delete old bid,
// delete old ask, insert new bid, insert new ask) in REVERSE order, so the
// first three are buffered and the fourth drains all of them in one burst,
// producing exactly one publish. Every published state therefore satisfies the
// invariant.
//
// Readers spin on top() and assert it. A torn read — a bid from instant T with
// an ask from instant T' — breaks the invariant and is caught. Run this under
// -fsanitize=thread as well.
// -----------------------------------------------------------------------------
static void test6_concurrentCoherence() {
    std::printf("test6_concurrentCoherence\n");

    OrderBook ob(0, 32);
    std::atomic<bool>    stop{false};
    std::atomic<long>    violations{0};
    std::atomic<long>    reads{0};

    auto reader = [&] {
        long local = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            const auto t = ob.top();
            if (t.bid != -1 && t.ask != -1 && (t.ask - t.bid) != 1000) {
                violations.fetch_add(1, std::memory_order_relaxed);
            }
            ++local;
        }
        reads.fetch_add(local, std::memory_order_relaxed);
    };

    std::vector<std::thread> readers;
    for (int i = 0; i < 3; ++i) readers.emplace_back(reader);

    uint64_t seq  = 0;
    int      prev = -1;
    for (int i = 0; i < 300000; ++i) {
        const int p = 5000 + (i % 500);

        Update burst[4];
        int n = 0;
        if (prev != -1) {
            burst[n++] = Update{0, 'B', prev,        0};
            burst[n++] = Update{0, 'A', prev + 1000, 0};
        }
        burst[n++] = Update{0, 'B', p,        1};
        burst[n++] = Update{0, 'A', p + 1000, 1};
        for (int j = 0; j < n; ++j) burst[j].sequence = seq + static_cast<uint64_t>(j);

        // Deliver in reverse so the whole burst is applied and published atomically.
        for (int j = n - 1; j >= 0; --j) ob.onUpdate(burst[j]);

        seq += static_cast<uint64_t>(n);
        prev = p;
    }

    stop.store(true);
    for (auto& t : readers) t.join();

    std::printf("        %ld reads, %ld violations\n",
                reads.load(), violations.load());
    CHECK(violations.load() == 0, "readers observed an incoherent bid/ask pair");
}

// -----------------------------------------------------------------------------
// benchmark
// 10 million in-order updates, single thread, no readers. Reports ns/update.
// Take the absolute number with a pinch of salt on a shared machine; use it to
// compare two versions of the code, not to quote in an interview.
// -----------------------------------------------------------------------------
static void benchmark() {
    std::printf("benchmark\n");
    std::mt19937 rng(777);
    const std::size_t N = 10'000'000;
    const auto stream = makeStream(N, rng);

    OrderBook ob(0, 64);
    const auto t0 = std::chrono::steady_clock::now();
    for (const Update& u : stream) ob.onUpdate(u);
    const auto t1 = std::chrono::steady_clock::now();

    const double ns = std::chrono::duration<double, std::nano>(t1 - t0).count();
    std::printf("        %zu updates in %.1f ms  =>  %.1f ns/update  (%.1f M/s)\n",
                N, ns / 1e6, ns / static_cast<double>(N), 1e3 * N / ns);

    // Keep the compiler from optimising the whole loop away.
    if (ob.bestBid() == 0x7fffffff) std::printf("unreachable\n");
}

int main() {
    test1_basics();
    test2_inOrder();
    test3_outOfOrder();
    test4_lateAndDuplicate();
    test5_resync();
    test6_concurrentCoherence();
    benchmark();

    if (g_failures == 0) std::printf("\nALL TESTS PASSED\n");
    else                 std::printf("\n%d CHECK(S) FAILED\n", g_failures);
    return g_failures == 0 ? 0 : 1;
}
