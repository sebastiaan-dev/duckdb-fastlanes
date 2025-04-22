#pragma once

#include <chrono>
#include <cstdint>

struct State {
public:
    explicit State(const uint32_t iterations)
        : n_iterations(this)
          , n(iterations) {
    }

    using clk = std::chrono::high_resolution_clock;

    [[nodiscard]] long long get_elapsed_time() const {
        return elapsed.count() / n;
    };

    //! Iterator which times the total duration of all iterations.
    struct Iter {
        State *parent;
        uint32_t idx;

        Iter(State *p, const uint32_t start_idx)
            : parent(p), idx(start_idx) {
        }

        // Define the termination condition.
        bool operator!=(Iter const &o) const {
            return idx != o.idx;
        }

        // When this is the first iteration, we set the start time.
        uint32_t operator*() const {
            if (idx == 0) {
                parent->start_time = clk::now();
            }
            return idx;
        }

        // After each iteration we increment the internal state by 1, if we are at the final iteration
        // we stop the timer and calculate the total amount of time.
        Iter &operator++() {
            ++idx;
            if (idx == parent->n) {
                const auto end = clk::now();
                parent->elapsed =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            end - parent->start_time
                        );
            }
            return *this;
        }
    };

    //! Define methods which the compiler can call when used in a for loop.
    struct Range {
        State *parent;

        explicit Range(State *p): parent(p) {
        }

        [[nodiscard]] Iter begin() const { return {parent, 0}; }
        [[nodiscard]] Iter end() const { return {parent, parent->n}; }
    };

public:
    //! Iterator which is used to benchmark a piece of code.
    Range n_iterations;

private:
    //! The amount of iterations when the benchmark is considered completed.
    uint32_t n;
    //! Time when the first iteration is called.
    clk::time_point start_time;
    //! Time from start to end in nanoseconds.
    std::chrono::nanoseconds elapsed{0};
};

template<typename Func, typename... Args>
static long long Bench(Func &&func, const uint32_t n, Args &&... args) {
    State state(n);
    std::invoke(std::forward<Func>(func), std::forward<State>(state), std::forward<Args>(args)...);
    return state.get_elapsed_time();
}