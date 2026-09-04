package io.dagflow.core.time;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@link Clock} that never really waits: {@link #sleep} advances the clock instead, and records
 * what it was asked to wait for.
 *
 * <p>That recording is the point. It turns "did the backoff behave correctly" from a timing
 * observation — inherently flaky, and slow in proportion to how correct the backoff is — into an
 * assertion on an exact list of durations. A test can drive five attempts of a 30-second-capped
 * exponential backoff in microseconds and check every delay.
 *
 * <p>This class is thread-safe, because the engine sleeps on worker threads.
 */
public final class TestClock implements Clock {

    private final List<Duration> sleeps = Collections.synchronizedList(new ArrayList<>());
    private volatile Instant now;

    public TestClock() {
        this(Instant.parse("2024-01-01T00:00:00Z"));
    }

    public TestClock(Instant start) {
        this.now = start;
    }

    @Override
    public Instant now() {
        return now;
    }

    /**
     * Records the request and advances the clock, without blocking.
     */
    @Override
    public void sleep(Duration duration) {
        sleeps.add(duration);
        // Duration.isPositive() is Java 18; this module targets 17.
        if (!duration.isNegative() && !duration.isZero()) {
            advance(duration);
        }
    }

    /**
     * Moves the clock forward without recording a sleep.
     */
    public synchronized TestClock advance(Duration duration) {
        if (duration.isNegative()) {
            throw new IllegalArgumentException("clock must not go backwards; got " + duration);
        }
        now = now.plus(duration);
        return this;
    }

    /**
     * @return every duration passed to {@link #sleep}, in order
     */
    public List<Duration> sleeps() {
        synchronized (sleeps) {
            return List.copyOf(sleeps);
        }
    }

    /**
     * @return how many times {@link #sleep} was called with a positive duration
     */
    public long sleepCount() {
        synchronized (sleeps) {
            return sleeps.stream().filter(d -> !d.isNegative() && !d.isZero()).count();
        }
    }

    public void reset() {
        sleeps.clear();
    }
}
