package io.dagflow.core.retry;

import java.time.Duration;
import java.util.Objects;
import java.util.random.RandomGenerator;

/**
 * How long to wait before the next attempt.
 *
 * <p>The choice matters more than it looks, because retries are how a partial outage becomes a total
 * one. When a dependency starts failing, every caller retries; if they all retry on the same schedule
 * they arrive together, and the dependency — which might have recovered under a trickle — is knocked
 * over again by a synchronised wave. That is the thundering herd, and jitter is the fix.
 *
 * <p>See {@link Jitter} for which flavour to use and why.
 */
@FunctionalInterface
public interface Backoff {

    /**
     * @param attempt the attempt that just failed, counting from 1
     * @param random source of randomness for jittered strategies
     * @return how long to wait before the next attempt; never negative
     */
    Duration delayAfter(int attempt, RandomGenerator random);

    /**
     * No delay at all.
     *
     * <p>Appropriate only when the failure cannot be caused by load — an optimistic-locking conflict,
     * say, where retrying immediately against fresh state is exactly right. Never use it for a failing
     * network call.
     */
    static Backoff none() {
        return (attempt, random) -> Duration.ZERO;
    }

    /**
     * The same delay every time.
     *
     * <p>Simple and predictable, and the classic thundering-herd generator: every failed caller comes
     * back at the same instant.
     */
    static Backoff fixed(Duration delay) {
        Objects.requireNonNull(delay, "delay");
        requireNonNegative(delay);
        return (attempt, random) -> delay;
    }

    /**
     * Exponential growth, capped, with jitter.
     *
     * <p>The default and the right choice for almost anything that talks to a network. The delay
     * doubles per attempt so a persistently failing dependency is left alone quickly, the cap stops it
     * growing to something absurd, and the jitter spreads the retries out.
     *
     * @param base delay after the first failure
     * @param max ceiling on the un-jittered delay
     * @param jitter how to randomise; see {@link Jitter}
     */
    static Backoff exponential(Duration base, Duration max, Jitter jitter) {
        Objects.requireNonNull(base, "base");
        Objects.requireNonNull(max, "max");
        Objects.requireNonNull(jitter, "jitter");
        requireNonNegative(base);
        requireNonNegative(max);
        if (max.compareTo(base) < 0) {
            throw new IllegalArgumentException("max (" + max + ") must be at least base (" + base + ")");
        }

        return (attempt, random) -> {
            long capNanos = max.toNanos();
            long baseNanos = base.toNanos();

            // Shift rather than Math.pow: exact, and the guard stops a large attempt count from
            // shifting past 63 bits into nonsense. Anything above the cap is the cap anyway.
            int exponent = Math.min(attempt - 1, 62);
            long grown = baseNanos > 0 && exponent < 63 && baseNanos <= capNanos >> Math.min(exponent, 62)
                    ? baseNanos << exponent
                    : capNanos;
            long ceiling = Math.min(grown, capNanos);

            return Duration.ofNanos(jitter.apply(ceiling, random));
        };
    }

    /**
     * Exponential backoff with full jitter and sensible defaults: 100ms base, 30s cap.
     */
    static Backoff exponentialDefault() {
        return exponential(Duration.ofMillis(100), Duration.ofSeconds(30), Jitter.FULL);
    }

    private static void requireNonNegative(Duration duration) {
        if (duration.isNegative()) {
            throw new IllegalArgumentException("delay must not be negative, got " + duration);
        }
    }
}
