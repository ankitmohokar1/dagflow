package io.dagflow.core.retry;

import java.util.random.RandomGenerator;

/**
 * How much randomness to mix into a backoff delay.
 *
 * <p>The names and behaviour follow the analysis AWS published in "Exponential Backoff and Jitter",
 * which measured these against each other rather than reasoning about them.
 */
public enum Jitter {

    /**
     * None: wait exactly the computed delay.
     *
     * <p>Included for completeness and for tests that need determinism. In production this is what
     * produces synchronised retry waves — every caller that failed at the same moment comes back at
     * the same moment.
     */
    NONE {
        @Override
        long apply(long ceilingNanos, RandomGenerator random) {
            return ceilingNanos;
        }
    },

    /**
     * Uniform over {@code [0, delay]}.
     *
     * <p>The best general-purpose choice, and the default here. It spreads retries maximally, which
     * both minimises collisions and — less obviously — reduces total work done, because callers that
     * draw a short delay discover recovery sooner and the rest never retry at all.
     *
     * <p>The objection is that a caller can draw a near-zero delay and hammer immediately. In
     * aggregate that does not matter: the population is spread, which is the property that protects
     * the dependency.
     */
    FULL {
        @Override
        long apply(long ceilingNanos, RandomGenerator random) {
            return ceilingNanos <= 0 ? 0 : random.nextLong(ceilingNanos + 1);
        }
    },

    /**
     * Uniform over {@code [delay/2, delay]}.
     *
     * <p>A compromise: still spread, but with a floor, so no caller retries immediately. Slightly
     * worse than {@link #FULL} on contention in AWS's measurements, and worth choosing when a
     * guaranteed minimum wait matters more — for instance when each attempt is itself expensive.
     */
    EQUAL {
        @Override
        long apply(long ceilingNanos, RandomGenerator random) {
            if (ceilingNanos <= 0) {
                return 0;
            }
            long half = ceilingNanos / 2;
            return half + random.nextLong(ceilingNanos - half + 1);
        }
    };

    /**
     * @param ceilingNanos the un-jittered delay
     * @return the delay to actually wait, in nanoseconds
     */
    abstract long apply(long ceilingNanos, RandomGenerator random);
}
