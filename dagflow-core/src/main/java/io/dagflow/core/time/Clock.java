package io.dagflow.core.time;

import java.time.Duration;
import java.time.Instant;

/**
 * The scheduler's view of time and of waiting.
 *
 * <p>Both halves are here on purpose. A scheduler does not only read the clock, it <em>sleeps</em> —
 * between retries, while polling for work, while waiting on a lease. Injecting {@link #now()} but
 * leaving {@code Thread.sleep} scattered through the code leaves the test suite waiting in real time
 * for backoffs that exist precisely to be long.
 *
 * <p>With both behind one interface, a test can run a five-attempt retry with a 30-second cap
 * instantly, and assert on the exact delays that were requested rather than on how long the test took.
 */
public interface Clock {

    /**
     * @return the current wall-clock instant, used for timestamps, leases and deadlines
     */
    Instant now();

    /**
     * Blocks for {@code duration}.
     *
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    void sleep(Duration duration) throws InterruptedException;

    /**
     * @return a clock backed by the system clock and {@link Thread#sleep}
     */
    static Clock system() {
        return new Clock() {
            @Override
            public Instant now() {
                return Instant.now();
            }

            @Override
            public void sleep(Duration duration) throws InterruptedException {
                if (duration.isNegative() || duration.isZero()) {
                    return;
                }
                Thread.sleep(duration.toMillis(), duration.toNanosPart() % 1_000_000);
            }
        };
    }
}
