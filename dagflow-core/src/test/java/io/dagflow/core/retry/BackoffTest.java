package io.dagflow.core.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class BackoffTest {

    /** Seeded so jitter is reproducible; an unseeded source would make these tests flaky by design. */
    private final RandomGenerator random = new Random(20240917L);

    @Test
    void noneNeverWaits() {
        assertThat(Backoff.none().delayAfter(1, random)).isEqualTo(Duration.ZERO);
        assertThat(Backoff.none().delayAfter(99, random)).isEqualTo(Duration.ZERO);
    }

    @Test
    void fixedAlwaysWaitsTheSameAmount() {
        Backoff backoff = Backoff.fixed(Duration.ofMillis(250));

        assertThat(backoff.delayAfter(1, random)).isEqualTo(Duration.ofMillis(250));
        assertThat(backoff.delayAfter(5, random)).isEqualTo(Duration.ofMillis(250));
    }

    @Test
    @DisplayName("exponential doubles per attempt when jitter is off")
    void exponentialDoubles() {
        Backoff backoff = Backoff.exponential(Duration.ofMillis(100), Duration.ofMinutes(1), Jitter.NONE);

        assertThat(backoff.delayAfter(1, random)).isEqualTo(Duration.ofMillis(100));
        assertThat(backoff.delayAfter(2, random)).isEqualTo(Duration.ofMillis(200));
        assertThat(backoff.delayAfter(3, random)).isEqualTo(Duration.ofMillis(400));
        assertThat(backoff.delayAfter(4, random)).isEqualTo(Duration.ofMillis(800));
    }

    @Test
    @DisplayName("growth stops at the cap")
    void exponentialRespectsTheCap() {
        Backoff backoff = Backoff.exponential(Duration.ofMillis(100), Duration.ofSeconds(1), Jitter.NONE);

        assertThat(backoff.delayAfter(4, random)).isEqualTo(Duration.ofMillis(800));
        assertThat(backoff.delayAfter(5, random)).isEqualTo(Duration.ofSeconds(1));
        assertThat(backoff.delayAfter(50, random)).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    @DisplayName("a huge attempt count cannot overflow the shift into a nonsense delay")
    void survivesAbsurdAttemptCounts() {
        Backoff backoff = Backoff.exponential(Duration.ofSeconds(1), Duration.ofMinutes(5), Jitter.NONE);

        for (int attempt : new int[] {60, 63, 64, 1_000, Integer.MAX_VALUE}) {
            assertThat(backoff.delayAfter(attempt, random))
                    .as("attempt %d", attempt)
                    .isEqualTo(Duration.ofMinutes(5));
        }
    }

    @ParameterizedTest
    @EnumSource(Jitter.class)
    @DisplayName("no jitter strategy ever exceeds the cap or goes negative")
    void jitterStaysWithinBounds(Jitter jitter) {
        Duration cap = Duration.ofSeconds(10);
        Backoff backoff = Backoff.exponential(Duration.ofMillis(50), cap, jitter);

        for (int attempt = 1; attempt <= 30; attempt++) {
            Duration delay = backoff.delayAfter(attempt, random);
            assertThat(delay).as("attempt %d with %s", attempt, jitter).isBetween(Duration.ZERO, cap);
        }
    }

    @Test
    @DisplayName("full jitter actually spreads retries out — the point of having it")
    void fullJitterProducesASpread() {
        Backoff backoff = Backoff.exponential(Duration.ofSeconds(1), Duration.ofSeconds(10), Jitter.FULL);

        List<Duration> delays = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            // The same attempt number every time: this is 200 callers failing simultaneously, which is
            // exactly the thundering herd the jitter exists to break up.
            delays.add(backoff.delayAfter(5, random));
        }

        assertThat(delays.stream().distinct().count())
                .as("200 simultaneous callers must not all come back at the same instant")
                .isGreaterThan(150);
        assertThat(delays).allSatisfy(d -> assertThat(d).isBetween(Duration.ZERO, Duration.ofSeconds(10)));
    }

    @Test
    @DisplayName("equal jitter keeps a floor of half the delay")
    void equalJitterNeverRetriesImmediately() {
        Backoff backoff = Backoff.exponential(Duration.ofSeconds(4), Duration.ofSeconds(4), Jitter.EQUAL);

        for (int i = 0; i < 200; i++) {
            assertThat(backoff.delayAfter(1, random))
                    .isBetween(Duration.ofSeconds(2), Duration.ofSeconds(4));
        }
    }

    @Test
    @DisplayName("no jitter means every caller returns at the same instant — the herd")
    void noJitterIsPerfectlySynchronised() {
        Backoff backoff = Backoff.exponential(Duration.ofSeconds(1), Duration.ofSeconds(10), Jitter.NONE);

        List<Duration> delays = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            delays.add(backoff.delayAfter(3, random));
        }

        assertThat(delays.stream().distinct().count()).isEqualTo(1);
    }

    @Test
    void rejectsNegativeDelays() {
        assertThatThrownBy(() -> Backoff.fixed(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be negative");
    }

    @Test
    void rejectsACapBelowTheBase() {
        assertThatThrownBy(() ->
                        Backoff.exponential(Duration.ofSeconds(10), Duration.ofSeconds(1), Jitter.FULL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be at least base");
    }

    @Test
    void defaultsAreExponentialWithFullJitterAndAThirtySecondCap() {
        Backoff backoff = Backoff.exponentialDefault();

        for (int attempt = 1; attempt <= 20; attempt++) {
            assertThat(backoff.delayAfter(attempt, random)).isBetween(Duration.ZERO, Duration.ofSeconds(30));
        }
    }
}
