package io.dagflow.core.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;
import java.util.Random;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {

    private final RandomGenerator random = new Random(1L);

    @Test
    @DisplayName("none() means one attempt and no more")
    void noneDoesNotRetry() {
        RetryPolicy policy = RetryPolicy.none();

        assertThat(policy.maxAttempts()).isEqualTo(1);
        assertThat(policy.shouldRetry(1, new RuntimeException())).isFalse();
    }

    @Test
    @DisplayName("maxAttempts counts the first attempt, not the retries after it")
    void maxAttemptsIncludesTheFirstAttempt() {
        RetryPolicy policy = RetryPolicy.exponential(3);

        assertThat(policy.shouldRetry(1, new RuntimeException())).isTrue();
        assertThat(policy.shouldRetry(2, new RuntimeException())).isTrue();
        assertThat(policy.shouldRetry(3, new RuntimeException()))
                .as("three attempts means two retries, then stop")
                .isFalse();
    }

    @Test
    @DisplayName("a permanent failure is not retried even with attempts left")
    void permanentFailuresAreNotRetried() {
        // Retrying an IllegalArgumentException turns one fast failure into several slow ones, and
        // loads a dependency that was never at fault.
        RetryPolicy policy = RetryPolicy.exponential(5).notRetrying(IllegalArgumentException.class);

        assertThat(policy.shouldRetry(1, new IOException("connection reset"))).isTrue();
        assertThat(policy.shouldRetry(1, new IllegalArgumentException("malformed input"))).isFalse();
    }

    @Test
    void notRetryingMatchesSubclasses() {
        RetryPolicy policy = RetryPolicy.exponential(5).notRetrying(RuntimeException.class);

        assertThat(policy.shouldRetry(1, new IllegalStateException())).isFalse();
        assertThat(policy.shouldRetry(1, new IOException())).isTrue();
    }

    @Test
    void retryingOnlyAcceptsAnArbitraryPredicate() {
        RetryPolicy policy = RetryPolicy.exponential(5)
                .retryingOnly(failure -> failure.getMessage() != null && failure.getMessage().contains("retry me"));

        assertThat(policy.shouldRetry(1, new RuntimeException("please retry me"))).isTrue();
        assertThat(policy.shouldRetry(1, new RuntimeException("do not"))).isFalse();
    }

    @Test
    void delaysComeFromTheConfiguredBackoff() {
        RetryPolicy policy = RetryPolicy.of(5, Backoff.fixed(Duration.ofMillis(500)));

        assertThat(policy.delayAfter(1, random)).isEqualTo(Duration.ofMillis(500));
        assertThat(policy.delayAfter(4, random)).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void rejectsFewerThanOneAttempt() {
        assertThatThrownBy(() -> RetryPolicy.of(0, Backoff.none()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 1");
    }
}
