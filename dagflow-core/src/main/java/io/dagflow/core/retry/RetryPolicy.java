package io.dagflow.core.retry;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.random.RandomGenerator;

/**
 * How many times to retry a failed task, how long to wait, and which failures are worth retrying at
 * all.
 *
 * <h2>Not every failure should be retried</h2>
 *
 * This is the part most often skipped. Retrying a timeout is sensible: the dependency may recover.
 * Retrying an {@code IllegalArgumentException} is not: the input will still be malformed in 200ms,
 * and all the retry achieves is turning one fast failure into several slow ones, plus load on a
 * dependency that was never at fault.
 *
 * <p>{@link #retryOn} exists so that distinction is expressible. The default retries everything,
 * because a scheduler cannot know which of a caller's exceptions are permanent — but a policy that
 * names its retryable failures is a better policy.
 *
 * @param maxAttempts total attempts including the first; 1 means no retries
 * @param backoff how long to wait between attempts
 * @param retryOn which failures are worth another attempt
 */
public record RetryPolicy(int maxAttempts, Backoff backoff, Predicate<Throwable> retryOn) {

    public RetryPolicy {
        Objects.requireNonNull(backoff, "backoff");
        Objects.requireNonNull(retryOn, "retryOn");
        if (maxAttempts < 1) {
            throw new IllegalArgumentException(
                    "maxAttempts counts the first attempt, so it must be at least 1, got " + maxAttempts);
        }
    }

    /**
     * No retries: one attempt, and a failure is final.
     *
     * <p>The right default for anything not known to be idempotent. Since execution is at-least-once,
     * a task with side effects that has not been made safe to repeat should not be repeated on purpose
     * as well.
     */
    public static RetryPolicy none() {
        return new RetryPolicy(1, Backoff.none(), throwable -> true);
    }

    /**
     * Exponential backoff with full jitter — 100ms base, 30s cap — over {@code maxAttempts} attempts.
     */
    public static RetryPolicy exponential(int maxAttempts) {
        return new RetryPolicy(maxAttempts, Backoff.exponentialDefault(), throwable -> true);
    }

    public static RetryPolicy of(int maxAttempts, Backoff backoff) {
        return new RetryPolicy(maxAttempts, backoff, throwable -> true);
    }

    /**
     * @return a copy of this policy that only retries failures matching {@code predicate}
     */
    public RetryPolicy retryingOnly(Predicate<Throwable> predicate) {
        return new RetryPolicy(maxAttempts, backoff, Objects.requireNonNull(predicate, "predicate"));
    }

    /**
     * @return a copy of this policy that does not retry the given exception types. Useful for the
     *     common shape: retry everything except the failures you know are permanent.
     */
    @SafeVarargs
    public final RetryPolicy notRetrying(Class<? extends Throwable>... permanent) {
        Class<? extends Throwable>[] types = permanent.clone();
        return retryingOnly(throwable -> {
            for (Class<? extends Throwable> type : types) {
                if (type.isInstance(throwable)) {
                    return false;
                }
            }
            return true;
        });
    }

    /**
     * Decides whether another attempt should be made.
     *
     * @param completedAttempts how many attempts have already failed
     * @param failure what the last attempt threw
     */
    public boolean shouldRetry(int completedAttempts, Throwable failure) {
        return completedAttempts < maxAttempts && retryOn.test(failure);
    }

    /**
     * @param completedAttempts how many attempts have already failed
     * @return how long to wait before the next one
     */
    public Duration delayAfter(int completedAttempts, RandomGenerator random) {
        return backoff.delayAfter(completedAttempts, random);
    }
}
