/*
 * Copyright 2017-2023, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.oauth.server.authorizer;

import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.kafka.oauth.common.BearerTokenWithPayload;
import io.strimzi.kafka.oauth.common.HttpException;
import io.strimzi.kafka.oauth.common.JSONUtil;
import io.strimzi.kafka.oauth.services.ServiceException;
import io.strimzi.kafka.oauth.services.Services;
import io.strimzi.kafka.oauth.validator.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static io.strimzi.kafka.oauth.common.LogUtil.mask;

@SuppressFBWarnings("THROWS_METHOD_THROWS_CLAUSE_BASIC_EXCEPTION")
class GrantsHandler implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(GrantsHandler.class);

    private final HashMap<String, Info> grantsCache = new HashMap<>();

    private final Semaphores<JsonNode> semaphores = new Semaphores<>();

    private final ExecutorService workerPool;

    private final ScheduledExecutorService gcWorker;

    private final long gcPeriodMillis;

    private final Function<String, JsonNode> authorizationGrantsProvider;

    private final int httpRetries;

    private final long grantsMaxIdleMillis;

    private long lastGcRunTimeMillis;

    @Override
    public void close() {
        try {
            workerPool.shutdownNow();
        } catch (Throwable t) {
            log.error("[IGNORED] Failed to normally shutdown worker pool: ", t);
        }
        try {
            gcWorker.shutdownNow();
        } catch (Throwable t) {
            log.error("[IGNORED] Failed to normally shutdown garbage collector worker: ", t);
        }
    }

    static class Info {
        private volatile String accessToken;
        private volatile JsonNode grants;
        private volatile long expiresAt;
        private volatile long lastUsed;

        Info(String accessToken, long expiresAt) {
            this.accessToken = accessToken;
            this.expiresAt = expiresAt;
            this.lastUsed = System.currentTimeMillis();
        }

        synchronized void updateTokenIfNewer(BearerTokenWithPayload token) {
            lastUsed = System.currentTimeMillis();
            if (token.lifetimeMs() > expiresAt) {
                accessToken = token.value();
                expiresAt = token.lifetimeMs();
            }
        }

        String getAccessToken() {
            return accessToken;
        }

        JsonNode getGrants() {
            return grants;
        }

        void setGrants(JsonNode newGrants) {
            grants = newGrants;
        }

        long getLastUsed() {
            return lastUsed;
        }

        boolean isExpiredAt(long timestamp) {
            return expiresAt < timestamp;
        }
    }

    static class Future implements java.util.concurrent.Future<JsonNode> {

        private final java.util.concurrent.Future<JsonNode> delegate;
        private final String userId;
        private final Info grantsInfo;

        /**
         * Create a new instance
         *
         * @param future Original future instance to wrap
         */
        @SuppressFBWarnings("EI_EXPOSE_REP2")
        public Future(String userId, GrantsHandler.Info grantsInfo, java.util.concurrent.Future<JsonNode> future) {
            this.userId = userId;
            this.grantsInfo = grantsInfo;
            this.delegate = future;
        }

        /**
         * Get a <code>BearerTokenWithPayload</code> object representing a session
         *
         * @return A token instance
         */
        @SuppressFBWarnings("EI_EXPOSE_REP")
        public GrantsHandler.Info getGrantsInfo() {
            return grantsInfo;
        }

        public String getUserId() {
            return userId;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return delegate.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return delegate.isCancelled();
        }

        @Override
        public boolean isDone() {
            return delegate.isDone();
        }

        @Override
        public JsonNode get() throws InterruptedException, ExecutionException {
            return delegate.get();
        }

        @Override
        public JsonNode get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return delegate.get(timeout, unit);
        }
    }

    GrantsHandler(int grantsRefreshPeriodSeconds, int grantsRefreshPoolSize, int grantsMaxIdleTimeSeconds, Function<String, JsonNode> httpGrantsProvider, int httpRetries, int gcPeriodSeconds) {
        this.authorizationGrantsProvider = httpGrantsProvider;
        this.httpRetries = httpRetries;

        if (grantsMaxIdleTimeSeconds <= 0) {
            throw new IllegalArgumentException("grantsMaxIdleTimeSeconds <= 0");
        }
        this.grantsMaxIdleMillis = grantsMaxIdleTimeSeconds * 1000L;

        DaemonThreadFactory daemonThreadFactory = new DaemonThreadFactory();
        if (grantsRefreshPeriodSeconds > 0) {
            this.workerPool = Executors.newFixedThreadPool(grantsRefreshPoolSize, daemonThreadFactory);
            setupRefreshGrantsJob(daemonThreadFactory, grantsRefreshPeriodSeconds);
        } else {
            this.workerPool = null;
        }

        if (gcPeriodSeconds <= 0) {
            throw new IllegalArgumentException("gcPeriodSeconds <= 0");
        }
        this.gcPeriodMillis = gcPeriodSeconds * 1000L;
        this.gcWorker = Executors.newSingleThreadScheduledExecutor(daemonThreadFactory);
        gcWorker.scheduleAtFixedRate(this::gcGrantsCacheRunnable, gcPeriodSeconds, gcPeriodSeconds, TimeUnit.SECONDS);
    }

    private void setupRefreshGrantsJob(ThreadFactory threadFactory, int refreshSeconds) {
        // Set up periodic timer to fetch grants for active sessions every refresh seconds
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
        scheduler.scheduleAtFixedRate(this::performRefreshGrantsRun, refreshSeconds, refreshSeconds, TimeUnit.SECONDS);
    }

    private void gcGrantsCacheRunnable() {
        long timePassedSinceGc = System.currentTimeMillis() - lastGcRunTimeMillis;
        if (timePassedSinceGc < gcPeriodMillis - 1000) { // give or take one second
            log.debug("Skipped queued gc run (last run {} ms ago)", timePassedSinceGc);
            return;
        }
        lastGcRunTimeMillis = System.currentTimeMillis();
        gcGrantsCache();
    }

    private void gcGrantsCache() {
        HashSet<String> userIds = new HashSet<>(Services.getInstance().getSessions().map(BearerTokenWithPayload::principalName));
        log.trace("Grants gc: active users: {}", userIds);
        int beforeSize;
        int afterSize;
        synchronized (grantsCache) {
            beforeSize = grantsCache.size();
            // keep the active sessions, remove grants for unknown user ids
            grantsCache.keySet().retainAll(userIds);
            afterSize = grantsCache.size();
        }
        log.debug("Grants gc: active users count: {}, grantsCache size before: {}, grantsCache size after: {}", userIds.size(), beforeSize, afterSize);
    }

    private JsonNode fetchAndSaveGrants(String userId, Info grantsInfo) {
        // If no grants found, fetch grants from server
        JsonNode grants = null;
        try {
            log.debug("Fetching grants from Keycloak for user {}", userId);
            grants = fetchGrantsWithRetry(grantsInfo.getAccessToken());
            if (grants == null) {
                log.debug("Received null grants for user: {}, token: {}", userId, mask(grantsInfo.getAccessToken()));
                grants = JSONUtil.newObjectNode();
            }
        } catch (HttpException e) {
            if (e.getStatus() == 403) {
                grants = JSONUtil.newObjectNode();
            } else {
                log.warn("Unexpected status while fetching authorization data - will retry next time: " + e.getMessage());
            }
        }
        if (grants != null) {
            // Store authz grants in the token, so they are available for subsequent requests
            log.debug("Saving non-null grants for user: {}, token: {}", userId, mask(grantsInfo.getAccessToken()));
            grantsInfo.setGrants(grants);
        }
        return grants;
    }

    /**
     * Method that performs the POST request to fetch grants for the token.
     * In case of a connection failure or a non-200 status response this method immediately retries the request if so configured.
     * <p>
     * Status 401 does not trigger a retry since it is used to signal an invalid token.
     * Status 403 does not trigger a retry either since it signals no permissions.
     *
     * @param token The raw access token
     * @return Grants JSON response
     */
    private JsonNode fetchGrantsWithRetry(String token) {

        int i = 0;
        do {
            i += 1;

            try {
                if (i > 1) {
                    log.debug("Grants request attempt no. " + i);
                }
                return authorizationGrantsProvider.apply(token);

            } catch (Exception e) {
                if (e instanceof HttpException) {
                    int status = ((HttpException) e).getStatus();
                    if (403 == status || 401 == status) {
                        throw e;
                    }
                }

                log.info("Failed to fetch grants on try no. " + i, e);
                if (i > httpRetries) {
                    log.debug("Failed to fetch grants after " + i + " tries");
                    throw e;
                }
            }
        } while (true);
    }

    private void performRefreshGrantsRun() {
        try {
            log.debug("Refreshing authorization grants ...");

            HashMap<String, Info> workmap;
            synchronized (grantsCache) {
                workmap = new HashMap<>(grantsCache);
            }

            Set<Map.Entry<String, Info>> entries = workmap.entrySet();
            List<Future> scheduled = new ArrayList<>(entries.size());
            for (Map.Entry<String, Info> ent : entries) {
                String userId = ent.getKey();
                Info grantsInfo = ent.getValue();
                if (grantsInfo.getLastUsed() < System.currentTimeMillis() - grantsMaxIdleMillis) {
                    log.debug("Skipping refreshing grants for user '{}' due to max idle time.", userId);
                    removeUserFromCacheIfExpiredOrIdle(userId);
                }
                scheduled.add(new Future(userId, grantsInfo, workerPool.submit(() -> {

                    if (log.isTraceEnabled()) {
                        log.trace("Fetch grants for user: " + userId + ", token: " + mask(grantsInfo.getAccessToken()));
                    }

                    JsonNode newGrants;
                    try {
                        newGrants = fetchGrantsWithRetry(grantsInfo.getAccessToken());
                    } catch (HttpException e) {
                        if (403 == e.getStatus()) {
                            // 403 happens when no policy matches the token - thus there are no grants, no permission granted
                            newGrants = JSONUtil.newObjectNode();
                        } else {
                            throw e;
                        }
                    }
                    Object oldGrants = grantsInfo.getGrants();
                    if (!newGrants.equals(oldGrants)) {
                        if (log.isDebugEnabled()) {
                            log.debug("Grants have changed for user: {}; before: {}; after: {}", userId, oldGrants, newGrants);
                        }
                        grantsInfo.setGrants(newGrants);
                    }

                    // Only added here to allow compiler to resolve the lambda as a Callable<JsonNode>
                    return newGrants;
                })));
            }

            for (GrantsHandler.Future f : scheduled) {
                try {
                    f.get();
                } catch (ExecutionException e) {
                    log.warn("[IGNORED] Failed to fetch grants for user: " + e.getMessage(), e);
                    final Throwable cause = e.getCause();
                    if (cause instanceof HttpException) {
                        if (401 == ((HttpException) cause).getStatus()) {
                            Services.getInstance().getSessions().removeAllWithMatchingAccessToken(f.getGrantsInfo().accessToken);
                        }
                    }
                } catch (Throwable e) {
                    log.warn("[IGNORED] Failed to fetch grants for user: " + f.getUserId() + ", token: " + mask(f.getGrantsInfo().accessToken) + " - " + e.getMessage(), e);
                }
            }

        } catch (Throwable t) {
            // Log, but don't rethrow the exception to prevent scheduler cancelling the scheduled job.
            log.error(t.getMessage(), t);
        } finally {
            log.debug("Done refreshing grants");
        }
    }

    private void removeUserFromCacheIfExpiredOrIdle(String userId) {
        synchronized (grantsCache) {
            Info info = grantsCache.get(userId);
            if (info != null) {
                long now = System.currentTimeMillis();
                boolean isIdle = info.getLastUsed() < now - grantsMaxIdleMillis;
                if (isIdle || info.isExpiredAt(now)) {
                    log.debug("Removed user from grants cache due to {}: {}", isIdle ? "'idle'" : "'expired'", userId);
                    grantsCache.remove(userId);
                }
            }
        }
    }

    Info getGrantsInfoFromCache(BearerTokenWithPayload token) {
        Info grantsInfo;

        synchronized (grantsCache) {
            grantsInfo = grantsCache.computeIfAbsent(token.principalName(),
                k -> new Info(token.value(), token.lifetimeMs()));
        }

        // Always keep the latest access token in the cache
        grantsInfo.updateTokenIfNewer(token);
        return grantsInfo;
    }

    JsonNode fetchGrantsForUserOrWaitForDelivery(String userId, Info grantsInfo) {
        // Fetch authorization grants
        Semaphores.SemaphoreResult<JsonNode> semaphore = semaphores.acquireSemaphore(userId);

        // Try to acquire semaphore for fetching grants
        if (semaphore.acquired()) {
            // If acquired
            try {
                JsonNode grants = fetchAndSaveGrants(userId, grantsInfo);
                semaphore.future().complete(grants);
                return grants;

            } catch (Throwable t) {
                semaphore.future().completeExceptionally(t);
                throw t;
            } finally {
                semaphores.releaseSemaphore(userId);
            }

        } else {
            try {
                log.debug("Waiting on another thread to get grants");
                return semaphore.future().get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof ServiceException) {
                    throw (ServiceException) cause;
                } else {
                    throw new ServiceException("ExecutionException waiting for grants result: ", e);
                }
            } catch (InterruptedException e) {
                throw new ServiceException("InterruptedException waiting for grants result: ", e);
            }
        }
    }
}
