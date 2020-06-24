/*
 * Copyright 2017-2020, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.oauth.server.services;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class contains singleton components shared among Kafka Broker sessions
 */
public class Services {

    private static Services services;

    private Sessions sessions;

    private Validators validators = new Validators();

    public static void configure(Map<String, ?> configs) {
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        Sessions sessions = new Sessions(executorService);

        services = new Services(sessions);
    }

    public static Services getInstance() {
        if (services == null) {
            throw new IllegalStateException("Services object has not been properly initialised");
        }
        return services;
    }

    public Validators getValidators() {
        return validators;
    }

    public static boolean isAvailable() {
        return services != null;
    }

    private Services(Sessions sessions) {
        this.sessions = sessions;
    }

    public Sessions getSessions() {
        return sessions;
    }
}