/*
 * Copyright 2017-2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.oauth.server.authorizer;

import io.strimzi.kafka.oauth.common.BearerTokenWithPayload;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class JwtKafkaPrincipal extends KafkaPrincipal {

    private BearerTokenWithPayload jwt;

    public JwtKafkaPrincipal(String principalType, String name) {
        super(principalType, name);
    }

    public JwtKafkaPrincipal(String principalType, String name, BearerTokenWithPayload jwt) {
        this(principalType, name);
        this.jwt = jwt;
    }

    public BearerTokenWithPayload getJwt() {
        return jwt;
    }
}