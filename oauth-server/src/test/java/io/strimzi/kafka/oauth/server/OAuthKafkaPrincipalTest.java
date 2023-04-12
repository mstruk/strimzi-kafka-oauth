/*
 * Copyright 2017-2020, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.oauth.server;

import com.fasterxml.jackson.databind.JsonNode;
import io.strimzi.kafka.oauth.common.BearerTokenWithPayload;
import io.strimzi.kafka.oauth.common.JSONUtil;
import io.strimzi.kafka.oauth.common.TokenInfo;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class OAuthKafkaPrincipalTest {

    @Test
    public void testEquals() {

        BearerTokenWithPayload token = new MockBearerTokenWithPayload("service-account-my-client", new HashSet<>(Arrays.asList("group1", "group2")),
                System.currentTimeMillis(), System.currentTimeMillis() + 60000, null, "BEARER-TOKEN-9823eh982u", JSONUtil.asJson("{}"));
        OAuthKafkaPrincipal principal = new OAuthKafkaPrincipal("User", "service-account-my-client", token);


        BearerTokenWithPayload token2 = new MockBearerTokenWithPayload("bob", Collections.emptySet(),
                System.currentTimeMillis(), System.currentTimeMillis() + 60000, null, "BEARER-TOKEN-0000dd0000", null);
        OAuthKafkaPrincipal principal2 = new OAuthKafkaPrincipal("User", "service-account-my-client", token2);


        OAuthKafkaPrincipal principal3 = new OAuthKafkaPrincipal("User", "service-account-my-client");

        OAuthKafkaPrincipal principal4 = new OAuthKafkaPrincipal("User", "bob");


        Assert.assertEquals("principal should be equal to principal2", principal, principal2);
        Assert.assertEquals("principal2 should be equal to principal", principal2, principal);

        Assert.assertEquals("principal should be equal to principal3", principal, principal3);
        Assert.assertEquals("principal3 should be equal to principal", principal3, principal);

        Assert.assertEquals("principal2 should be equal to principal3", principal2, principal3);
        Assert.assertEquals("principal3 should be equal to principal2", principal3, principal2);

        Assert.assertEquals("principal should be equal to itself", principal, principal);
        Assert.assertEquals("principal2 should be equal to itself", principal2, principal2);
        Assert.assertEquals("principal3 should be equal to itself", principal3, principal3);
        Assert.assertEquals("principal4 should be equal to itself", principal4, principal4);

        Assert.assertNotEquals("principal should not be equal to principal4", principal, principal4);
        Assert.assertNotEquals("principal4 should not be equal to principal", principal4, principal);
        Assert.assertNotEquals("principal3 should not be equal to principal4", principal3, principal4);
        Assert.assertNotEquals("principal4 should not be equal to principal3", principal4, principal3);

        long hash1 = principal.hashCode();
        long hash2 = principal2.hashCode();
        long hash3 = principal3.hashCode();
        long hash4 = principal4.hashCode();

        Assert.assertEquals("Hashcode1 should be equal to hashcode2", hash1, hash2);
        Assert.assertEquals("Hashcode1 should be equal to hashcode3", hash1, hash3);
        Assert.assertNotEquals("Hashcode1 should not be equal to hashcode4", hash1, hash4);
    }

    @Test
    public void testJwtPrincipal() throws IOException {
        String rawToken = "token-ignored";
        String json = "{\"sub\": \"bob\", \"iss\": \"http://localhost/\", \"iat\": \"1635721200\", \"exp\": \"1667257200\"}";

        JsonNode parsed = JSONUtil.readJSON(json, JsonNode.class);
        TokenInfo tki = new TokenInfo(parsed, rawToken, "bob");
        BearerTokenWithPayload jwt = new BearerTokenWithGrants(tki);
        OAuthKafkaPrincipal principalJwt = new OAuthKafkaPrincipal("User", "bob", jwt);

        Assert.assertEquals("Can access parsed JWT", parsed, principalJwt.getJwt().getJSON());
    }
}
