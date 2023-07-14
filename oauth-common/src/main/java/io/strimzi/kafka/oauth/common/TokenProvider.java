/*
 * Copyright 2017-2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.oauth.common;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * An interface with TokenProvider contract.
 */
public interface TokenProvider {

    /**
     * This method return an access token to be used to authenticate to the Kafka broker.
     *
     * @return An access token
     */
    String token();

    /**
     * A TokenProvider implementation that returns a single access token passed at creation time.
     */
    class StaticTokenProvider implements TokenProvider {
        private final String token;

        /**
         * Create a new instance
         *
         * @param token An access token to return when {@link #token()} is called.
         */
        public StaticTokenProvider(final String token) {
            this.token = token;
        }

        @Override
        public String token() {
            return token;
        }
    }

    /**
     * A TokenProvider implementation that return an access token read from a file on a local filesystem.
     */
    class FileBasedTokenProvider implements TokenProvider {
        private final Path filePath;

        /**
         * Create a new instance
         *
         * @param tokenFilePath A path to a file on a local disk that will be used to read an access token
         */
        public FileBasedTokenProvider(final String tokenFilePath) {
            this.filePath = Paths.get(tokenFilePath);
            if (!filePath.toFile().exists()) {
                throw new IllegalArgumentException("file '" + filePath + "' does not exist!");
            }
            if (!filePath.toFile().isFile()) {
                throw new IllegalArgumentException("'" + filePath + "' does not point to a file!");
            }
        }

        @Override
        public String token() {
            try {
                return new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
