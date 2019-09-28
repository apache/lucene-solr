/*
 * Copyright (C) 2012-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.packagemanager.pf4j;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Interface to verify a file.
 */
public interface FileVerifier {

    /**
     * Verifies a plugin release according to certain rules
     *
     * @param context the file verifier context object
     * @param file    the path to the downloaded file itself
     * @throws IOException     if there was a problem accessing file
     * @throws VerifyException in case of problems verifying the file
     */
    void verify(Context context, Path file) throws IOException, VerifyException;

    /**
     * Context to be passed to file verifiers
     */
    class Context {
        public String id;
        public Date date;
        public String version;
        public String requires;
        public String url;
        public String sha512sum;
        public Map<String,Object> meta = new HashMap<>();

        public Context(String id, PluginInfo.PluginRelease pluginRelease) {
            this.id = id;
            this.date = pluginRelease.date;
            this.version = pluginRelease.version;
            this.requires = pluginRelease.requires;
            this.url = pluginRelease.url;
            this.sha512sum = pluginRelease.sha512sum;
        }

        public Context(String id, Date date, String version, String requires, String url, String sha512sum) {
            this.id = id;
            this.date = date;
            this.version = version;
            this.requires = requires;
            this.url = url;
            this.sha512sum = sha512sum;
        }
    }
}
