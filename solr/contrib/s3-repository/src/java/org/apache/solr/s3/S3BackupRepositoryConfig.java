/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.s3;

import java.util.Locale;
import org.apache.solr.common.util.NamedList;

/**
 * Class representing the {@code backup} S3 config bundle specified in solr.xml. All user-provided
 * config can be overridden via environment variables (use uppercase, with '_' instead of '.'), see
 * {@link S3BackupRepositoryConfig#toEnvVar}.
 */
public class S3BackupRepositoryConfig {

  public static final String PROFILE = "s3.profile";
  public static final String BUCKET_NAME = "s3.bucket.name";
  public static final String REGION = "s3.region";
  public static final String ENDPOINT = "s3.endpoint";
  public static final String PROXY_URL = "s3.proxy.url";
  public static final String PROXY_USE_SYSTEM_SETTINGS = "s3.proxy.useSystemSettings";
  public static final String RETRIES_DISABLE = "s3.retries.disable";

  private final String profile;
  private final String bucketName;
  private final String region;
  private final String proxyURL;
  private final boolean proxyUseSystemSettings;
  private final String endpoint;
  private final boolean disableRetries;

  public S3BackupRepositoryConfig(NamedList<?> config) {
    profile = getStringConfig(config, PROFILE);
    region = getStringConfig(config, REGION);
    bucketName = getStringConfig(config, BUCKET_NAME);
    proxyURL = getStringConfig(config, PROXY_URL);
    proxyUseSystemSettings = getBooleanConfig(config, PROXY_USE_SYSTEM_SETTINGS, true);
    endpoint = getStringConfig(config, ENDPOINT);
    disableRetries = getBooleanConfig(config, RETRIES_DISABLE, false);
  }

  /** Construct a {@link S3StorageClient} from the provided config. */
  public S3StorageClient buildClient() {
    return new S3StorageClient(
        bucketName, profile, region, proxyURL, proxyUseSystemSettings, endpoint, disableRetries);
  }

  private static String getStringConfig(NamedList<?> config, String property) {
    String envProp = System.getenv().get(toEnvVar(property));
    if (envProp == null) {
      Object configProp = config.get(property);
      return configProp == null ? null : configProp.toString();
    } else {
      return envProp;
    }
  }

  private static int getIntConfig(NamedList<?> config, String property) {
    return getIntConfig(config, property, 0);
  }

  private static int getIntConfig(NamedList<?> config, String property, int def) {
    String envProp = System.getenv().get(toEnvVar(property));
    if (envProp == null) {
      Object configProp = config.get(property);
      return configProp instanceof Integer ? (int) configProp : def;
    } else {
      return Integer.parseInt(envProp);
    }
  }

  /** If the property as any other value than 'true' or 'TRUE', this will default to false. */
  private static boolean getBooleanConfig(NamedList<?> config, String property) {
    return getBooleanConfig(config, property, false);
  }

  private static boolean getBooleanConfig(NamedList<?> config, String property, boolean def) {
    String envProp = System.getenv().get(toEnvVar(property));
    if (envProp == null) {
      Boolean configProp = config.getBooleanArg(property);
      return configProp == null ? def : configProp;
    } else {
      return Boolean.parseBoolean(envProp);
    }
  }

  private static String toEnvVar(String property) {
    return property.toUpperCase(Locale.ROOT).replace('.', '_');
  }
}
