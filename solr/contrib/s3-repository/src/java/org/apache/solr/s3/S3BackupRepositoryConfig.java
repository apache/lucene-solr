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

  public static final String BUCKET_NAME = "s3.bucket.name";
  public static final String REGION = "s3.region";
  public static final String ENDPOINT = "s3.endpoint";
  public static final String PROXY_HOST = "s3.proxy.host";
  public static final String PROXY_PORT = "s3.proxy.port";

  private final String bucketName;
  private final String region;
  private final String proxyHost;
  private final int proxyPort;
  private final String endpoint;

  public S3BackupRepositoryConfig(NamedList<?> config) {
    region = getStringConfig(config, REGION);
    bucketName = getStringConfig(config, BUCKET_NAME);
    proxyHost = getStringConfig(config, PROXY_HOST);
    proxyPort = getIntConfig(config, PROXY_PORT);
    endpoint = getStringConfig(config, ENDPOINT);
  }

  /** Construct a {@link S3StorageClient} from the provided config. */
  public S3StorageClient buildClient() {
    return new S3StorageClient(bucketName, region, proxyHost, proxyPort, endpoint);
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
    String envProp = System.getenv().get(toEnvVar(property));
    if (envProp == null) {
      Object configProp = config.get(property);
      return configProp instanceof Integer ? (int) configProp : 0;
    } else {
      return Integer.parseInt(envProp);
    }
  }

  /** If the property as any other value than 'true' or 'TRUE', this will default to false. */
  private static boolean getBooleanConfig(NamedList<?> config, String property) {
    String envProp = System.getenv().get(toEnvVar(property));
    if (envProp == null) {
      Boolean configProp = config.getBooleanArg(property);
      return configProp != null && configProp;
    } else {
      return Boolean.parseBoolean(envProp);
    }
  }

  private static String toEnvVar(String property) {
    return property.toUpperCase(Locale.ROOT).replace('.', '_');
  }
}
