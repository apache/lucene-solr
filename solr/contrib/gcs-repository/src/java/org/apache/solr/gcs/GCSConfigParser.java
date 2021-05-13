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

package org.apache.solr.gcs;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.storage.StorageOptions;
import org.apache.solr.common.util.NamedList;
import org.threeten.bp.Duration;

import java.util.Map;

/**
 * Parses configuration for {@link GCSBackupRepository} from NamedList and environment variables
 */
public class GCSConfigParser {
  private static final String GCS_BUCKET_ENV_VAR_NAME = "GCS_BUCKET";
  private static final String GCS_CREDENTIAL_ENV_VAR_NAME = "GCS_CREDENTIAL_PATH";

  private static final String GCS_BUCKET_PARAM_NAME = "gcsBucket";
  private static final String GCS_CREDENTIAL_PARAM_NAME = "gcsCredentialPath";
  private static final String GCS_WRITE_BUFFER_SIZE_PARAM_NAME = "gcsWriteBufferSizeBytes";
  private static final String GCS_READ_BUFFER_SIZE_PARAM_NAME = "gcsReadBufferSizeBytes";
  private static final String HTTP_CONNECT_TIMEOUT_MILLIS_NAME = "gcsClientHttpConnectTimeoutMillis";
  private static final String HTTP_READ_TIMEOUT_MILLIS_NAME = "gcsClientHttpReadTimeoutMillis";
  private static final String MAX_REQUEST_RETRIES_NAME = "gcsClientMaxRetries";
  private static final String TOTAL_TIMEOUT_MILLIS_NAME = "gcsClientMaxRequestTimeoutMillis";
  private static final String HTTP_INITIAL_RETRY_DELAY_MILLIS_NAME = "gcsClientHttpInitialRetryDelayMillis";
  private static final String HTTP_SUBSEQUENT_RETRY_DELAY_MULTIPLIER_NAME = "gcsClientHttpRetryDelayMultiplier";
  private static final String HTTP_MAX_RETRY_DELAY_MILLIS_NAME = "gcsClientHttpMaxRetryDelayMillis";
  private static final String RPC_INITIAL_TIMEOUT_MILLIS_NAME = "gcsClientRpcInitialTimeoutMillis";
  private static final String RPC_SUBSEQUENT_TIMEOUT_MULTIPLIER_NAME = "gcsClientRpcTimeoutMultiplier";
  private static final String RPC_MAX_TIMEOUT_MILLIS_NAME = "gcsClientRpcMaxTimeoutMillis";

  private static final String DEFAULT_GCS_BUCKET_VALUE = "solrBackupsBucket";
  private static final int DEFAULT_GCS_WRITE_BUFFER_SIZE_VALUE = 16 * 1024 * 1024;
  private static final int DEFAULT_GCS_READ_BUFFER_SIZE_VALUE = 2 * 1024 * 1024;
  private static final int DEFAULT_HTTP_CONNECT_TIMEOUT_MILLIS = 20000;
  private static final int DEFAULT_HTTP_READ_TIMEOUT_MILLIS = 20000;
  private static final int DEFAULT_MAX_RETRIES = 10;
  private static final int DEFAULT_TOTAL_TIMEOUT_MILLIS = 300000;
  private static final int DEFAULT_HTTP_INITIAL_RETRY_DELAY_MILLIS = 1000;
  private static final int DEFAULT_HTTP_MAX_RETRY_DELAY_MILLIS = 30000;
  private static final double DEFAULT_HTTP_SUBSEQUENT_RETRY_DELAY_MULTIPLIER = 1.0;
  private static final int DEFAULT_RPC_INITIAL_TIMEOUT_MILLIS = 10000;
  private static final int DEFAULT_RPC_MAX_TIMEOUT_MILLIS = 30000;
  private static final double DEFAULT_RPC_SUBSEQUENT_TIMEOUT_MULTIPLIER = 1.0;

  public GCSConfig parseConfiguration(NamedList<Object> repositoryConfig) {
    return parseConfiguration(repositoryConfig, System.getenv());
  }

  public GCSConfig parseConfiguration(NamedList<Object> repoConfig, Map<String, String> envVars) {
    final String bucketName = parseBucket(repoConfig, envVars);
    final String credentialPathStr = parseCredentialPath(repoConfig, envVars);
    final int writeBufferSizeBytes = getIntOrDefault(repoConfig, GCS_WRITE_BUFFER_SIZE_PARAM_NAME, DEFAULT_GCS_WRITE_BUFFER_SIZE_VALUE);
    final int readBufferSizeBytes = getIntOrDefault(repoConfig, GCS_READ_BUFFER_SIZE_PARAM_NAME, DEFAULT_GCS_READ_BUFFER_SIZE_VALUE);
    final StorageOptions.Builder storageOptionsBuilder = parseStorageOptions(repoConfig);
    return new GCSConfig(bucketName, credentialPathStr, writeBufferSizeBytes, readBufferSizeBytes, storageOptionsBuilder);
  }

  /*
   * The GCS bucket name is retrieved from the NL 'bucket' key, or the 'GCS_BUCKET' env-var, or the value
   * 'solrBackupsBucket' is used (in that order).
   */
  private String parseBucket(NamedList<Object> repoConfig, Map<String, String> envVars) {
    if (repoConfig.get(GCS_BUCKET_PARAM_NAME) != null) {
      return repoConfig.get(GCS_BUCKET_PARAM_NAME).toString();
    }

    return envVars.getOrDefault(GCS_BUCKET_ENV_VAR_NAME, DEFAULT_GCS_BUCKET_VALUE);
  }

  private String parseCredentialPath(NamedList<Object> repoConfig, Map<String, String> envVars) {
    if (repoConfig.get(GCS_CREDENTIAL_PARAM_NAME) != null) {
      return repoConfig.get(GCS_CREDENTIAL_PARAM_NAME).toString();
    }

    return envVars.get(GCS_CREDENTIAL_ENV_VAR_NAME);
  }

  public static String missingCredentialErrorMsg() {
    return "GCSBackupRepository requires a credential for GCS communication, but none was provided.  Please specify a " +
            "path to this GCS credential by adding a '" + GCS_CREDENTIAL_PARAM_NAME + "' property to the repository " +
            "definition in your solrconfig, or by setting the path value in an env-var named '" +
            GCS_CREDENTIAL_ENV_VAR_NAME + "'";
  }

  private int getIntOrDefault(NamedList<Object> config, String propName, int defaultValue) {
    if (config.get(propName) != null) {
      return Integer.parseInt(config.get(propName).toString());
    }
    return defaultValue;
  }

  private double getDoubleOrDefault(NamedList<Object> config, String propName, double defaultValue) {
    if (config.get(propName) != null) {
      return Double.parseDouble(config.get(propName).toString());
    }
    return defaultValue;
  }

  private StorageOptions.Builder parseStorageOptions(NamedList<Object> repoConfig) {
    final StorageOptions.Builder builder = StorageOptions.newBuilder();
    builder.setTransportOptions(StorageOptions.getDefaultHttpTransportOptions().toBuilder()
            .setConnectTimeout(getIntOrDefault(repoConfig, HTTP_CONNECT_TIMEOUT_MILLIS_NAME, DEFAULT_HTTP_CONNECT_TIMEOUT_MILLIS))
            .setReadTimeout(getIntOrDefault(repoConfig, HTTP_READ_TIMEOUT_MILLIS_NAME, DEFAULT_HTTP_READ_TIMEOUT_MILLIS))
            .build());
    builder.setRetrySettings(RetrySettings.newBuilder()
                    // All retries
                    .setMaxAttempts(getIntOrDefault(repoConfig, MAX_REQUEST_RETRIES_NAME, DEFAULT_MAX_RETRIES))
                    .setTotalTimeout(Duration.ofMillis(getIntOrDefault(repoConfig, TOTAL_TIMEOUT_MILLIS_NAME, DEFAULT_TOTAL_TIMEOUT_MILLIS)))
                    //http requests
                    .setInitialRetryDelay(Duration.ofMillis(getIntOrDefault(repoConfig, HTTP_INITIAL_RETRY_DELAY_MILLIS_NAME, DEFAULT_HTTP_INITIAL_RETRY_DELAY_MILLIS)))
                    .setMaxRetryDelay(Duration.ofMillis(getIntOrDefault(repoConfig, HTTP_MAX_RETRY_DELAY_MILLIS_NAME, DEFAULT_HTTP_MAX_RETRY_DELAY_MILLIS)))
                    .setRetryDelayMultiplier(getDoubleOrDefault(repoConfig, HTTP_SUBSEQUENT_RETRY_DELAY_MULTIPLIER_NAME, DEFAULT_HTTP_SUBSEQUENT_RETRY_DELAY_MULTIPLIER))
                    //rpc requests
                    .setInitialRpcTimeout(Duration.ofMillis(getIntOrDefault(repoConfig, RPC_INITIAL_TIMEOUT_MILLIS_NAME, DEFAULT_RPC_INITIAL_TIMEOUT_MILLIS)))
                    .setMaxRpcTimeout(Duration.ofMillis(getIntOrDefault(repoConfig, RPC_MAX_TIMEOUT_MILLIS_NAME, DEFAULT_RPC_MAX_TIMEOUT_MILLIS)))
                    .setRpcTimeoutMultiplier(getDoubleOrDefault(repoConfig, RPC_SUBSEQUENT_TIMEOUT_MULTIPLIER_NAME, DEFAULT_RPC_SUBSEQUENT_TIMEOUT_MULTIPLIER))
                    .build());
    return builder;
  }


  public static class GCSConfig {
    private final StorageOptions.Builder optionsBuilder;
    private final String bucketName;
    private final String gcsCredentialPath;
    private final int writeBufferSizeBytes;
    private final int readBufferSizeBytes;

    public GCSConfig(String bucketName, String gcsCredentialPath, int writeBufferSizeBytes, int readBufferSizeBytes, StorageOptions.Builder optionsBuilder) {
      this.bucketName = bucketName;
      this.gcsCredentialPath = gcsCredentialPath;
      this.writeBufferSizeBytes = writeBufferSizeBytes;
      this.readBufferSizeBytes = readBufferSizeBytes;
      this.optionsBuilder = optionsBuilder;
    }

    public StorageOptions.Builder getStorageOptionsBuilder() {
      return optionsBuilder;
    }

    public String getBucketName() {
      return bucketName;
    }

    public String getCredentialPath() {
      return gcsCredentialPath;
    }

    public int getWriteBufferSize() {
      return writeBufferSizeBytes;
    }

    public int getReadBufferSize() {
      return readBufferSizeBytes;
    }
  }
}
