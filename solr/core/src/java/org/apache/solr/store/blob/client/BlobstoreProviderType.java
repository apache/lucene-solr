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
package org.apache.solr.store.blob.client;

import java.lang.invoke.MethodHandles;

import org.apache.solr.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enum of identifiers for the underlying blob store service
 */
public enum BlobstoreProviderType {
  /** Host's local disk */
  LOCAL_FILE_SYSTEM,
  /** Amazon Web Services - S3 */
  S3,
  /** Google Cloud Storage - GCS */
  GCS;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Blob storage provider is expected to be in this environment variable
   */
  private static final String BLOB_STORAGE_PROVIDER_ENV = "BLOB_STORAGE_PROVIDER";

  /**
   * Reads blob storage from environment variable
   * 
   * @return {@link BlobstoreProviderType} that's configured or
   *         {@link BlobstoreProviderType#LOCAL_FILE_SYSTEM} when blob storage provider environment variable is not set
   */
  public static BlobstoreProviderType getConfiguredProvider() {
    String provider = System.getenv(BLOB_STORAGE_PROVIDER_ENV);

    if (StringUtils.isEmpty(provider)) {
      log.error("BlobstoreProviderType: blob storage provider is not defined in environment variable "
          + BLOB_STORAGE_PROVIDER_ENV + ". Falling back to local file system.");
      return LOCAL_FILE_SYSTEM;
    }

    try {
      log.info("BlobstoreProviderType: Blob storage provider type configured is " + provider);
      return BlobstoreProviderType.valueOf(provider);
    } catch (IllegalArgumentException ex) {
      log.error("BlobstoreProviderType: unknown blob storage provider " + provider);
      throw ex;
    }
  }
}