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

import java.io.IOException;
import java.util.Locale;

/**
 * Builder for {@link CoreStorageClient}
 */
public class BlobStorageClientBuilder {
  private BlobstoreProviderType blobStorageProvider;
  private static final String UNKNOWN_PROVIDER_TYPE = "Blob storage provider [%s] is unknown. Please check configuration.";

  public BlobStorageClientBuilder(BlobstoreProviderType blobStorageProvider) {
    this.blobStorageProvider = blobStorageProvider;
  }

  public CoreStorageClient build() throws IllegalArgumentException, IOException {
    CoreStorageClient client;

    switch (blobStorageProvider) {
      case LOCAL_FILE_SYSTEM:
        client = new LocalStorageClient();
        break;
      case S3:
        client = new S3StorageClient();
        break;
      default:
        throw new IllegalArgumentException(String.format(Locale.ROOT, UNKNOWN_PROVIDER_TYPE, blobStorageProvider.name()));
    }
    return client;
  };
}
