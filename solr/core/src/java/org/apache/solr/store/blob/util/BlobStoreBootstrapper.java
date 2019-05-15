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
package org.apache.solr.store.blob.util;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.store.blob.process.BlobProcessUtil;
import org.apache.solr.store.blob.provider.BlobStorageProvider;
 /**
 * THROWAWAY CLASS
 * 
 * Helper class for managing blob-related bootstrapping
 * 
 * THROWAWAY CLASS 
 */
public class BlobStoreBootstrapper {
  
  private static String localBlobDir = System.getProperty("blob.local.dir", "/tmp/BlobStoreLocal/");//config.getSfdcConfigProperty(SfdcConfigProperty.LocalBlobStoreHome);
  private static String blobBucketName =  System.getProperty("blob.service.bucket", "");//config.getSfdcConfigProperty(SfdcConfigProperty.BlobServiceBucket);
  private static String blobstoreEndpoint = System.getProperty("blob.service.endpoint", "");//config.getSfdcConfigProperty(SfdcConfigProperty.BlobServiceEndpoint);
  private static String blobStorageProvider = System.getProperty("blob.service.provider", "LOCAL_FILE_SYSTEM");//config.getSfdcConfigProperty(SfdcConfigProperty.BlobStorageProvider);
  private static String blobstoreAccessKey = System.getProperty("blob.key.access", "");//config.getSfdcConfigProperty(SfdcConfigProperty.BlobServiceAccessToken);
  private static String blobstoreSecretKey = System.getProperty("blob.key.secret", "");//config.getSfdcConfigProperty(SfdcConfigProperty.BlobServiceSecretToken);
  
  
  public static void init(CoreContainer cores) throws Exception {
    BlobStorageProvider.init();
    BlobProcessUtil.init(cores);
  }
   public static String getLocalBlobDir() {
    return localBlobDir;
  }
   public static String getBlobBucketName() {
    return blobBucketName;
  }
   public static String getBlobstoreEndpoint() {
    return blobstoreEndpoint;
  }
   public static String getBlobStorageProvider() {
    return blobStorageProvider;
  }
   public static String getBlobstoreAccessKey() {
    return blobstoreAccessKey;
  }
   public static String getBlobstoreSecretKey() {
    return blobstoreSecretKey;
  }
  
}