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

import com.google.api.gax.paging.Page;
import com.google.cloud.Policy;
import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.HmacKey;
import com.google.cloud.storage.PostPolicyV4;
import com.google.cloud.storage.ServiceAccount;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageOptions;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Provides a thread-safe wrapper around any {@link Storage} implementations which may not offer thread safety.
 *
 * The {@link Storage} implementation provided by google-cloud-nio for in-memory testing is thread-unsafe.  This causes
 * a problem for backup-restore tests, which have threads reading and writing cloud-store data from the test threads,
 * overseer nodes, as well as any nodes hosting individual cores.  This class takes a heavyhanded approach to solving
 * this by adding synchronization across all instance methods.  This allows google-cloud-nio's in-memory fake to be used
 * from multiple threads simultaneously as our tests require.
 */
public class ConcurrentDelegatingStorage implements Storage {

  private final Storage delegate;

  public ConcurrentDelegatingStorage(Storage delegate) {
    this.delegate = delegate;
  }

  @Override
  public synchronized Bucket create(BucketInfo bucketInfo, BucketTargetOption... options) { return delegate.create(bucketInfo, options); }

  @Override
  public synchronized Blob create(BlobInfo blobInfo, BlobTargetOption... options) { return delegate.create(blobInfo, options); }

  @Override
  public synchronized Blob create(BlobInfo blobInfo, byte[] content, BlobTargetOption... options) { return delegate.create(blobInfo, content, options); }

  @Override
  public synchronized Blob create(BlobInfo blobInfo, byte[] content, int offset, int length, BlobTargetOption... options) { return delegate.create(blobInfo, content, offset, length, options); }

  @Override
  public synchronized Blob create(BlobInfo blobInfo, InputStream content, BlobWriteOption... options) { return delegate.create(blobInfo, content, options); }

  @Override
  public Blob createFrom(BlobInfo blobInfo, Path path, BlobWriteOption... blobWriteOptions) throws IOException { return delegate.createFrom(blobInfo, path, blobWriteOptions); }

  @Override
  public Blob createFrom(BlobInfo blobInfo, Path path, int i, BlobWriteOption... blobWriteOptions) throws IOException { return delegate.createFrom(blobInfo, path, i, blobWriteOptions); }

  @Override
  public Blob createFrom(BlobInfo blobInfo, InputStream inputStream, BlobWriteOption... blobWriteOptions) throws IOException { return delegate.createFrom(blobInfo, inputStream, blobWriteOptions); }

  @Override
  public Blob createFrom(BlobInfo blobInfo, InputStream inputStream, int i, BlobWriteOption... blobWriteOptions) throws IOException { return delegate.createFrom(blobInfo, inputStream, i, blobWriteOptions); }

  @Override
  public synchronized Bucket get(String bucket, BucketGetOption... options) { return delegate.get(bucket, options); }

  @Override
  public synchronized Bucket lockRetentionPolicy(BucketInfo bucket, BucketTargetOption... options) { return delegate.lockRetentionPolicy(bucket, options); }

  @Override
  public synchronized Blob get(String bucket, String blob, BlobGetOption... options) { return delegate.get(bucket, blob, options); }

  @Override
  public synchronized Blob get(BlobId blob, BlobGetOption... options) { return delegate.get(blob, options); }

  @Override
  public synchronized Blob get(BlobId blob) { return delegate.get(blob); }

  @Override
  public synchronized Page<Bucket> list(BucketListOption... options) { return delegate.list(options); }

  @Override
  public synchronized Page<Blob> list(String bucket, BlobListOption... options) { return delegate.list(bucket, options); }

  @Override
  public synchronized Bucket update(BucketInfo bucketInfo, BucketTargetOption... options) { return delegate.update(bucketInfo, options); }

  @Override
  public synchronized Blob update(BlobInfo blobInfo, BlobTargetOption... options) { return delegate.update(blobInfo, options); }

  @Override
  public synchronized Blob update(BlobInfo blobInfo) { return delegate.update(blobInfo); }

  @Override
  public synchronized boolean delete(String bucket, BucketSourceOption... options) { return delegate.delete(bucket, options); }

  @Override
  public synchronized boolean delete(String bucket, String blob, BlobSourceOption... options) { return delegate.delete(bucket, blob, options); }

  @Override
  public synchronized boolean delete(BlobId blob, BlobSourceOption... options) { return delegate.delete(blob, options); }

  @Override
  public synchronized boolean delete(BlobId blob) { return delegate.delete(blob); }

  @Override
  public synchronized Blob compose(ComposeRequest composeRequest) { return delegate.compose(composeRequest); }

  @Override
  public synchronized CopyWriter copy(CopyRequest copyRequest) { return delegate.copy(copyRequest); }

  @Override
  public synchronized byte[] readAllBytes(String bucket, String blob, BlobSourceOption... options) { return delegate.readAllBytes(bucket, blob, options); }

  @Override
  public synchronized byte[] readAllBytes(BlobId blob, BlobSourceOption... options) { return delegate.readAllBytes(blob, options); }

  @Override
  public synchronized StorageBatch batch() { return delegate.batch(); }

  @Override
  public synchronized ReadChannel reader(String bucket, String blob, BlobSourceOption... options) { return new ConcurrentReadChannel(this, delegate.reader(bucket, blob, options)); }

  @Override
  public synchronized ReadChannel reader(BlobId blob, BlobSourceOption... options) { return new ConcurrentReadChannel(this, delegate.reader(blob, options)); }

  @Override
  public synchronized WriteChannel writer(BlobInfo blobInfo, BlobWriteOption... options) { return new ConcurrentWriteChannel(this, delegate.writer(blobInfo, options)); }

  @Override
  public synchronized WriteChannel writer(URL signedURL) { return new ConcurrentWriteChannel(this, delegate.writer(signedURL)); }

  @Override
  public synchronized URL signUrl(BlobInfo blobInfo, long duration, TimeUnit unit, SignUrlOption... options) { return delegate.signUrl(blobInfo, duration, unit, options); }

  @Override
  public PostPolicyV4 generateSignedPostPolicyV4(BlobInfo blobInfo, long l, TimeUnit timeUnit, PostPolicyV4.PostFieldsV4 postFieldsV4, PostPolicyV4.PostConditionsV4 postConditionsV4, PostPolicyV4Option... postPolicyV4Options) { return delegate.generateSignedPostPolicyV4(blobInfo, l, timeUnit, postFieldsV4, postConditionsV4, postPolicyV4Options); }

  @Override
  public PostPolicyV4 generateSignedPostPolicyV4(BlobInfo blobInfo, long l, TimeUnit timeUnit, PostPolicyV4.PostFieldsV4 postFieldsV4, PostPolicyV4Option... postPolicyV4Options) { return delegate.generateSignedPostPolicyV4(blobInfo, l, timeUnit, postFieldsV4, postPolicyV4Options); }

  @Override
  public PostPolicyV4 generateSignedPostPolicyV4(BlobInfo blobInfo, long l, TimeUnit timeUnit, PostPolicyV4.PostConditionsV4 postConditionsV4, PostPolicyV4Option... postPolicyV4Options) { return delegate.generateSignedPostPolicyV4(blobInfo, l, timeUnit, postConditionsV4, postPolicyV4Options); }

  @Override
  public PostPolicyV4 generateSignedPostPolicyV4(BlobInfo blobInfo, long l, TimeUnit timeUnit, PostPolicyV4Option... postPolicyV4Options) { return delegate.generateSignedPostPolicyV4(blobInfo, l, timeUnit, postPolicyV4Options); }

  @Override
  public synchronized List<Blob> get(BlobId... blobIds) { return delegate.get(blobIds); }

  @Override
  public synchronized List<Blob> get(Iterable<BlobId> blobIds) { return delegate.get(blobIds); }

  @Override
  public synchronized List<Blob> update(BlobInfo... blobInfos) { return delegate.update(blobInfos); }

  @Override
  public synchronized List<Blob> update(Iterable<BlobInfo> blobInfos) { return delegate.update(blobInfos); }

  @Override
  public synchronized List<Boolean> delete(BlobId... blobIds) { return delegate.delete(blobIds); }

  @Override
  public synchronized List<Boolean> delete(Iterable<BlobId> blobIds) { return delegate.delete(blobIds); }

  @Override
  public synchronized Acl getAcl(String bucket, Acl.Entity entity, BucketSourceOption... options) { return delegate.getAcl(bucket, entity, options); }

  @Override
  public synchronized Acl getAcl(String bucket, Acl.Entity entity) { return delegate.getAcl(bucket, entity); }

  @Override
  public synchronized boolean deleteAcl(String bucket, Acl.Entity entity, BucketSourceOption... options) { return deleteAcl(bucket, entity, options); }

  @Override
  public synchronized boolean deleteAcl(String bucket, Acl.Entity entity) { return delegate.deleteAcl(bucket, entity); }

  @Override
  public synchronized Acl createAcl(String bucket, Acl acl, BucketSourceOption... options) { return delegate.createAcl(bucket, acl, options); }

  @Override
  public synchronized Acl createAcl(String bucket, Acl acl) { return delegate.createAcl(bucket, acl); }

  @Override
  public synchronized Acl updateAcl(String bucket, Acl acl, BucketSourceOption... options) { return delegate.updateAcl(bucket, acl, options); }

  @Override
  public synchronized Acl updateAcl(String bucket, Acl acl) { return delegate.updateAcl(bucket, acl); }

  @Override
  public synchronized List<Acl> listAcls(String bucket, BucketSourceOption... options) { return delegate.listAcls(bucket, options); }

  @Override
  public synchronized List<Acl> listAcls(String bucket) { return delegate.listAcls(bucket); }

  @Override
  public synchronized Acl getDefaultAcl(String bucket, Acl.Entity entity) { return delegate.getDefaultAcl(bucket, entity); }

  @Override
  public synchronized boolean deleteDefaultAcl(String bucket, Acl.Entity entity) { return delegate.deleteDefaultAcl(bucket, entity); }

  @Override
  public synchronized Acl createDefaultAcl(String bucket, Acl acl) { return delegate.createDefaultAcl(bucket, acl); }

  @Override
  public synchronized Acl updateDefaultAcl(String bucket, Acl acl) { return delegate.updateDefaultAcl(bucket, acl); }

  @Override
  public synchronized List<Acl> listDefaultAcls(String bucket) { return delegate.listDefaultAcls(bucket); }

  @Override
  public synchronized Acl getAcl(BlobId blob, Acl.Entity entity) { return delegate.getAcl(blob, entity); }

  @Override
  public synchronized boolean deleteAcl(BlobId blob, Acl.Entity entity) { return delegate.deleteAcl(blob, entity); }

  @Override
  public synchronized Acl createAcl(BlobId blob, Acl acl) { return delegate.createAcl(blob, acl); }

  @Override
  public synchronized Acl updateAcl(BlobId blob, Acl acl) { return delegate.updateAcl(blob, acl); }

  @Override
  public synchronized List<Acl> listAcls(BlobId blob) { return delegate.listAcls(blob); }

  @Override
  public HmacKey createHmacKey(ServiceAccount serviceAccount, CreateHmacKeyOption... createHmacKeyOptions) { return delegate.createHmacKey(serviceAccount, createHmacKeyOptions); }

  @Override
  public Page<HmacKey.HmacKeyMetadata> listHmacKeys(ListHmacKeysOption... listHmacKeysOptions) { return delegate.listHmacKeys(listHmacKeysOptions); }

  @Override
  public HmacKey.HmacKeyMetadata getHmacKey(String s, GetHmacKeyOption... getHmacKeyOptions) { return delegate.getHmacKey(s, getHmacKeyOptions); }

  @Override
  public void deleteHmacKey(HmacKey.HmacKeyMetadata hmacKeyMetadata, DeleteHmacKeyOption... deleteHmacKeyOptions) { delegate.deleteHmacKey(hmacKeyMetadata, deleteHmacKeyOptions); }

  @Override
  public HmacKey.HmacKeyMetadata updateHmacKeyState(HmacKey.HmacKeyMetadata hmacKeyMetadata, HmacKey.HmacKeyState hmacKeyState, UpdateHmacKeyOption... updateHmacKeyOptions) { return delegate.updateHmacKeyState(hmacKeyMetadata, hmacKeyState, updateHmacKeyOptions); }

  @Override
  public synchronized Policy getIamPolicy(String bucket, BucketSourceOption... options) {
    return delegate.getIamPolicy(bucket, options);
  }

  @Override
  public synchronized Policy setIamPolicy(String bucket, Policy policy, BucketSourceOption... options) {
    return delegate.setIamPolicy(bucket, policy, options);
  }

  @Override
  public synchronized List<Boolean> testIamPermissions(String bucket, List<String> permissions, BucketSourceOption... options) {
    return delegate.testIamPermissions(bucket, permissions, options);
  }

  @Override
  public synchronized ServiceAccount getServiceAccount(String projectId) {
    return delegate.getServiceAccount(projectId);
  }

  @Override
  public synchronized StorageOptions getOptions() {
    return delegate.getOptions();
  }

  public static class ConcurrentReadChannel implements ReadChannel {

    private final ConcurrentDelegatingStorage storage;
    private final ReadChannel delegate;

    public ConcurrentReadChannel(ConcurrentDelegatingStorage storage, ReadChannel delegate) {
      this.storage = storage;
      this.delegate = delegate;
    }

    @Override
    public boolean isOpen() {
      synchronized (storage) {
        return delegate.isOpen();
      }
    }

    @Override
    public void close() {
      synchronized (storage) {
        delegate.close();
      }
    }

    @Override
    public void seek(long position) throws IOException {
      synchronized (storage) {
        delegate.seek(position);
      }
    }

    @Override
    public void setChunkSize(int chunkSize) {
      synchronized (storage) {
        delegate.setChunkSize(chunkSize);
      }
    }

    @Override
    public RestorableState<ReadChannel> capture() {
      synchronized (storage) {
        return delegate.capture();
      }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      synchronized (storage) {
        return delegate.read(dst);
      }
    }
  }

  public static class ConcurrentWriteChannel implements WriteChannel {

    private final ConcurrentDelegatingStorage storage;
    private final WriteChannel delegate;

    public ConcurrentWriteChannel(ConcurrentDelegatingStorage storage, WriteChannel delegate) {
      this.storage = storage;
      this.delegate = delegate;
    }

    @Override
    public void setChunkSize(int chunkSize) {
      synchronized (storage) {
        delegate.setChunkSize(chunkSize);
      }
    }

    @Override
    public RestorableState<WriteChannel> capture() {
      synchronized (storage) {
        return delegate.capture();
      }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      synchronized (storage) {
        return delegate.write(src);
      }
    }

    @Override
    public boolean isOpen() {
      synchronized (storage) {
        return delegate.isOpen();
      }
    }

    @Override
    public void close() throws IOException {
      synchronized (storage) {
        delegate.close();
      }
    }
  }
}
