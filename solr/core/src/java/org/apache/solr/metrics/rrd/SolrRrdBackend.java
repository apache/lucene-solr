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
package org.apache.solr.metrics.rrd;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.rrd4j.core.RrdByteArrayBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated this functionality will be removed in Solr 9.0
 */
@Deprecated
public class SolrRrdBackend extends RrdByteArrayBackend implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrRrdBackendFactory factory;
  private final boolean readOnly;
  private final ReentrantLock lock = new ReentrantLock();
  private volatile boolean dirty = false;
  private volatile boolean closed = false;
  private volatile long lastModifiedTime;

  public static final class SyncData {
    public byte[] data;
    public long timestamp;

    public SyncData(byte[] data, long timestamp) {
      this.data = data;
      this.timestamp = timestamp;
    }
  }

  public SolrRrdBackend(String path, boolean readOnly, SolrRrdBackendFactory factory) {
    super(path);
    this.factory = factory;
    this.lastModifiedTime = TimeUnit.MILLISECONDS.convert(factory.getTimeSource().getEpochTimeNs(), TimeUnit.NANOSECONDS);
    try {
      SyncData syncData = factory.getData(path);
      if (syncData != null) {
        setBuffer(syncData.data);
        this.lastModifiedTime = syncData.timestamp;
      }
    } catch (IOException e) {
      log.warn("Exception retrieving data from {}, store will be readOnly", path, e);
      readOnly = true;
    }
    this.readOnly = readOnly;
  }

  /**
   * Open an unregistered (throwaway) read-only clone of another backend.
   * @param other other backend
   */
  public SolrRrdBackend(SolrRrdBackend other) {
    super(other.getPath());
    readOnly = true;
    factory = null;
    this.lastModifiedTime = other.lastModifiedTime;
    byte[] otherBuffer = other.getBuffer();
    byte[] newBuffer = new byte[otherBuffer.length];
    System.arraycopy(otherBuffer, 0, newBuffer, 0, otherBuffer.length);
    super.setBuffer(newBuffer);
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

  private void markDirty() {
    lastModifiedTime = TimeUnit.MILLISECONDS.convert(factory.getTimeSource().getEpochTimeNs(), TimeUnit.NANOSECONDS);
    dirty = true;
  }

  @Override
  protected void write(long offset, byte[] bytes) throws IOException {
    if (readOnly || closed) {
      return;
    }
    lock.lock();
    try {
      super.write(offset, bytes);
      markDirty();
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void writeShort(long offset, short value) throws IOException {
    if (readOnly || closed) {
      return;
    }
    lock.lock();
    try {
      super.writeShort(offset, value);
      markDirty();
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void writeInt(long offset, int value) throws IOException {
    if (readOnly || closed) {
      return;
    }
    lock.lock();
    try {
      super.writeInt(offset, value);
      markDirty();
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void writeLong(long offset, long value) throws IOException {
    if (readOnly || closed) {
      return;
    }
    lock.lock();
    try {
      super.writeLong(offset, value);
      markDirty();
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void writeDouble(long offset, double value) throws IOException {
    if (readOnly || closed) {
      return;
    }
    lock.lock();
    try {
      super.writeDouble(offset, value);
      markDirty();
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void writeDouble(long offset, double value, int count) throws IOException {
    if (readOnly || closed) {
      return;
    }
    lock.lock();
    try {
      super.writeDouble(offset, value, count);
      markDirty();
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void writeDouble(long offset, double[] values) throws IOException {
    if (readOnly || closed) {
      return;
    }
    lock.lock();
    try {
      super.writeDouble(offset, values);
      markDirty();
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void writeString(long offset, String value, int length) throws IOException {
    if (readOnly || closed) {
      return;
    }
    lock.lock();
    try {
      super.writeString(offset, value, length);
      markDirty();
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected boolean isDirty() {
    return dirty;
  }

  @Override
  protected void setBuffer(byte[] buffer) {
    if (readOnly || closed) {
      return;
    }
    lock.lock();
    try {
      super.setBuffer(buffer);
      markDirty();
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void setLength(long length) throws IOException {
    if (readOnly || closed) {
      return;
    }
    lock.lock();
    try {
      super.setLength(length);
      markDirty();
    } finally {
      lock.unlock();
    }
  }

  public SyncData getSyncDataAndMarkClean() {
    if (readOnly || closed) {
      return null;
    }
    if (!dirty) {
      return null;
    }
    // hold a lock to block writes so that we get consistent data
    lock.lock();
    try {
      byte[] oldBuffer = getBuffer();
      byte[] bufferCopy = new byte[oldBuffer.length];
      System.arraycopy(oldBuffer, 0, bufferCopy, 0, oldBuffer.length);
      return new SyncData(bufferCopy, lastModifiedTime);
    } finally {
      // reset the dirty flag
      dirty = false;
      lock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    closed = true;
    if (factory != null) {
      // unregister myself from the factory
      factory.unregisterBackend(getPath());
    }
    // close
  }
}
