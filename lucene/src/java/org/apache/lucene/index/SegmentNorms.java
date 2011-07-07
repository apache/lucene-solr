package org.apache.lucene.index;

/**
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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * Byte[] referencing is used because a new norm object needs 
 * to be created for each clone, and the byte array is all 
 * that is needed for sharing between cloned readers.  The 
 * current norm referencing is for sharing between readers 
 * whereas the byte[] referencing is for copy on write which 
 * is independent of reader references (i.e. incRef, decRef).
 */

final class SegmentNorms implements Cloneable {

  /** norms header placeholder */
  static final byte[] NORMS_HEADER = new byte[]{'N','R','M',-1};

  int refCount = 1;

  // If this instance is a clone, the originalNorm
  // references the Norm that has a real open IndexInput:
  private SegmentNorms origNorm;

  private IndexInput in;
  private long normSeek;

  // null until bytes is set
  private AtomicInteger bytesRef;
  private byte[] bytes;
  private int number;

  boolean dirty;
  boolean rollbackDirty;
  
  private final SegmentReader owner;
  
  public SegmentNorms(IndexInput in, int number, long normSeek, SegmentReader owner) {
    this.in = in;
    this.number = number;
    this.normSeek = normSeek;
    this.owner = owner;
  }

  public synchronized void incRef() {
    assert refCount > 0 && (origNorm == null || origNorm.refCount > 0);
    refCount++;
  }

  private void closeInput() throws IOException {
    if (in != null) {
      if (in != owner.singleNormStream) {
        // It's private to us -- just close it
        in.close();
      } else {
        // We are sharing this with others -- decRef and
        // maybe close the shared norm stream
        if (owner.singleNormRef.decrementAndGet() == 0) {
          owner.singleNormStream.close();
          owner.singleNormStream = null;
        }
      }

      in = null;
    }
  }

  public synchronized void decRef() throws IOException {
    assert refCount > 0 && (origNorm == null || origNorm.refCount > 0);

    if (--refCount == 0) {
      if (origNorm != null) {
        origNorm.decRef();
        origNorm = null;
      } else {
        closeInput();
      }

      if (bytes != null) {
        assert bytesRef != null;
        bytesRef.decrementAndGet();
        bytes = null;
        bytesRef = null;
      } else {
        assert bytesRef == null;
      }
    }
  }

  // Load & cache full bytes array.  Returns bytes.
  public synchronized byte[] bytes() throws IOException {
    assert refCount > 0 && (origNorm == null || origNorm.refCount > 0);
    if (bytes == null) {                     // value not yet read
      assert bytesRef == null;
      if (origNorm != null) {
        // Ask origNorm to load so that for a series of
        // reopened readers we share a single read-only
        // byte[]
        bytes = origNorm.bytes();
        bytesRef = origNorm.bytesRef;
        bytesRef.incrementAndGet();

        // Once we've loaded the bytes we no longer need
        // origNorm:
        origNorm.decRef();
        origNorm = null;

      } else {
        // We are the origNorm, so load the bytes for real
        // ourself:
        final int count = owner.maxDoc();
        bytes = new byte[count];

        // Since we are orig, in must not be null
        assert in != null;

        // Read from disk.
        synchronized(in) {
          in.seek(normSeek);
          in.readBytes(bytes, 0, count, false);
        }

        bytesRef = new AtomicInteger(1);
        closeInput();
      }
    }

    return bytes;
  }

  // Only for testing
  AtomicInteger bytesRef() {
    return bytesRef;
  }

  // Called if we intend to change a norm value.  We make a
  // private copy of bytes if it's shared with others:
  public synchronized byte[] copyOnWrite() throws IOException {
    assert refCount > 0 && (origNorm == null || origNorm.refCount > 0);
    bytes();
    assert bytes != null;
    assert bytesRef != null;
    if (bytesRef.get() > 1) {
      // I cannot be the origNorm for another norm
      // instance if I'm being changed.  Ie, only the
      // "head Norm" can be changed:
      assert refCount == 1;
      final AtomicInteger oldRef = bytesRef;
      bytes = owner.cloneNormBytes(bytes);
      bytesRef = new AtomicInteger(1);
      oldRef.decrementAndGet();
    }
    dirty = true;
    return bytes;
  }
  
  // Returns a copy of this Norm instance that shares
  // IndexInput & bytes with the original one
  @Override
  public synchronized Object clone() {
    assert refCount > 0 && (origNorm == null || origNorm.refCount > 0);
      
    SegmentNorms clone;
    try {
      clone = (SegmentNorms) super.clone();
    } catch (CloneNotSupportedException cnse) {
      // Cannot happen
      throw new RuntimeException("unexpected CloneNotSupportedException", cnse);
    }
    clone.refCount = 1;

    if (bytes != null) {
      assert bytesRef != null;
      assert origNorm == null;

      // Clone holds a reference to my bytes:
      clone.bytesRef.incrementAndGet();
    } else {
      assert bytesRef == null;
      if (origNorm == null) {
        // I become the origNorm for the clone:
        clone.origNorm = this;
      }
      clone.origNorm.incRef();
    }

    // Only the origNorm will actually readBytes from in:
    clone.in = null;

    return clone;
  }

  // Flush all pending changes to the next generation
  // separate norms file.
  public void reWrite(SegmentInfo si) throws IOException {
    assert refCount > 0 && (origNorm == null || origNorm.refCount > 0): "refCount=" + refCount + " origNorm=" + origNorm;

    // NOTE: norms are re-written in regular directory, not cfs
    si.advanceNormGen(this.number);
    final String normFileName = si.getNormFileName(this.number);
    IndexOutput out = owner.directory().createOutput(normFileName);
    boolean success = false;
    try {
      try {
        out.writeBytes(SegmentNorms.NORMS_HEADER, 0, SegmentNorms.NORMS_HEADER.length);
        out.writeBytes(bytes, owner.maxDoc());
      } finally {
        out.close();
      }
      success = true;
    } finally {
      if (!success) {
        try {
          owner.directory().deleteFile(normFileName);
        } catch (Throwable t) {
          // suppress this so we keep throwing the
          // original exception
        }
      }
    }
    this.dirty = false;
  }
}
