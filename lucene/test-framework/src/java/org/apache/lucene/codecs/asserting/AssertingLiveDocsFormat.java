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
package org.apache.lucene.codecs.asserting;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.MutableBits;
import org.apache.lucene.util.TestUtil;

/**
 * Just like the default live docs format but with additional asserts.
 */
public class AssertingLiveDocsFormat extends LiveDocsFormat {
  private final LiveDocsFormat in = TestUtil.getDefaultCodec().liveDocsFormat();

  @Override
  public MutableBits newLiveDocs(int size) throws IOException {
    assert size >= 0;
    MutableBits raw = in.newLiveDocs(size);
    assert raw != null;
    assert raw.length() == size;
    for (int i = 0; i < raw.length(); i++) {
      assert raw.get(i);
    }
    return new AssertingMutableBits(raw);
  }

  @Override
  public MutableBits newLiveDocs(Bits existing) throws IOException {
    assert existing instanceof AssertingBits;
    Bits rawExisting = ((AssertingBits)existing).in;
    MutableBits raw = in.newLiveDocs(rawExisting);
    assert raw != null;
    assert raw.length() == rawExisting.length();
    for (int i = 0; i < raw.length(); i++) {
      assert rawExisting.get(i) == raw.get(i);
    }
    return new AssertingMutableBits(raw);
  }

  @Override
  public Bits readLiveDocs(Directory dir, SegmentCommitInfo info, IOContext context) throws IOException {
    Bits raw = in.readLiveDocs(dir, info, context);
    assert raw != null;
    check(raw, info.info.maxDoc(), info.getDelCount());
    return new AssertingBits(raw);
  }

  @Override
  public void writeLiveDocs(MutableBits bits, Directory dir, SegmentCommitInfo info, int newDelCount, IOContext context) throws IOException {
    MutableBits raw = bits;
    /**
     * bits is not necessarily an AssertingMutableBits because index sorting needs to wrap it in a sorted view.
     */
    if (bits instanceof AssertingMutableBits) {
      raw = (MutableBits) ((AssertingMutableBits) bits).in;
    }
    check(raw, info.info.maxDoc(), info.getDelCount() + newDelCount);
    in.writeLiveDocs(raw, dir, info, newDelCount, context);
  }
  
  private void check(Bits bits, int expectedLength, int expectedDeleteCount) {
    assert bits.length() == expectedLength;
    int deletedCount = 0;
    for (int i = 0; i < bits.length(); i++) {
      if (!bits.get(i)) {
        deletedCount++;
      }
    }
    assert deletedCount == expectedDeleteCount;
  }

  @Override
  public void files(SegmentCommitInfo info, Collection<String> files) throws IOException {
    in.files(info, files);
  }
  
  @Override
  public String toString() {
    return "Asserting(" + in + ")";
  }

  static class AssertingBits implements Bits {
    final Bits in;
    
    AssertingBits(Bits in) {
      this.in = in;
      assert in.length() >= 0;
    }
    
    @Override
    public boolean get(int index) {
      assert index >= 0;
      assert index < in.length(): "index=" + index + " vs in.length()=" + in.length();
      return in.get(index);
    }
    
    @Override
    public int length() {
      return in.length();
    }
    
    @Override
    public String toString() {
      return "Asserting(" + in + ")";
    }
  }
  
  static class AssertingMutableBits extends AssertingBits implements MutableBits {   
    AssertingMutableBits(MutableBits in) {
      super(in);
    }

    @Override
    public void clear(int index) {
      assert index >= 0;
      assert index < in.length();
      ((MutableBits)in).clear(index);
    }
  }
}
