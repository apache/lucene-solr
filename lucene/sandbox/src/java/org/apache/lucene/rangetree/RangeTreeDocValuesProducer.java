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
package org.apache.lucene.rangetree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

class RangeTreeDocValuesProducer extends DocValuesProducer {

  private final Map<String,RangeTreeReader> treeReaders = new HashMap<>();
  private final Map<Integer,Long> fieldToIndexFPs = new HashMap<>();

  private final IndexInput datIn;
  private final AtomicLong ramBytesUsed;
  private final int maxDoc;
  private final DocValuesProducer delegate;
  private final boolean merging;

  public RangeTreeDocValuesProducer(DocValuesProducer delegate, SegmentReadState state) throws IOException {
    String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, RangeTreeDocValuesFormat.META_EXTENSION);
    ChecksumIndexInput metaIn = state.directory.openChecksumInput(metaFileName, state.context);
    CodecUtil.checkIndexHeader(metaIn, RangeTreeDocValuesFormat.META_CODEC_NAME, RangeTreeDocValuesFormat.META_VERSION_START, RangeTreeDocValuesFormat.META_VERSION_CURRENT,
                               state.segmentInfo.getId(), state.segmentSuffix);
    int fieldCount = metaIn.readVInt();
    for(int i=0;i<fieldCount;i++) {
      int fieldNumber = metaIn.readVInt();
      long indexFP = metaIn.readVLong();
      fieldToIndexFPs.put(fieldNumber, indexFP);
    }
    CodecUtil.checkFooter(metaIn);
    metaIn.close();

    String datFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, RangeTreeDocValuesFormat.DATA_EXTENSION);
    datIn = state.directory.openInput(datFileName, state.context);
    CodecUtil.checkIndexHeader(datIn, RangeTreeDocValuesFormat.DATA_CODEC_NAME, RangeTreeDocValuesFormat.DATA_VERSION_START, RangeTreeDocValuesFormat.DATA_VERSION_CURRENT,
                               state.segmentInfo.getId(), state.segmentSuffix);
    ramBytesUsed = new AtomicLong(RamUsageEstimator.shallowSizeOfInstance(getClass()));
    maxDoc = state.segmentInfo.maxDoc();
    this.delegate = delegate;
    merging = false;
  }

  // clone for merge: we don't hang onto the RangeTrees we load
  RangeTreeDocValuesProducer(RangeTreeDocValuesProducer orig) throws IOException {
    assert Thread.holdsLock(orig);
    datIn = orig.datIn.clone();
    ramBytesUsed = new AtomicLong(orig.ramBytesUsed.get());
    delegate = orig.delegate.getMergeInstance();
    fieldToIndexFPs.putAll(orig.fieldToIndexFPs);
    treeReaders.putAll(orig.treeReaders);
    merging = true;
    maxDoc = orig.maxDoc;
  }

  @Override
  public synchronized SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    RangeTreeReader treeReader = treeReaders.get(field.name);
    if (treeReader == null) {
      // Lazy load
      Long fp = fieldToIndexFPs.get(field.number);
      // FieldInfos checks has already ensured we are a DV field of this type, and Codec ensures
      // this DVFormat was used at write time:
      assert fp != null;

      // LUCENE-6697: never do real IOPs with the original IndexInput because search
      // threads can be concurrently cloning it:
      IndexInput clone = datIn.clone();
      clone.seek(fp);
      treeReader = new RangeTreeReader(clone);

      // Only hang onto the reader when we are not merging:
      if (merging == false) {
        treeReaders.put(field.name, treeReader);
        ramBytesUsed.addAndGet(treeReader.ramBytesUsed());
      }
    }

    return new RangeTreeSortedNumericDocValues(treeReader, delegate.getSortedNumeric(field));
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(datIn, delegate);
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(datIn);
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo field) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    RangeTreeReader treeReader = treeReaders.get(field.name);
    if (treeReader == null) {
      // Lazy load
      Long fp = fieldToIndexFPs.get(field.number);

      // FieldInfos checks has already ensured we are a DV field of this type, and Codec ensures
      // this DVFormat was used at write time:
      assert fp != null;

      // LUCENE-6697: never do real IOPs with the original IndexInput because search
      // threads can be concurrently cloning it:
      IndexInput clone = datIn.clone();
      clone.seek(fp);
      treeReader = new RangeTreeReader(clone);

      // Only hang onto the reader when we are not merging:
      if (merging == false) {
        treeReaders.put(field.name, treeReader);
        ramBytesUsed.addAndGet(treeReader.ramBytesUsed());
      }
    }

    return new RangeTreeSortedSetDocValues(treeReader, delegate.getSortedSet(field));
  }

  @Override
  public Bits getDocsWithField(FieldInfo field) throws IOException {
    return delegate.getDocsWithField(field);
  }

  @Override
  public synchronized Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<>();
    for(Map.Entry<String,RangeTreeReader> ent : treeReaders.entrySet()) {
      resources.add(Accountables.namedAccountable("field " + ent.getKey(), ent.getValue()));
    }
    resources.add(Accountables.namedAccountable("delegate", delegate));

    return resources;
  }

  @Override
  public synchronized DocValuesProducer getMergeInstance() throws IOException {
    return new RangeTreeDocValuesProducer(this);
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed.get() + delegate.ramBytesUsed();
  }
}
