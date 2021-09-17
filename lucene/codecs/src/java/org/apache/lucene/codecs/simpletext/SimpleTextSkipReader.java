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
package org.apache.lucene.codecs.simpletext;

import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.CHILD_POINTER;
import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.FREQ;
import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.IMPACT;
import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.IMPACTS;
import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.IMPACTS_END;
import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.LEVEL_LENGTH;
import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.NORM;
import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.SKIP_DOC;
import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.SKIP_DOC_FP;
import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.SKIP_LIST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.codecs.MultiLevelSkipListReader;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.StringHelper;

/**
 * This class reads skip lists with multiple levels.
 *
 * <p>See {@link SimpleTextFieldsWriter} for the information about the encoding of the multi level
 * skip lists.
 *
 * @lucene.experimental
 */
class SimpleTextSkipReader extends MultiLevelSkipListReader {

  private final CharsRefBuilder scratchUTF16 = new CharsRefBuilder();
  private final BytesRefBuilder scratch = new BytesRefBuilder();
  private Impacts impacts;
  private List<List<Impact>> perLevelImpacts;
  private long nextSkipDocFP = -1;
  private int numLevels = 1;
  private boolean hasSkipList = false;

  SimpleTextSkipReader(IndexInput skipStream) {
    super(
        skipStream,
        SimpleTextSkipWriter.maxSkipLevels,
        SimpleTextSkipWriter.BLOCK_SIZE,
        SimpleTextSkipWriter.skipMultiplier);
    impacts =
        new Impacts() {
          @Override
          public int numLevels() {
            return numLevels;
          }

          @Override
          public int getDocIdUpTo(int level) {
            return skipDoc[level];
          }

          @Override
          public List<Impact> getImpacts(int level) {
            assert level < numLevels;
            return perLevelImpacts.get(level);
          }
        };
    init();
  }

  @Override
  public int skipTo(int target) throws IOException {
    if (!hasSkipList) {
      return -1;
    }
    int result = super.skipTo(target);
    if (numberOfSkipLevels > 0) {
      numLevels = numberOfSkipLevels;
    } else {
      // End of postings don't have skip data anymore, so we fill with dummy data
      // like SlowImpactsEnum.
      numLevels = 1;
      perLevelImpacts.add(0, Collections.singletonList(new Impact(Integer.MAX_VALUE, 1L)));
    }
    return result;
  }

  @Override
  protected int readSkipData(int level, IndexInput skipStream) throws IOException {
    perLevelImpacts.get(level).clear();
    int skipDoc = DocIdSetIterator.NO_MORE_DOCS;
    ChecksumIndexInput input = new BufferedChecksumIndexInput(skipStream);
    int freq = 1;
    while (true) {
      SimpleTextUtil.readLine(input, scratch);
      if (scratch.get().equals(SimpleTextFieldsWriter.END)) {
        SimpleTextUtil.checkFooter(input);
        break;
      } else if (scratch.get().equals(IMPACTS_END)
          || scratch.get().equals(SimpleTextFieldsWriter.TERM)
          || scratch.get().equals(SimpleTextFieldsWriter.FIELD)) {
        break;
      } else if (StringHelper.startsWith(scratch.get(), SKIP_LIST)) {
        // continue
      } else if (StringHelper.startsWith(scratch.get(), SKIP_DOC)) {
        scratchUTF16.copyUTF8Bytes(
            scratch.bytes(), SKIP_DOC.length, scratch.length() - SKIP_DOC.length);
        skipDoc = ArrayUtil.parseInt(scratchUTF16.chars(), 0, scratchUTF16.length());
        // Because the MultiLevelSkipListReader stores doc id delta,but simple text codec stores doc
        // id
        skipDoc = skipDoc - super.skipDoc[level];
      } else if (StringHelper.startsWith(scratch.get(), SKIP_DOC_FP)) {
        scratchUTF16.copyUTF8Bytes(
            scratch.bytes(), SKIP_DOC_FP.length, scratch.length() - SKIP_DOC_FP.length);
        nextSkipDocFP = ArrayUtil.parseInt(scratchUTF16.chars(), 0, scratchUTF16.length());
      } else if (StringHelper.startsWith(scratch.get(), IMPACTS)
          || StringHelper.startsWith(scratch.get(), IMPACT)) {
        // continue;
      } else if (StringHelper.startsWith(scratch.get(), FREQ)) {
        scratchUTF16.copyUTF8Bytes(scratch.bytes(), FREQ.length, scratch.length() - FREQ.length);
        freq = ArrayUtil.parseInt(scratchUTF16.chars(), 0, scratchUTF16.length());
      } else if (StringHelper.startsWith(scratch.get(), NORM)) {
        scratchUTF16.copyUTF8Bytes(scratch.bytes(), NORM.length, scratch.length() - NORM.length);
        long norm = Long.parseLong(scratchUTF16.toString());
        Impact impact = new Impact(freq, norm);
        perLevelImpacts.get(level).add(impact);
      }
    }
    return skipDoc;
  }

  @Override
  protected long readLevelLength(IndexInput skipStream) throws IOException {
    SimpleTextUtil.readLine(skipStream, scratch);
    scratchUTF16.copyUTF8Bytes(
        scratch.bytes(), LEVEL_LENGTH.length, scratch.length() - LEVEL_LENGTH.length);
    return Long.parseLong(scratchUTF16.toString());
  }

  @Override
  protected long readChildPointer(IndexInput skipStream) throws IOException {
    SimpleTextUtil.readLine(skipStream, scratch);
    scratchUTF16.copyUTF8Bytes(
        scratch.bytes(), CHILD_POINTER.length, scratch.length() - CHILD_POINTER.length);
    return Long.parseLong(scratchUTF16.toString());
  }

  void reset(long skipPointer, int docFreq) throws IOException {
    init();
    if (skipPointer > 0) {
      super.init(skipPointer, docFreq);
      hasSkipList = true;
    }
  }

  private void init() {
    nextSkipDocFP = -1;
    numLevels = 1;
    perLevelImpacts = new ArrayList<>(maxNumberOfSkipLevels);
    for (int level = 0; level < maxNumberOfSkipLevels; level++) {
      List<Impact> impacts = new ArrayList<>();
      impacts.add(new Impact(Integer.MAX_VALUE, 1L));
      perLevelImpacts.add(level, impacts);
    }
    hasSkipList = false;
  }

  Impacts getImpacts() {
    return impacts;
  }

  long getNextSkipDocFP() {
    return nextSkipDocFP;
  }

  int getNextSkipDoc() {
    if (!hasSkipList) {
      return DocIdSetIterator.NO_MORE_DOCS;
    }
    return skipDoc[0];
  }

  boolean hasSkipList() {
    return hasSkipList;
  }
}
