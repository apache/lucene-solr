package org.apache.lucene.search.suggest;

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

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.search.spell.TermFreqIterator;
import org.apache.lucene.search.suggest.Sort.ByteSequencesReader;
import org.apache.lucene.search.suggest.Sort.ByteSequencesWriter;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * This wrapper buffers incoming elements and makes sure they are sorted based on given comparator.
 * @lucene.experimental
 */
public class SortedTermFreqIteratorWrapper implements TermFreqIterator {
  
  private final TermFreqIterator source;
  private File tempInput;
  private File tempSorted;
  private final ByteSequencesReader reader;
  private final Comparator<BytesRef> comparator;
  private boolean done = false;
  
  private long weight;
  private final BytesRef scratch = new BytesRef();
  
  /**
   * Creates a new sorted wrapper, using {@link
   * BytesRef#getUTF8SortedAsUnicodeComparator} for
   * sorting. */
  public SortedTermFreqIteratorWrapper(TermFreqIterator source) throws IOException {
    this(source, BytesRef.getUTF8SortedAsUnicodeComparator());
  }

  /**
   * Creates a new sorted wrapper, sorting by BytesRef
   * (ascending) then cost (ascending).
   */
  public SortedTermFreqIteratorWrapper(TermFreqIterator source, Comparator<BytesRef> comparator) throws IOException {
    this.source = source;
    this.comparator = comparator;
    this.reader = sort();
  }
  
  @Override
  public Comparator<BytesRef> getComparator() {
    return comparator;
  }
  
  @Override
  public BytesRef next() throws IOException {
    boolean success = false;
    if (done) {
      return null;
    }
    try {
      ByteArrayDataInput input = new ByteArrayDataInput();
      if (reader.read(scratch)) {
        weight = decode(scratch, input);
        success = true;
        return scratch;
      }
      close();
      success = done = true;
      return null;
    } finally {
      if (!success) {
        done = true;
        close();
      }
    }
  }
  
  @Override
  public long weight() {
    return weight;
  }

  /** Sortes by BytesRef (ascending) then cost (ascending). */
  private final Comparator<BytesRef> tieBreakByCostComparator = new Comparator<BytesRef>() {

    private final BytesRef leftScratch = new BytesRef();
    private final BytesRef rightScratch = new BytesRef();
    private final ByteArrayDataInput input = new ByteArrayDataInput();
    
    @Override
    public int compare(BytesRef left, BytesRef right) {
      // Make shallow copy in case decode changes the BytesRef:
      leftScratch.bytes = left.bytes;
      leftScratch.offset = left.offset;
      leftScratch.length = left.length;
      rightScratch.bytes = right.bytes;
      rightScratch.offset = right.offset;
      rightScratch.length = right.length;
      long leftCost = decode(leftScratch, input);
      long rightCost = decode(rightScratch, input);
      int cmp = comparator.compare(leftScratch, rightScratch);
      if (cmp != 0) {
        return cmp;
      }
      if (leftCost < rightCost) {
        return -1;
      } else if (rightCost < leftCost) {
        return 1;
      } else {
        return 0;
      }
    }
  };
  
  private Sort.ByteSequencesReader sort() throws IOException {
    String prefix = getClass().getSimpleName();
    File directory = Sort.defaultTempDir();
    tempInput = File.createTempFile(prefix, ".input", directory);
    tempSorted = File.createTempFile(prefix, ".sorted", directory);
    
    final Sort.ByteSequencesWriter writer = new Sort.ByteSequencesWriter(tempInput);
    boolean success = false;
    try {
      BytesRef spare;
      byte[] buffer = new byte[0];
      ByteArrayDataOutput output = new ByteArrayDataOutput(buffer);

      while ((spare = source.next()) != null) {
        encode(writer, output, buffer, spare, source.weight());
      }
      writer.close();
      new Sort(tieBreakByCostComparator).sort(tempInput, tempSorted);
      ByteSequencesReader reader = new Sort.ByteSequencesReader(tempSorted);
      success = true;
      return reader;
      
    } finally {
      if (success) {
        IOUtils.close(writer);
      } else {
        try {
          IOUtils.closeWhileHandlingException(writer);
        } finally {
          close();
        }
      }
    }
  }
  
  private void close() throws IOException {
    IOUtils.close(reader);
    if (tempInput != null) {
      tempInput.delete();
    }
    if (tempSorted != null) {
      tempSorted.delete();
    }
  }
  
  /** encodes an entry (bytes+weight) to the provided writer */
  protected void encode(ByteSequencesWriter writer, ByteArrayDataOutput output, byte[] buffer, BytesRef spare, long weight) throws IOException {
    if (spare.length + 8 >= buffer.length) {
      buffer = ArrayUtil.grow(buffer, spare.length + 8);
    }
    output.reset(buffer);
    output.writeBytes(spare.bytes, spare.offset, spare.length);
    output.writeLong(weight);
    writer.write(buffer, 0, output.getPosition());
  }
  
  /** decodes the weight at the current position */
  protected long decode(BytesRef scratch, ByteArrayDataInput tmpInput) {
    tmpInput.reset(scratch.bytes);
    tmpInput.skipBytes(scratch.length - 8); // suggestion
    scratch.length -= 8; // long
    return tmpInput.readLong();
  }
}
