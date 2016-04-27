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
package org.apache.lucene.search.suggest;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.OfflineSorter.ByteSequencesReader;
import org.apache.lucene.util.OfflineSorter.ByteSequencesWriter;
import org.apache.lucene.util.OfflineSorter;

/**
 * This wrapper buffers incoming elements and makes sure they are sorted based on given comparator.
 * @lucene.experimental
 */
public class SortedInputIterator implements InputIterator {
  
  private final InputIterator source;
  private IndexOutput tempInput;
  private String tempSortedFileName;
  private final ByteSequencesReader reader;
  private final Comparator<BytesRef> comparator;
  private final boolean hasPayloads;
  private final boolean hasContexts;
  private final Directory tempDir;
  private final String tempFileNamePrefix;
  private boolean done = false;
  
  private long weight;
  private BytesRef payload = new BytesRef();
  private Set<BytesRef> contexts = null;
  
  /**
   * Creates a new sorted wrapper, using {@linkplain Comparator#naturalOrder() natural order}
   * for sorting. */
  public SortedInputIterator(Directory tempDir, String tempFileNamePrefix, InputIterator source) throws IOException {
    this(tempDir, tempFileNamePrefix, source, Comparator.naturalOrder());
  }

  /**
   * Creates a new sorted wrapper, sorting by BytesRef
   * (ascending) then cost (ascending).
   */
  public SortedInputIterator(Directory tempDir, String tempFileNamePrefix, InputIterator source, Comparator<BytesRef> comparator) throws IOException {
    this.hasPayloads = source.hasPayloads();
    this.hasContexts = source.hasContexts();
    this.source = source;
    this.comparator = comparator;
    this.tempDir = tempDir;
    this.tempFileNamePrefix = tempFileNamePrefix;
    this.reader = sort();
  }
  
  @Override
  public BytesRef next() throws IOException {
    boolean success = false;
    if (done) {
      return null;
    }
    try {
      ByteArrayDataInput input = new ByteArrayDataInput();
      BytesRef bytes = reader.next();
      if (bytes != null) {
        weight = decode(bytes, input);
        if (hasPayloads) {
          payload = decodePayload(bytes, input);
        }
        if (hasContexts) {
          contexts = decodeContexts(bytes, input);
        }
        success = true;
        return bytes;
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

  @Override
  public BytesRef payload() {
    if (hasPayloads) {
      return payload;
    }
    return null;
  }

  @Override
  public boolean hasPayloads() {
    return hasPayloads;
  }
  
  @Override
  public Set<BytesRef> contexts() {
    return contexts;
  }

  @Override
  public boolean hasContexts() {
    return hasContexts;
  }

  /** Sortes by BytesRef (ascending) then cost (ascending). */
  private final Comparator<BytesRef> tieBreakByCostComparator = new Comparator<BytesRef>() {

    private final BytesRef leftScratch = new BytesRef();
    private final BytesRef rightScratch = new BytesRef();
    private final ByteArrayDataInput input = new ByteArrayDataInput();
    
    @Override
    public int compare(BytesRef left, BytesRef right) {
      // Make shallow copy in case decode changes the BytesRef:
      assert left != right;
      leftScratch.bytes = left.bytes;
      leftScratch.offset = left.offset;
      leftScratch.length = left.length;
      rightScratch.bytes = right.bytes;
      rightScratch.offset = right.offset;
      rightScratch.length = right.length;
      long leftCost = decode(leftScratch, input);
      long rightCost = decode(rightScratch, input);
      if (hasPayloads) {
        decodePayload(leftScratch, input);
        decodePayload(rightScratch, input);
      }
      if (hasContexts) {
        decodeContexts(leftScratch, input);
        decodeContexts(rightScratch, input);
      }
      int cmp = comparator.compare(leftScratch, rightScratch);
      if (cmp != 0) {
        return cmp;
      }
      return Long.compare(leftCost, rightCost);
    }
  };
  
  private ByteSequencesReader sort() throws IOException {

    OfflineSorter sorter = new OfflineSorter(tempDir, tempFileNamePrefix, tieBreakByCostComparator);
    tempInput = tempDir.createTempOutput(tempFileNamePrefix, "input", IOContext.DEFAULT);
    
    try (OfflineSorter.ByteSequencesWriter writer = new OfflineSorter.ByteSequencesWriter(tempInput)) {
      BytesRef spare;
      byte[] buffer = new byte[0];
      ByteArrayDataOutput output = new ByteArrayDataOutput(buffer);

      while ((spare = source.next()) != null) {
        encode(writer, output, buffer, spare, source.payload(), source.contexts(), source.weight());
      }
      CodecUtil.writeFooter(tempInput);
    }

    tempSortedFileName = sorter.sort(tempInput.getName());
    return new OfflineSorter.ByteSequencesReader(tempDir.openChecksumInput(tempSortedFileName, IOContext.READONCE), tempSortedFileName);
  }
  
  private void close() throws IOException {
    try {
      IOUtils.close(reader);
    } finally {
      IOUtils.deleteFilesIgnoringExceptions(tempDir,
                                            tempInput == null ? null : tempInput.getName(),
                                            tempSortedFileName);
    }
  }
  
  /** encodes an entry (bytes+(contexts)+(payload)+weight) to the provided writer */
  protected void encode(ByteSequencesWriter writer, ByteArrayDataOutput output, byte[] buffer, BytesRef spare, BytesRef payload, Set<BytesRef> contexts, long weight) throws IOException {
    int requiredLength = spare.length + 8 + ((hasPayloads) ? 2 + payload.length : 0);
    if (hasContexts) {
      for(BytesRef ctx : contexts) {
        requiredLength += 2 + ctx.length;
      }
      requiredLength += 2; // for length of contexts
    }
    if (requiredLength >= buffer.length) {
      buffer = ArrayUtil.grow(buffer, requiredLength);
    }
    output.reset(buffer);
    output.writeBytes(spare.bytes, spare.offset, spare.length);
    if (hasContexts) {
      for (BytesRef ctx : contexts) {
        output.writeBytes(ctx.bytes, ctx.offset, ctx.length);
        output.writeShort((short) ctx.length);
      }
      output.writeShort((short) contexts.size());
    }
    if (hasPayloads) {
      output.writeBytes(payload.bytes, payload.offset, payload.length);
      output.writeShort((short) payload.length);
    }
    output.writeLong(weight);
    writer.write(buffer, 0, output.getPosition());
  }
  
  /** decodes the weight at the current position */
  protected long decode(BytesRef scratch, ByteArrayDataInput tmpInput) {
    tmpInput.reset(scratch.bytes, scratch.offset, scratch.length);
    tmpInput.skipBytes(scratch.length - 8); // suggestion
    scratch.length -= Long.BYTES; // long
    return tmpInput.readLong();
  }
  
  /** decodes the contexts at the current position */
  protected Set<BytesRef> decodeContexts(BytesRef scratch, ByteArrayDataInput tmpInput) {
    tmpInput.reset(scratch.bytes, scratch.offset, scratch.length);
    tmpInput.skipBytes(scratch.length - 2); //skip to context set size
    short ctxSetSize = tmpInput.readShort();
    scratch.length -= 2;
    final Set<BytesRef> contextSet = new HashSet<>();
    for (short i = 0; i < ctxSetSize; i++) {
      tmpInput.setPosition(scratch.offset + scratch.length - 2);
      short curContextLength = tmpInput.readShort();
      scratch.length -= 2;
      tmpInput.setPosition(scratch.offset + scratch.length - curContextLength);
      BytesRef contextSpare = new BytesRef(curContextLength);
      tmpInput.readBytes(contextSpare.bytes, 0, curContextLength);
      contextSpare.length = curContextLength;
      contextSet.add(contextSpare);
      scratch.length -= curContextLength;
    }
    return contextSet;
  }
  
  /** decodes the payload at the current position */
  protected BytesRef decodePayload(BytesRef scratch, ByteArrayDataInput tmpInput) {
    tmpInput.reset(scratch.bytes, scratch.offset, scratch.length);
    tmpInput.skipBytes(scratch.length - 2); // skip to payload size
    short payloadLength = tmpInput.readShort(); // read payload size
    assert payloadLength >= 0: payloadLength;
    tmpInput.setPosition(scratch.offset + scratch.length - 2 - payloadLength); // setPosition to start of payload
    BytesRef payloadScratch = new BytesRef(payloadLength); 
    tmpInput.readBytes(payloadScratch.bytes, 0, payloadLength); // read payload
    payloadScratch.length = payloadLength;
    scratch.length -= 2; // payload length info (short)
    scratch.length -= payloadLength; // payload
    return payloadScratch;
  }
}
