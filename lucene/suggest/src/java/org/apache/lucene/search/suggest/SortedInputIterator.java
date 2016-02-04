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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.OfflineSorter.ByteSequencesReader;
import org.apache.lucene.util.OfflineSorter.ByteSequencesWriter;

/**
 * This wrapper buffers incoming elements and makes sure they are sorted based on given comparator.
 * @lucene.experimental
 */
public class SortedInputIterator implements InputIterator {
  
  private final InputIterator source;
  private Path tempInput;
  private Path tempSorted;
  private final ByteSequencesReader reader;
  private final Comparator<BytesRef> comparator;
  private final boolean hasPayloads;
  private final boolean hasContexts;
  private boolean done = false;
  
  private long weight;
  private final BytesRefBuilder scratch = new BytesRefBuilder();
  private BytesRef payload = new BytesRef();
  private Set<BytesRef> contexts = null;
  
  /**
   * Creates a new sorted wrapper, using {@link
   * BytesRef#getUTF8SortedAsUnicodeComparator} for
   * sorting. */
  public SortedInputIterator(InputIterator source) throws IOException {
    this(source, BytesRef.getUTF8SortedAsUnicodeComparator());
  }

  /**
   * Creates a new sorted wrapper, sorting by BytesRef
   * (ascending) then cost (ascending).
   */
  public SortedInputIterator(InputIterator source, Comparator<BytesRef> comparator) throws IOException {
    this.hasPayloads = source.hasPayloads();
    this.hasContexts = source.hasContexts();
    this.source = source;
    this.comparator = comparator;
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
      if (reader.read(scratch)) {
      final BytesRef bytes = scratch.get();
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
    String prefix = getClass().getSimpleName();
    Path directory = OfflineSorter.getDefaultTempDir();
    tempInput = Files.createTempFile(directory, prefix, ".input");
    tempSorted = Files.createTempFile(directory, prefix, ".sorted");
    
    final OfflineSorter.ByteSequencesWriter writer = new OfflineSorter.ByteSequencesWriter(tempInput);
    boolean success = false;
    try {
      BytesRef spare;
      byte[] buffer = new byte[0];
      ByteArrayDataOutput output = new ByteArrayDataOutput(buffer);

      while ((spare = source.next()) != null) {
        encode(writer, output, buffer, spare, source.payload(), source.contexts(), source.weight());
      }
      writer.close();
      new OfflineSorter(tieBreakByCostComparator).sort(tempInput, tempSorted);
      ByteSequencesReader reader = new OfflineSorter.ByteSequencesReader(tempSorted);
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
    boolean success = false;
    try {
      IOUtils.close(reader);
      success = true;
    } finally {
      if (success) {
        IOUtils.deleteFilesIfExist(tempInput, tempSorted);
      } else {
        IOUtils.deleteFilesIgnoringExceptions(tempInput, tempSorted);
      }
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
    tmpInput.reset(scratch.bytes);
    tmpInput.skipBytes(scratch.length - 8); // suggestion
    scratch.length -= 8; // long
    return tmpInput.readLong();
  }
  
  /** decodes the contexts at the current position */
  protected Set<BytesRef> decodeContexts(BytesRef scratch, ByteArrayDataInput tmpInput) {
    tmpInput.reset(scratch.bytes);
    tmpInput.skipBytes(scratch.length - 2); //skip to context set size
    short ctxSetSize = tmpInput.readShort();
    scratch.length -= 2;
    final Set<BytesRef> contextSet = new HashSet<>();
    for (short i = 0; i < ctxSetSize; i++) {
      tmpInput.setPosition(scratch.length - 2);
      short curContextLength = tmpInput.readShort();
      scratch.length -= 2;
      tmpInput.setPosition(scratch.length - curContextLength);
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
    tmpInput.reset(scratch.bytes);
    tmpInput.skipBytes(scratch.length - 2); // skip to payload size
    short payloadLength = tmpInput.readShort(); // read payload size
    tmpInput.setPosition(scratch.length - 2 - payloadLength); // setPosition to start of payload
    BytesRef payloadScratch = new BytesRef(payloadLength); 
    tmpInput.readBytes(payloadScratch.bytes, 0, payloadLength); // read payload
    payloadScratch.length = payloadLength;
    scratch.length -= 2; // payload length info (short)
    scratch.length -= payloadLength; // payload
    return payloadScratch;
  }
}
