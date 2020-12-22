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
package org.apache.lucene.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.StringHelper;

/**
 * Prefix codes term instances (prefixes are shared). This is expected to be faster to build than a
 * FST and might also be more compact if there are no common suffixes.
 *
 * @lucene.internal
 */
public class PrefixCodedTerms implements Accountable {
  private final List<ByteBuffer> content;
  private final long size;
  private long delGen;
  private int lazyHash;

  private PrefixCodedTerms(List<ByteBuffer> content, long size) {
    this.content = Objects.requireNonNull(content);
    this.size = size;
  }

  @Override
  public long ramBytesUsed() {
    return content.stream().mapToLong(buf -> buf.capacity()).sum() + 2 * Long.BYTES;
  }

  /** Records del gen for this packet. */
  public void setDelGen(long delGen) {
    this.delGen = delGen;
  }

  /** Builds a PrefixCodedTerms: call add repeatedly, then finish. */
  public static class Builder {
    private ByteBuffersDataOutput output = new ByteBuffersDataOutput();
    private Term lastTerm = new Term("");
    private BytesRefBuilder lastTermBytes = new BytesRefBuilder();
    private long size;

    /** Sole constructor. */
    public Builder() {}

    /** add a term */
    public void add(Term term) {
      add(term.field(), term.bytes());
    }

    /** add a term. This fully consumes in the incoming {@link BytesRef}. */
    public void add(String field, BytesRef bytes) {
      assert lastTerm.equals(new Term("")) || new Term(field, bytes).compareTo(lastTerm) > 0;

      try {
        final int prefix;
        if (size > 0 && field.equals(lastTerm.field)) {
          // same field as the last term
          prefix = StringHelper.bytesDifference(lastTerm.bytes, bytes);
          output.writeVInt(prefix << 1);
        } else {
          // field change
          prefix = 0;
          output.writeVInt(1);
          output.writeString(field);
        }

        int suffix = bytes.length - prefix;
        output.writeVInt(suffix);
        output.writeBytes(bytes.bytes, bytes.offset + prefix, suffix);
        lastTermBytes.copyBytes(bytes);
        lastTerm.bytes = lastTermBytes.get();
        lastTerm.field = field;
        size += 1;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    /** return finalized form */
    public PrefixCodedTerms finish() {
      return new PrefixCodedTerms(output.toBufferList(), size);
    }
  }

  /** An iterator over the list of terms stored in a {@link PrefixCodedTerms}. */
  public static class TermIterator extends FieldTermIterator {
    final ByteBuffersDataInput input;
    final BytesRefBuilder builder = new BytesRefBuilder();
    final BytesRef bytes = builder.get();
    final long end;
    final long delGen;
    String field = "";

    private TermIterator(long delGen, ByteBuffersDataInput input) {
      this.input = input;
      end = input.size();
      this.delGen = delGen;
    }

    @Override
    public BytesRef next() {
      if (input.position() < end) {
        try {
          int code = input.readVInt();
          boolean newField = (code & 1) != 0;
          if (newField) {
            field = input.readString();
          }
          int prefix = code >>> 1;
          int suffix = input.readVInt();
          readTermBytes(prefix, suffix);
          return bytes;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        field = null;
        return null;
      }
    }

    // TODO: maybe we should freeze to FST or automaton instead?
    private void readTermBytes(int prefix, int suffix) throws IOException {
      builder.grow(prefix + suffix);
      input.readBytes(builder.bytes(), prefix, suffix);
      builder.setLength(prefix + suffix);
    }

    // Copied from parent-class because javadoc doesn't do it for some reason
    /**
     * Returns current field. This method should not be called after iteration is done. Note that
     * you may use == to detect a change in field.
     */
    @Override
    public String field() {
      return field;
    }

    // Copied from parent-class because javadoc doesn't do it for some reason
    /** Del gen of the current term. */
    @Override
    public long delGen() {
      return delGen;
    }
  }

  /** Return an iterator over the terms stored in this {@link PrefixCodedTerms}. */
  public TermIterator iterator() {
    return new TermIterator(delGen, new ByteBuffersDataInput(content));
  }

  /** Return the number of terms stored in this {@link PrefixCodedTerms}. */
  public long size() {
    return size;
  }

  @Override
  public int hashCode() {
    if (lazyHash == 0) {
      int h = 1;
      for (ByteBuffer bb : content) {
        h = h + 31 * bb.hashCode();
      }
      h = 31 * h + (int) (delGen ^ (delGen >>> 32));
      lazyHash = h;
    }
    return lazyHash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    PrefixCodedTerms other = (PrefixCodedTerms) obj;
    return delGen == other.delGen && this.content.equals(other.content);
  }
}
