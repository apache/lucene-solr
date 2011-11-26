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
import java.util.Iterator;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RAMFile;
import org.apache.lucene.store.RAMInputStream;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;

/**
 * Prefix codes term instances (prefixes are shared)
 * @lucene.experimental
 */
class PrefixCodedTerms implements Iterable<Term> {
  final RAMFile buffer;
  
  private PrefixCodedTerms(RAMFile buffer) {
    this.buffer = buffer;
  }
  
  /** @return size in bytes */
  public long getSizeInBytes() {
    return buffer.getSizeInBytes();
  }
  
  /** @return iterator over the bytes */
  public Iterator<Term> iterator() {
    return new PrefixCodedTermsIterator();
  }
  
  class PrefixCodedTermsIterator implements Iterator<Term> {
    final IndexInput input;
    String field = "";
    BytesRef bytes = new BytesRef();
    Term term = new Term(field, bytes);

    PrefixCodedTermsIterator() {
      try {
        input = new RAMInputStream("PrefixCodedTermsIterator", buffer);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public boolean hasNext() {
      return input.getFilePointer() < input.length();
    }
    
    public Term next() {
      assert hasNext();
      try {
        int code = input.readVInt();
        if ((code & 1) != 0) {
          // new field
          field = input.readString();
        }
        int prefix = code >>> 1;
        int suffix = input.readVInt();
        bytes.grow(prefix + suffix);
        input.readBytes(bytes.bytes, prefix, suffix);
        bytes.length = prefix + suffix;
        term.set(field, bytes);
        return term;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
  
  /** Builds a PrefixCodedTerms: call add repeatedly, then finish. */
  public static class Builder {
    private RAMFile buffer = new RAMFile();
    private RAMOutputStream output = new RAMOutputStream(buffer);
    private Term lastTerm = new Term("");

    /** add a term */
    public void add(Term term) {
      assert lastTerm.equals(new Term("")) || term.compareTo(lastTerm) > 0;

      try {
        int prefix = sharedPrefix(lastTerm.bytes, term.bytes);
        int suffix = term.bytes.length - prefix;
        if (term.field.equals(lastTerm.field)) {
          output.writeVInt(prefix << 1);
        } else {
          output.writeVInt(prefix << 1 | 1);
          output.writeString(term.field);
        }
        output.writeVInt(suffix);
        output.writeBytes(term.bytes.bytes, term.bytes.offset + prefix, suffix);
        lastTerm.bytes.copyBytes(term.bytes);
        lastTerm.field = term.field;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    /** return finalized form */
    public PrefixCodedTerms finish() {
      try {
        output.close();
        return new PrefixCodedTerms(buffer);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    private int sharedPrefix(BytesRef term1, BytesRef term2) {
      int pos1 = 0;
      int pos1End = pos1 + Math.min(term1.length, term2.length);
      int pos2 = 0;
      while(pos1 < pos1End) {
        if (term1.bytes[term1.offset + pos1] != term2.bytes[term2.offset + pos2]) {
          return pos1;
        }
        pos1++;
        pos2++;
      }
      return pos1;
    }
  }
}
