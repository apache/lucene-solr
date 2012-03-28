package org.apache.lucene.codecs.lucene3x;

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
import java.util.Comparator;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.FieldInfos;

/**
 * @lucene.experimental
 * @deprecated (4.0)
 */
@Deprecated
final class TermBuffer implements Cloneable {

  private String field;
  private Term term;                            // cached

  private BytesRef bytes = new BytesRef(10);

  // Cannot be -1 since (strangely) we write that
  // fieldNumber into index for first indexed term:
  private int currentFieldNumber = -2;

  private static final Comparator<BytesRef> utf8AsUTF16Comparator = BytesRef.getUTF8SortedAsUTF16Comparator();

  int newSuffixStart;                             // only valid right after .read is called

  public int compareTo(TermBuffer other) {
    if (field == other.field) 	  // fields are interned
                                  // (only by PreFlex codec)
      return utf8AsUTF16Comparator.compare(bytes, other.bytes);
    else
      return field.compareTo(other.field);
  }

  public void read(IndexInput input, FieldInfos fieldInfos)
    throws IOException {
    this.term = null;                           // invalidate cache
    newSuffixStart = input.readVInt();
    int length = input.readVInt();
    int totalLength = newSuffixStart + length;
    if (bytes.bytes.length < totalLength) {
      bytes.grow(totalLength);
    }
    bytes.length = totalLength;
    input.readBytes(bytes.bytes, newSuffixStart, length);
    final int fieldNumber = input.readVInt();
    if (fieldNumber != currentFieldNumber) {
      currentFieldNumber = fieldNumber;
      field = fieldInfos.fieldName(currentFieldNumber).intern();
    } else {
      assert field.equals(fieldInfos.fieldName(fieldNumber)): "currentFieldNumber=" + currentFieldNumber + " field=" + field + " vs " + fieldInfos.fieldName(fieldNumber);
    }
  }

  public void set(Term term) {
    if (term == null) {
      reset();
      return;
    }
    bytes.copyBytes(term.bytes());
    field = term.field().intern();
    currentFieldNumber = -1;
    this.term = term;
  }

  public void set(TermBuffer other) {
    field = other.field;
    currentFieldNumber = other.currentFieldNumber;
    // dangerous to copy Term over, since the underlying
    // BytesRef could subsequently be modified:
    term = null;
    bytes.copyBytes(other.bytes);
  }

  public void reset() {
    field = null;
    term = null;
    currentFieldNumber=  -1;
  }

  public Term toTerm() {
    if (field == null)                            // unset
      return null;

    if (term == null) {
      term = new Term(field, BytesRef.deepCopyOf(bytes));
    }

    return term;
  }

  @Override
  protected TermBuffer clone() {
    TermBuffer clone = null;
    try {
      clone = (TermBuffer)super.clone();
    } catch (CloneNotSupportedException e) {}
    clone.bytes = BytesRef.deepCopyOf(bytes);
    return clone;
  }
}
