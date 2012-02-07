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

import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.ReaderUtil;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

/**
 * Exposes flex API, merged from flex API of sub-segments.
 * This does a merge sort, by field name, of the
 * sub-readers.
 *
 * @lucene.experimental
 */

public final  class MultiFieldsEnum extends FieldsEnum {
  private final FieldMergeQueue queue;

  // Holds sub-readers containing field we are currently
  // on, popped from queue.
  private final FieldsEnumWithSlice[] top;

  private int numTop;

  private final Fields fields;

  private String currentField;

  /** The subs array must be newly initialized FieldsEnum
   *  (ie, {@link FieldsEnum#next} has not been called. */
  public MultiFieldsEnum(MultiFields fields, FieldsEnum[] subs, ReaderUtil.Slice[] subSlices) throws IOException {
    this.fields = fields;
    queue = new FieldMergeQueue(subs.length);
    top = new FieldsEnumWithSlice[subs.length];
    List<FieldsEnumWithSlice> enumWithSlices = new ArrayList<FieldsEnumWithSlice>();

    // Init q
    for(int i=0;i<subs.length;i++) {
      assert subs[i] != null;
      final String field = subs[i].next();
      if (field != null) {
        // this FieldsEnum has at least one field
        final FieldsEnumWithSlice sub = new FieldsEnumWithSlice(subs[i], subSlices[i], i);
        enumWithSlices.add(sub);
        sub.current = field;
        queue.add(sub);
      }
    }
  }

  @Override
  public String next() throws IOException {

    // restore queue
    for(int i=0;i<numTop;i++) {
      top[i].current = top[i].fields.next();
      if (top[i].current != null) {
        queue.add(top[i]);
      } else {
        // no more fields in this sub-reader
      }
    }

    numTop = 0;

    // gather equal top fields
    if (queue.size() > 0) {
      while(true) {
        top[numTop++] = queue.pop();
        if (queue.size() == 0 || !(queue.top()).current.equals(top[0].current)) {
          break;
        }
      }
      currentField = top[0].current;
    } else {
      currentField = null;
    }

    return currentField;
  }

  @Override
  public Terms terms() throws IOException {
    // Ask our parent MultiFields:
    return fields.terms(currentField);
  }

  public final static class FieldsEnumWithSlice {
    public static final FieldsEnumWithSlice[] EMPTY_ARRAY = new FieldsEnumWithSlice[0];
    final FieldsEnum fields;
    final ReaderUtil.Slice slice;
    final int index;
    String current;

    public FieldsEnumWithSlice(FieldsEnum fields, ReaderUtil.Slice slice, int index) throws IOException {
      this.slice = slice;
      this.index = index;
      assert slice.length >= 0: "length=" + slice.length;
      this.fields = fields;
    }
  }

  private final static class FieldMergeQueue extends PriorityQueue<FieldsEnumWithSlice> {
    FieldMergeQueue(int size) {
      super(size);
    }

    @Override
    protected final boolean lessThan(FieldsEnumWithSlice fieldsA, FieldsEnumWithSlice fieldsB) {
      // No need to break ties by field name: TermsEnum handles that
      return fieldsA.current.compareTo(fieldsB.current) < 0;
    }
  }
}

