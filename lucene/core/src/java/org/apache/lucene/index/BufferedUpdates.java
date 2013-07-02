package org.apache.lucene.index;

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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.util.RamUsageEstimator;

/* Holds buffered updates by term for a
 * single segment. This is used to hold buffered pending
 * updates against the to-be-flushed segment.  Once the
 * updates are pushed (on flush in DocumentsWriter), these
 * updates are converted to a FrozenUpdates instance. */

// NOTE: we are sync'd by BufferedUpdates, ie, all access to
// instances of this class is via sync'd methods on
// BufferedUpdates

class BufferedUpdates {

  final AtomicInteger numTermUpdates = new AtomicInteger();
  final ConcurrentSkipListMap<Term,List<FieldsUpdate>> terms = new ConcurrentSkipListMap<Term,List<FieldsUpdate>>();

  public static final Integer MAX_INT = Integer.valueOf(Integer.MAX_VALUE);

  final AtomicLong bytesUsed;

  private final static boolean VERBOSE_DELETES = false;

  long gen;
  public BufferedUpdates() {
    this(new AtomicLong());
  }

  BufferedUpdates(AtomicLong bytesUsed) {
    assert bytesUsed != null;
    this.bytesUsed = bytesUsed;
  }

  @Override
  public String toString() {
    if (VERBOSE_DELETES) {
      return "gen=" + gen + " numTerms=" + numTermUpdates + ", terms=" + terms
        + ", bytesUsed=" + bytesUsed;
    } else {
      String s = "gen=" + gen;
      if (numTermUpdates.get() != 0) {
        s += " " + numTermUpdates.get() + " updated terms (unique count=" + terms.size() + ")";
      }
      if (bytesUsed.get() != 0) {
        s += " bytesUsed=" + bytesUsed.get();
      }

      return s;
    }
  }

  public synchronized void addTerm(Term term, FieldsUpdate update) {
    List<FieldsUpdate> current = terms.get(term);

    if (current == null) {
      current = new ArrayList<FieldsUpdate>(1);
      terms.put(term, current);
      bytesUsed.addAndGet(BufferedDeletes.BYTES_PER_DEL_TERM
          + term.bytes.length
          + (RamUsageEstimator.NUM_BYTES_CHAR * term.field().length()));
    }
    current.add(update);
    numTermUpdates.incrementAndGet();
  }
 
  void clear() {
    terms.clear();
    numTermUpdates.set(0);
    bytesUsed.set(0);
  }
  
  boolean any() {
    return terms.size() > 0;
  }
}
