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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader.ReaderClosedListener;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;

// nocommit javadocs

// nocommit better name

// TODO: should this class handle deletions better...?
final class LiveUniqueValues implements ReferenceManager.RefreshListener, Closeable, Accountable {

  // Holds reused TermsEnum/DocsEnum state for faster lookups:
  private final ConcurrentMap<IndexReader,CloseableThreadLocal<PerThreadLookup>> lookupStates = new ConcurrentHashMap<>();

  // Evicts this reader from lookupStates once it's closed:
  private final ReaderClosedListener removeLookupState = new ReaderClosedListener() {
      @Override
      public void onClose(IndexReader reader) {
        CloseableThreadLocal<PerThreadLookup> ctl = lookupStates.remove(reader);
        if (ctl != null) {
          ctl.close();
        }
      }
    };

  // Maps the id to TRUE if it's live, else FALSE:
  private volatile Map<BytesRef,Boolean> old = newMap();
  private volatile Map<BytesRef,Boolean> current = newMap();
  private final ReaderManager mgr;
  private final String uidField;

  private static Map<BytesRef,Boolean> newMap() {
    return new HashMap<BytesRef,Boolean>();
  }

  /** Sole constructor. */
  public LiveUniqueValues(String uidField, ReaderManager mgr) {
    this.uidField = uidField;
    this.mgr = mgr;
    mgr.addListener(this);
  }

  @Override
  public void close() {
    mgr.removeListener(this);
  }

  @Override
  public synchronized void beforeRefresh() throws IOException {
    old = current;
    // Start sending all updates after this point to the new
    // map.  While reopen is running, any lookup will first
    // try this new map, then fallback to old, then to the
    // current searcher:
    current = newMap();
  }

  @Override
  public synchronized void afterRefresh(boolean didRefresh) throws IOException {
    // Now drop all the old values because they are now
    // visible via the searcher that was just opened; if
    // didRefresh is false, it's possible old has some
    // entries in it, which is fine: it means they were
    // actually already included in the previously opened
    // reader.  So we can safely clear old here:
    old = newMap();
  }

  /** Call this to try adding a value; this returns false if the add
   *  fails because the value is already present in this field. */
  // TODO: improve concurrency
  public synchronized boolean add(BytesRef id) throws IOException {
    Boolean v = current.get(id);
    if (v != null) {
      if (v == Boolean.FALSE) {
        current.put(id, Boolean.TRUE);
        return true;
      } else {
        return false;
      }
    }
    v = old.get(id);
    if (v != null) {
      if (v == Boolean.FALSE) {
        current.put(id, Boolean.TRUE);
        return true;
      } else {
        return false;
      }
    }
    DirectoryReader reader = mgr.acquire();
    try {
      PerThreadLookup lookup = getLookupState(reader);
      if (lookup.exists(id)) {
        return false;
      } else {
        current.put(id, Boolean.TRUE);
        return true;
      }
    } finally {
      mgr.release(reader);
    }
  }

  /** Call this after you've successfully deleted a document
   *  from the index. */
  public synchronized void delete(BytesRef id) {
    current.put(id, Boolean.FALSE);
  }

  /** Returns the [approximate] number of id/value pairs
   *  buffered in RAM. */
  public synchronized int size() {
    return current.size() + old.size();
  }

  private PerThreadLookup getLookupState(DirectoryReader reader) throws IOException {
    CloseableThreadLocal<PerThreadLookup> ctl = lookupStates.get(reader);
    if (ctl == null) {
      // First time we are seeing this reader; make a new CTL:
      ctl = new CloseableThreadLocal<PerThreadLookup>();
      CloseableThreadLocal<PerThreadLookup> other = lookupStates.putIfAbsent(reader, ctl);
      if (other == null) {
        // Our CTL won, we must remove it when the reader is closed:
        reader.addReaderClosedListener(removeLookupState);
      } else {
        // Another thread beat us to it: just use their CTL:
        ctl.close();
        ctl = other;
      }
    }

    PerThreadLookup lookupState = ctl.get();
    if (lookupState == null) {
      // First time this thread searches this reader:
      lookupState = new PerThreadLookup(reader, uidField);
      ctl.set(lookupState);
    }

    return lookupState;
  }

  public long ramBytesUsed() {
    // nocommit todo
    return 0;
  }

  public Iterable<? extends Accountable> getChildResources() {
    return Collections.emptyList();
  }

  // TODO: optimize this so that on toplevel reader reopen, we reuse TermsEnum for shared segments:
  private final static class PerThreadLookup {

    private final LeafReaderContext[] readerContexts;
    private final TermsEnum[] termsEnums;
    private final DocsEnum[] docsEnums;
    private final Bits[] liveDocs;
    private final int numSegs;
    private final boolean hasDeletions;

    public PerThreadLookup(IndexReader r, String uidFieldName) throws IOException {

      List<LeafReaderContext> leaves = new ArrayList<>(r.leaves());

      readerContexts = leaves.toArray(new LeafReaderContext[leaves.size()]);
      termsEnums = new TermsEnum[leaves.size()];
      docsEnums = new DocsEnum[leaves.size()];
      liveDocs = new Bits[leaves.size()];
      int numSegs = 0;
      boolean hasDeletions = false;

      // iterate backwards to optimize for the frequently updated documents
      // which are likely to be in the last segments
      for(int i=leaves.size()-1;i>=0;i--) {
        LeafReaderContext readerContext = leaves.get(i);
        Fields fields = readerContext.reader().fields();
        if (fields != null) {
          Terms terms = fields.terms(uidFieldName);
          if (terms != null) {
            readerContexts[numSegs] = readerContext;
            termsEnums[numSegs] = terms.iterator(null);
            assert termsEnums[numSegs] != null;
            liveDocs[numSegs] = readerContext.reader().getLiveDocs();
            hasDeletions |= readerContext.reader().hasDeletions();
            numSegs++;
          }
        }
      }
      this.numSegs = numSegs;
      this.hasDeletions = hasDeletions;
    }

    /** Return true if id is found. */
    public boolean exists(BytesRef id) throws IOException {
      for(int seg=0;seg<numSegs;seg++) {
        if (termsEnums[seg].seekExact(id)) {
          // nocommit once we remove deleted postings on flush we don't need the live docs:
          DocsEnum docs = docsEnums[seg] = termsEnums[seg].docs(liveDocs[seg], docsEnums[seg], 0);
          int docID = docs.nextDoc();
          if (docID != DocsEnum.NO_MORE_DOCS) {
            assert docs.nextDoc() == DocsEnum.NO_MORE_DOCS;
            return true;
          } else {
            assert hasDeletions;
          }
        }
      }
      return false;
    }
  }

}

