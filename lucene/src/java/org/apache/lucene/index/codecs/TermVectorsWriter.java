package org.apache.lucene.index.codecs;

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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.util.Bits;

public abstract class TermVectorsWriter implements Closeable {
  
  /** Called before {@link #close()}, passing in the number
   *  of documents that were written. Note that this is 
   *  intentionally redundant (equivalent to the number of
   *  calls to XXX, but a Codec should
   *  check that this is the case to detect the JRE bug described 
   *  in LUCENE-1282. */
  public abstract void finish(int numDocs) throws IOException;
  
  /** Merges in the stored fields from the readers in 
   *  <code>mergeState</code>. The default implementation skips
   *  over deleted documents, and uses XXX, XXX, and XXX
   *  returning the number of documents that were written.
   *  Implementations can override this method for more sophisticated
   *  merging (bulk-byte copying, etc). */
  // nocommit: test that I work
  public int merge(MergeState mergeState) throws IOException {
    int docCount = 0;
    for (MergeState.IndexReaderAndLiveDocs reader : mergeState.readers) {
      final int maxDoc = reader.reader.maxDoc();
      final Bits liveDocs = reader.liveDocs;
      for (int i = 0; i < maxDoc; i++) {
        if (liveDocs != null && !liveDocs.get(i)) {
          // skip deleted docs
          continue;
        }
        // NOTE: it's very important to first assign to vectors then pass it to
        // termVectorsWriter.addAllDocVectors; see LUCENE-1282
        TermFreqVector[] vectors = reader.reader.getTermFreqVectors(i);
        addAllDocVectors(vectors);
        docCount++;
        mergeState.checkAbort.work(300);
      }
    }
    finish(docCount);
    return docCount;
  }
  
  // nocommit: this should be a sugar method only that consumes the normal api (once we have one)
  public abstract void addAllDocVectors(TermFreqVector[] vectors) throws IOException;
  
  // nocommit: nuke this
  public abstract void addRawDocuments(TermVectorsReader reader, int[] tvdLengths, int[] tvfLengths, int numDocs) throws IOException;
}
