package org.apache.lucene.sandbox.queries;
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

import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;

public class DuplicateFilter extends Filter {
  // TODO: make duplicate filter aware of ReaderContext such that we can
  // filter duplicates across segments

  /**
   * KeepMode determines which document id to consider as the master, all others being
   * identified as duplicates. Selecting the "first occurrence" can potentially save on IO.
   */
  public enum KeepMode {
    KM_USE_FIRST_OCCURRENCE, KM_USE_LAST_OCCURRENCE
  }

  private KeepMode keepMode;

  /**
   * "Full" processing mode starts by setting all bits to false and only setting bits
   * for documents that contain the given field and are identified as none-duplicates.
   * <p/>
   * "Fast" processing sets all bits to true then unsets all duplicate docs found for the
   * given field. This approach avoids the need to read TermDocs for terms that are seen
   * to have a document frequency of exactly "1" (i.e. no duplicates). While a potentially
   * faster approach , the downside is that bitsets produced will include bits set for
   * documents that do not actually contain the field given.
   */

  public enum ProcessingMode {
    PM_FULL_VALIDATION, PM_FAST_INVALIDATION
  }

  private ProcessingMode processingMode;

  private String fieldName;

  public DuplicateFilter(String fieldName) {
    this(fieldName, KeepMode.KM_USE_LAST_OCCURRENCE, ProcessingMode.PM_FULL_VALIDATION);
  }

  public DuplicateFilter(String fieldName, KeepMode keepMode, ProcessingMode processingMode) {
    this.fieldName = fieldName;
    this.keepMode = keepMode;
    this.processingMode = processingMode;
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    if (processingMode == ProcessingMode.PM_FAST_INVALIDATION) {
      return fastBits(context.reader(), acceptDocs);
    } else {
      return correctBits(context.reader(), acceptDocs);
    }
  }

  private FixedBitSet correctBits(AtomicReader reader, Bits acceptDocs) throws IOException {
    FixedBitSet bits = new FixedBitSet(reader.maxDoc()); //assume all are INvalid
    Terms terms = reader.fields().terms(fieldName);

    if (terms == null) {
      return bits;
    }

    TermsEnum termsEnum = terms.iterator(null);
    DocsEnum docs = null;
    while (true) {
      BytesRef currTerm = termsEnum.next();
      if (currTerm == null) {
        break;
      } else {
        docs = termsEnum.docs(acceptDocs, docs, false);
        int doc = docs.nextDoc();
        if (doc != DocIdSetIterator.NO_MORE_DOCS) {
          if (keepMode == KeepMode.KM_USE_FIRST_OCCURRENCE) {
            bits.set(doc);
          } else {
            int lastDoc = doc;
            while (true) {
              lastDoc = doc;
              doc = docs.nextDoc();
              if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                break;
              }
            }
            bits.set(lastDoc);
          }
        }
      }
    }
    return bits;
  }

  private FixedBitSet fastBits(AtomicReader reader, Bits acceptDocs) throws IOException {
    FixedBitSet bits = new FixedBitSet(reader.maxDoc());
    bits.set(0, reader.maxDoc()); //assume all are valid
    Terms terms = reader.fields().terms(fieldName);

    if (terms == null) {
      return bits;
    }

    TermsEnum termsEnum = terms.iterator(null);
    DocsEnum docs = null;
    while (true) {
      BytesRef currTerm = termsEnum.next();
      if (currTerm == null) {
        break;
      } else {
        if (termsEnum.docFreq() > 1) {
          // unset potential duplicates
          docs = termsEnum.docs(acceptDocs, docs, false);
          int doc = docs.nextDoc();
          if (doc != DocIdSetIterator.NO_MORE_DOCS) {
            if (keepMode == KeepMode.KM_USE_FIRST_OCCURRENCE) {
              doc = docs.nextDoc();
            }
          }

          int lastDoc = -1;
          while (true) {
            lastDoc = doc;
            bits.clear(lastDoc);
            doc = docs.nextDoc();
            if (doc == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            }
          }

          if (keepMode == KeepMode.KM_USE_LAST_OCCURRENCE) {
            // restore the last bit
            bits.set(lastDoc);
          }
        }
      }
    }

    return bits;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public KeepMode getKeepMode() {
    return keepMode;
  }

  public void setKeepMode(KeepMode keepMode) {
    this.keepMode = keepMode;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (obj.getClass() != this.getClass())) {
      return false;
    }

    DuplicateFilter other = (DuplicateFilter) obj;
    return keepMode == other.keepMode &&
        processingMode == other.processingMode &&
        fieldName != null && fieldName.equals(other.fieldName);
  }

  @Override
  public int hashCode() {
    int hash = 217;
    hash = 31 * hash + keepMode.hashCode();
    hash = 31 * hash + processingMode.hashCode();
    hash = 31 * hash + fieldName.hashCode();
    return hash;
  }

  public ProcessingMode getProcessingMode() {
    return processingMode;
  }

  public void setProcessingMode(ProcessingMode processingMode) {
    this.processingMode = processingMode;
  }
}
