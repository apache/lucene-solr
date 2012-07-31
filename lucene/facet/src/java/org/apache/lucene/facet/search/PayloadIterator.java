package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

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

/**
 * A utility class for iterating through a posting list of a given term and
 * retrieving the payload of the first occurrence in every document. Comes with
 * its own working space (buffer).
 * 
 * @lucene.experimental
 */
public class PayloadIterator {

  protected byte[] buffer;
  protected int payloadLength;

  DocsAndPositionsEnum tp;

  private boolean hasMore;

  public PayloadIterator(IndexReader indexReader, Term term)
      throws IOException {
    this(indexReader, term, new byte[1024]);
  }

  public PayloadIterator(IndexReader indexReader, Term term, byte[] buffer)
      throws IOException {
    this.buffer = buffer;
    // TODO (Facet): avoid Multi*?
    Bits liveDocs = MultiFields.getLiveDocs(indexReader);
    this.tp = MultiFields.getTermPositionsEnum(indexReader, liveDocs, term.field(), term.bytes(), DocsAndPositionsEnum.FLAG_PAYLOADS);
  }

  /**
   * (re)initialize the iterator. Should be done before the first call to
   * {@link #setdoc(int)}. Returns false if there is no category list found
   * (no setdoc() will never return true).
   */
  public boolean init() throws IOException {
    hasMore = tp != null && tp.nextDoc() != DocIdSetIterator.NO_MORE_DOCS;
    return hasMore;
  }

  /**
   * Skip forward to document docId. Return true if this document exists and
   * has any payload.
   * <P>
   * Users should call this method with increasing docIds, and implementations
   * can assume that this is the case.
   */
  public boolean setdoc(int docId) throws IOException {
    if (!hasMore) {
      return false;
    }
    
    if (tp.docID() > docId) {
      return false;
    }

    // making sure we have the requested document
    if (tp.docID() < docId) {
      // Skipping to requested document
      if (tp.advance(docId) == DocIdSetIterator.NO_MORE_DOCS) {
        this.hasMore = false;
        return false;
      }

      // If document not found (skipped to much)
      if (tp.docID() != docId) {
        return false;
      }
    }

    // Prepare for payload extraction
    tp.nextPosition();

    // TODO: fix bug in SepCodec and then remove this check (the null check should be enough)
    if (!tp.hasPayload()) {
      return false;
    }

    BytesRef br = tp.getPayload();

    if (br == null || br.length == 0) {
      return false;
    }

    this.payloadLength = br.length;
    
    if (this.payloadLength > this.buffer.length) {
      // Growing if necessary.
      this.buffer = new byte[this.payloadLength * 2 + 1];
    }
    // Loading the payload
    System.arraycopy(br.bytes, br.offset, this.buffer, 0, payloadLength);
    return true;
  }

  /**
   * Get the buffer with the content of the last read payload.
   */
  public byte[] getBuffer() {
    return buffer;
  }

  /**
   * Get the length of the last read payload.
   */
  public int getPayloadLength() {
    return payloadLength;
  }

}
