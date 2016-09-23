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

package org.apache.lucene.concordance.charoffsets;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttributeImpl;
import org.apache.lucene.document.Document;



/**
 * Simple class to store a document id (atomic and unique), a StoredDocument,
 * and the offsets for a SpanQuery hit
 */
public class DocTokenOffsets {
  private int atomicDocId = -1;
  private long uniqueId = -1;
  private Document document = null;
  private List<OffsetAttribute> offsets = new ArrayList<>();

  public void addOffset(int start, int end) {
    OffsetAttributeImpl offset = new OffsetAttributeImpl();
    offset.setOffset(start, end);
    offsets.add(offset);
  }

  public void reset(int base, int atomicDocId, Document d) {
    this.atomicDocId = atomicDocId;
    this.uniqueId = base + atomicDocId;
    setDocument(d);
    offsets.clear();
  }

  public List<OffsetAttribute> getOffsets() {
    return offsets;
  }

  public Document getDocument() {
    return document;
  }

  public void setDocument(Document d) {
    this.document = d;
  }

  /*
   * required by DocTokenOffsetsIterator
   */
  protected int getAtomicDocId() {
    return atomicDocId;
  }

  public long getUniqueDocId() {
    return uniqueId;
  }

  public DocTokenOffsets deepishCopy() {
    DocTokenOffsets copy = new DocTokenOffsets();
    copy.atomicDocId = atomicDocId;
    copy.uniqueId = uniqueId;
    copy.document = document;
    List<OffsetAttribute> copyOffsets = new ArrayList<OffsetAttribute>();
    copyOffsets.addAll(offsets);
    copy.offsets = copyOffsets;
    return copy;
  }

  public boolean isEmpty() {
    if (atomicDocId < 0)
      return true;
    return false;
  }

  public void pseudoEmpty() {
    atomicDocId = -1;
  }
}
