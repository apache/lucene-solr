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

import java.util.HashMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.HashSet;
import java.io.IOException;

import org.apache.lucene.util.ArrayUtil;

/** This is just a "splitter" class: it lets you wrap two
 *  DocFieldConsumer instances as a single consumer. */

final class DocFieldConsumers extends DocFieldConsumer {
  final DocFieldConsumer one;
  final DocFieldConsumer two;

  public DocFieldConsumers(DocFieldConsumer one, DocFieldConsumer two) {
    this.one = one;
    this.two = two;
  }

  void setFieldInfos(FieldInfos fieldInfos) {
    super.setFieldInfos(fieldInfos);
    one.setFieldInfos(fieldInfos);
    two.setFieldInfos(fieldInfos);
  }

  public void flush(Map threadsAndFields, DocumentsWriter.FlushState state) throws IOException {

    Map oneThreadsAndFields = new HashMap();
    Map twoThreadsAndFields = new HashMap();

    Iterator it = threadsAndFields.entrySet().iterator();
    while(it.hasNext()) {

      Map.Entry entry = (Map.Entry) it.next();

      DocFieldConsumersPerThread perThread = (DocFieldConsumersPerThread) entry.getKey();

      Collection fields = (Collection) entry.getValue();

      Iterator fieldsIt = fields.iterator();
      Collection oneFields = new HashSet();
      Collection twoFields = new HashSet();
      while(fieldsIt.hasNext()) {
        DocFieldConsumersPerField perField = (DocFieldConsumersPerField) fieldsIt.next();
        oneFields.add(perField.one);
        twoFields.add(perField.two);
      }

      oneThreadsAndFields.put(perThread.one, oneFields);
      twoThreadsAndFields.put(perThread.two, twoFields);
    }
    

    one.flush(oneThreadsAndFields, state);
    two.flush(twoThreadsAndFields, state);
  }

  public void closeDocStore(DocumentsWriter.FlushState state) throws IOException {      
    try {
      one.closeDocStore(state);
    } finally {
      two.closeDocStore(state);
    }
  }

  public void abort() {
    try {
      one.abort();
    } finally {
      two.abort();
    }
  }

  public boolean freeRAM() {
    boolean any = one.freeRAM();
    any |= two.freeRAM();
    return any;
  }

  public DocFieldConsumerPerThread addThread(DocFieldProcessorPerThread docFieldProcessorPerThread) throws IOException {
    return new DocFieldConsumersPerThread(docFieldProcessorPerThread, this, one.addThread(docFieldProcessorPerThread), two.addThread(docFieldProcessorPerThread));
  }

  PerDoc[] docFreeList = new PerDoc[1];
  int freeCount;
  int allocCount;

  synchronized PerDoc getPerDoc() {
    if (freeCount == 0) {
      allocCount++;
      if (allocCount > docFreeList.length) {
        // Grow our free list up front to make sure we have
        // enough space to recycle all outstanding PerDoc
        // instances
        assert allocCount == 1+docFreeList.length;
        docFreeList = new PerDoc[ArrayUtil.getNextSize(allocCount)];
      }
      return new PerDoc();
    } else
      return docFreeList[--freeCount];
  }

  synchronized void freePerDoc(PerDoc perDoc) {
    assert freeCount < docFreeList.length;
    docFreeList[freeCount++] = perDoc;
  }

  class PerDoc extends DocumentsWriter.DocWriter {

    DocumentsWriter.DocWriter one;
    DocumentsWriter.DocWriter two;

    public long sizeInBytes() {
      return one.sizeInBytes() + two.sizeInBytes();
    }

    public void finish() throws IOException {
      try {
        try {
          one.finish();
        } finally {
          two.finish();
        }
      } finally {
        freePerDoc(this);
      }
    }

    public void abort() {
      try {
        try {
          one.abort();
        } finally {
          two.abort();
        }
      } finally {
        freePerDoc(this);
      }
    }
  }
}
