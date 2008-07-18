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

final class DocFieldConsumersPerThread extends DocFieldConsumerPerThread {

  final DocFieldConsumerPerThread one;
  final DocFieldConsumerPerThread two;
  final DocFieldConsumers parent;
  final DocumentsWriter.DocState docState;

  public DocFieldConsumersPerThread(DocFieldProcessorPerThread docFieldProcessorPerThread,
                                    DocFieldConsumers parent, DocFieldConsumerPerThread one, DocFieldConsumerPerThread two) {
    this.parent = parent;
    this.one = one;
    this.two = two;
    docState = docFieldProcessorPerThread.docState;
  }

  public void startDocument() throws IOException {
    one.startDocument();
    two.startDocument();
  }

  public void abort() {
    try {
      one.abort();
    } finally {
      two.abort();
    }
  }

  public DocumentsWriter.DocWriter finishDocument() throws IOException {
    final DocumentsWriter.DocWriter oneDoc = one.finishDocument();
    final DocumentsWriter.DocWriter twoDoc = two.finishDocument();
    if (oneDoc == null)
      return twoDoc;
    else if (twoDoc == null)
      return oneDoc;
    else {
      DocFieldConsumers.PerDoc both = parent.getPerDoc();
      both.docID = docState.docID;
      assert oneDoc.docID == docState.docID;
      assert twoDoc.docID == docState.docID;
      both.one = oneDoc;
      both.two = twoDoc;
      return both;
    }
  }

  public DocFieldConsumerPerField addField(FieldInfo fi) {
    return new DocFieldConsumersPerField(this, one.addField(fi), two.addField(fi));
  }
}
