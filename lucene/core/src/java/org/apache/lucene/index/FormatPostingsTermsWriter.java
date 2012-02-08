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

import java.io.Closeable;
import java.io.IOException;

final class FormatPostingsTermsWriter extends FormatPostingsTermsConsumer implements Closeable {

  final FormatPostingsFieldsWriter parent;
  final FormatPostingsDocsWriter docsWriter;
  final TermInfosWriter termsOut;
  FieldInfo fieldInfo;

  FormatPostingsTermsWriter(SegmentWriteState state, FormatPostingsFieldsWriter parent) throws IOException {
    this.parent = parent;
    termsOut = parent.termsOut;
    docsWriter = new FormatPostingsDocsWriter(state, this);
  }

  void setField(FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    docsWriter.setField(fieldInfo);
  }

  char[] currentTerm;
  int currentTermStart;

  long freqStart;
  long proxStart;

  /** Adds a new term in this field */
  @Override
  FormatPostingsDocsConsumer addTerm(char[] text, int start) {
    currentTerm = text;
    currentTermStart = start;

    // TODO: this is abstraction violation -- ideally this
    // terms writer is not so "invasive", looking for file
    // pointers in its child consumers.
    freqStart = docsWriter.out.getFilePointer();
    if (docsWriter.posWriter.out != null)
      proxStart = docsWriter.posWriter.out.getFilePointer();

    parent.skipListWriter.resetSkip();

    return docsWriter;
  }

  /** Called when we are done adding terms to this field */
  @Override
  void finish() {
  }

  public void close() throws IOException {
    docsWriter.close();
  }
}
