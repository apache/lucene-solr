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

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * @lucene.experimental
 */

abstract class FormatPostingsTermsConsumer {

  /** Adds a new term in this field; term ends with U+FFFF
   *  char */
  abstract FormatPostingsDocsConsumer addTerm(char[] text, int start) throws IOException;

  char[] termBuffer;
  FormatPostingsDocsConsumer addTerm(String text) throws IOException {
    final int len = text.length();
    if (termBuffer == null || termBuffer.length < 1+len)
      termBuffer = new char[ArrayUtil.oversize(1+len, RamUsageEstimator.NUM_BYTES_CHAR)];
    text.getChars(0, len, termBuffer, 0);
    termBuffer[len] = 0xffff;
    return addTerm(termBuffer, 0);
  }

  /** Called when we are done adding terms to this field */
  abstract void finish() throws IOException;
}
