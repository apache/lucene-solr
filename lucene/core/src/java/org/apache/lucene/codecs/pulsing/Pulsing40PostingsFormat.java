package org.apache.lucene.codecs.pulsing;

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

import org.apache.lucene.codecs.BlockTreeTermsWriter;
import org.apache.lucene.codecs.lucene40.Lucene40PostingsBaseFormat;

/**
 * @lucene.experimental
 */
public class Pulsing40PostingsFormat extends PulsingPostingsFormat {

  /** Inlines docFreq=1 terms, otherwise uses the normal "Lucene40" format. */
  public Pulsing40PostingsFormat() {
    this(1);
  }

  /** Inlines docFreq=<code>freqCutoff</code> terms, otherwise uses the normal "Lucene40" format. */
  public Pulsing40PostingsFormat(int freqCutoff) {
    this(freqCutoff, BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE, BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  /** Inlines docFreq=<code>freqCutoff</code> terms, otherwise uses the normal "Lucene40" format. */
  public Pulsing40PostingsFormat(int freqCutoff, int minBlockSize, int maxBlockSize) {
    super("Pulsing40", new Lucene40PostingsBaseFormat(), freqCutoff, minBlockSize, maxBlockSize);
  }
}
