package org.apache.lucene.util;

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

import org.apache.lucene.index.codecs.CodecProvider;

/**
 * Base test class for Lucene test classes that to test across
 * all core codecs.
 */
public abstract class MultiCodecTestCase extends LuceneTestCase {

  @Override
  public void runBare() throws Throwable {
    final String savedDefaultCodec = CodecProvider.getDefaultCodec();
    try {
      for(String codec : CodecProvider.CORE_CODECS) {
        CodecProvider.setDefaultCodec(codec);
        try {
          super.runBare();
        } catch (Throwable e) {
          System.out.println("Test failure of '" + getName()
                             + "' occurred with \"" + codec + "\" codec");
          throw e;
        }
      }
    } finally {
      CodecProvider.setDefaultCodec(savedDefaultCodec);
    }
  }
}
