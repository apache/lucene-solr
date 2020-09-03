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
package org.apache.lucene.codecs.lucene87;

import java.util.zip.Deflater;

import org.apache.lucene.util.Constants;

/**
 * This class is a workaround for JDK bug
 * <a href="https://bugs.openjdk.java.net/browse/JDK-8252739">JDK-8252739</a>.
 */
final class BugfixDeflater_JDK8252739 extends Deflater {
  
  private final byte[] dictBytesScratch;

  private BugfixDeflater_JDK8252739(int level, boolean nowrap, int dictLength) {
    super(level, nowrap);
    this.dictBytesScratch = new byte[dictLength];
  }
  
  @Override
  public void setDictionary(byte[] dictBytes, int off, int len) {
    if (off > 0) {
      System.arraycopy(dictBytes, off, dictBytesScratch, 0, len);
      super.setDictionary(dictBytesScratch, 0, len);
    } else {
      super.setDictionary(dictBytes, off, len);
    }
  }

  public static Deflater createDeflaterInstance(int level, boolean nowrap, int dictLength) {
    if (dictLength < 0) {
      throw new IllegalArgumentException("dictLength must be >= 0");
    }
    if (Constants.JRE_IS_MINIMUM_JAVA11) {
      return new BugfixDeflater_JDK8252739(level, nowrap, dictLength);
    } else {
      return new Deflater(level, nowrap);
    }
  }

}
