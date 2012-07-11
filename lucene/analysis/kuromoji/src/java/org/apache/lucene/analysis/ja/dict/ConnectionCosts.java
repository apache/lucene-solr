package org.apache.lucene.analysis.ja.dict;

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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.IOUtils;

/**
 * n-gram connection cost data
 */
public final class ConnectionCosts {
  
  public static final String FILENAME_SUFFIX = ".dat";
  public static final String HEADER = "kuromoji_cc";
  public static final int VERSION = 1;
  
  private final short[][] costs; // array is backward IDs first since get is called using the same backward ID consecutively. maybe doesn't matter.
  
  private ConnectionCosts() throws IOException {
    IOException priorE = null;
    InputStream is = null;
    short[][] costs = null;
    try {
      is = BinaryDictionary.getClassResource(getClass(), FILENAME_SUFFIX);
      is = new BufferedInputStream(is);
      final DataInput in = new InputStreamDataInput(is);
      CodecUtil.checkHeader(in, HEADER, VERSION, VERSION);
      int forwardSize = in.readVInt();
      int backwardSize = in.readVInt();
      costs = new short[backwardSize][forwardSize];
      int accum = 0;
      for (int j = 0; j < costs.length; j++) {
        final short[] a = costs[j];
        for (int i = 0; i < a.length; i++) {
          int raw = in.readVInt();
          accum += (raw >>> 1) ^ -(raw & 1);
          a[i] = (short)accum;
        }
      }
    } catch (IOException ioe) {
      priorE = ioe;
    } finally {
      IOUtils.closeWhileHandlingException(priorE, is);
    }
    
    this.costs = costs;
  }
  
  public int get(int forwardId, int backwardId) {
    return costs[backwardId][forwardId];
  }
  
  public static ConnectionCosts getInstance() {
    return SingletonHolder.INSTANCE;
  }
  
  private static class SingletonHolder {
    static final ConnectionCosts INSTANCE;
    static {
      try {
        INSTANCE = new ConnectionCosts();
      } catch (IOException ioe) {
        throw new RuntimeException("Cannot load ConnectionCosts.", ioe);
      }
    }
   }
  
}
