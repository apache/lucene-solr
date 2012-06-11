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
import java.io.InputStream;
import java.io.IOException;

import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;

/**
 * Binary dictionary implementation for a known-word dictionary model:
 * Words are encoded into an FST mapping to a list of wordIDs.
 */
public final class TokenInfoDictionary extends BinaryDictionary {

  public static final String FST_FILENAME_SUFFIX = "$fst.dat";

  private final TokenInfoFST fst;
  
  private TokenInfoDictionary() throws IOException {
    super();
    IOException priorE = null;
    InputStream is = null;
    FST<Long> fst = null;
    try {
      is = getResource(FST_FILENAME_SUFFIX);
      is = new BufferedInputStream(is);
      fst = new FST<Long>(new InputStreamDataInput(is), PositiveIntOutputs.getSingleton(true));
    } catch (IOException ioe) {
      priorE = ioe;
    } finally {
      IOUtils.closeWhileHandlingException(priorE, is);
    }
    // TODO: some way to configure?
    this.fst = new TokenInfoFST(fst, true);
  }
  
  public TokenInfoFST getFST() {
    return fst;
  }
   
  public static TokenInfoDictionary getInstance() {
    return SingletonHolder.INSTANCE;
  }
  
  private static class SingletonHolder {
    static final TokenInfoDictionary INSTANCE;
    static {
      try {
        INSTANCE = new TokenInfoDictionary();
      } catch (IOException ioe) {
        throw new RuntimeException("Cannot load TokenInfoDictionary.", ioe);
      }
    }
   }
  
}
