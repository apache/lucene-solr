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
package org.apache.lucene.analysis.ja.util;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.lucene.analysis.ja.dict.TokenInfoDictionary;
import org.apache.lucene.util.fst.FST;

public class TokenInfoDictionaryWriter extends BinaryDictionaryWriter {
  private FST<Long> fst;

  public TokenInfoDictionaryWriter(int size) {
    super(TokenInfoDictionary.class, size);
  }
  
  public void setFST(FST<Long> fst) {
    this.fst = fst;
  }
  
  @Override
  public void write(String baseDir) throws IOException {
    super.write(baseDir);
    writeFST(getBaseFileName(baseDir) + TokenInfoDictionary.FST_FILENAME_SUFFIX);
  }
  
  protected void writeFST(String filename) throws IOException {
    Path p = Paths.get(filename);
    Files.createDirectories(p.getParent());
    fst.save(p);
  }  
}
