package org.apache.solr.core;
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


import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;


/**
 *  Directly provide MMapDirectory instead of relying on {@link org.apache.lucene.store.FSDirectory#open}
 *
 * Can set the following parameters:
 * <ul>
 *  <li>unmap -- See {@link org.apache.lucene.store.MMapDirectory#setUseUnmap(boolean)}</li>
 *  <li>maxChunkSize -- The Max chunk size.  See {@link org.apache.lucene.store.MMapDirectory#setMaxChunkSize(int)}</li>
 * </ul>
 *
 **/
public class MMapDirectoryFactory extends DirectoryFactory {
  private transient static Logger log = LoggerFactory.getLogger(MMapDirectoryFactory.class);
  boolean unmapHack;
  private int maxChunk;

  @Override
  public Directory open(String path) throws IOException {
    MMapDirectory mapDirectory = new MMapDirectory(new File(path));
    try {
      mapDirectory.setUseUnmap(unmapHack);
    } catch (Exception e) {
      log.warn("Unmap not supported on this JVM, continuing on without setting unmap", e);
    }
    mapDirectory.setMaxChunkSize(maxChunk);
    return mapDirectory;
  }

  @Override
  public void init(NamedList args) {
    SolrParams params = SolrParams.toSolrParams( args );
    maxChunk = params.getInt("maxChunkSize", MMapDirectory.DEFAULT_MAX_BUFF);
    if (maxChunk <= 0){
      throw new IllegalArgumentException("maxChunk must be greater than 0");
    }
    unmapHack = params.getBool("unmap", true);
  }
}
