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
package org.apache.solr.core;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory; // javadocs
import org.apache.lucene.store.MMapDirectory;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Directly provide MMapDirectory instead of relying on {@link org.apache.lucene.store.FSDirectory#open}.
 * <p>
 * Can set the following parameters:
 * <ul>
 *  <li>unmap -- See {@link MMapDirectory#setUseUnmap(boolean)}</li>
 *  <li>preload -- See {@link MMapDirectory#setPreload(boolean)}</li>
 *  <li>maxChunkSize -- The Max chunk size.  See {@link MMapDirectory#MMapDirectory(Path, LockFactory, int)}</li>
 * </ul>
 *
 **/
public class MMapDirectoryFactory extends StandardDirectoryFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  boolean unmapHack;
  boolean preload;
  private int maxChunk;

  @Override
  @SuppressWarnings({"rawtypes"})
  public void init(NamedList args) {
    super.init(args);
    SolrParams params = args.toSolrParams();
    maxChunk = params.getInt("maxChunkSize", MMapDirectory.DEFAULT_MAX_CHUNK_SIZE);
    if (maxChunk <= 0){
      throw new IllegalArgumentException("maxChunk must be greater than 0");
    }
    unmapHack = params.getBool("unmap", true);
    preload = params.getBool("preload", false); //default turn-off
  }

  @Override
  protected Directory create(String path, LockFactory lockFactory, DirContext dirContext) throws IOException {
    // we pass NoLockFactory, because the real lock factory is set later by injectLockFactory:
    MMapDirectory mapDirectory = new MMapDirectory(new File(path).toPath(), lockFactory, maxChunk);
    try {
      mapDirectory.setUseUnmap(unmapHack);
    } catch (IllegalArgumentException e) {
      log.warn("Unmap not supported on this JVM, continuing on without setting unmap", e);
    }
    mapDirectory.setPreload(preload);
    return mapDirectory;
  }
  
  @Override
  public boolean isAbsolute(String path) {
    return new File(path).isAbsolute();
  }
}
