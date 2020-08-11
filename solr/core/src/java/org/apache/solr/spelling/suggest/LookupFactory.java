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
package org.apache.solr.spelling.suggest;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.store.FSDirectory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.spelling.suggest.jaspell.JaspellLookupFactory;

/**
 * Suggester factory for creating {@link Lookup} instances.
 */
public abstract class LookupFactory {
  
  /** Default lookup implementation to use for SolrSuggester */
  public static String DEFAULT_FILE_BASED_DICT = JaspellLookupFactory.class.getName();
  
  /**
   * Create a Lookup using config options in <code>params</code> and 
   * current <code>core</code>
   */
  public abstract Lookup create(@SuppressWarnings({"rawtypes"})NamedList params, SolrCore core);
  
  /** 
   * <p>Returns the filename in which the in-memory data structure is stored </p>
   * <b>NOTE:</b> not all {@link Lookup} implementations store in-memory data structures
   * */
  public abstract String storeFileName();

  /** Non-null if this sugggester created a temp dir, needed only during build */
  private static FSDirectory tmpBuildDir;

  protected static synchronized FSDirectory getTempDir() {
    if (tmpBuildDir == null) {
      // Lazy init
      String tempDirPath = System.getProperty("java.io.tmpdir");
      if (tempDirPath == null)  {
        throw new RuntimeException("Java has no temporary folder property (java.io.tmpdir)?");
      }
      try {
        tmpBuildDir = FSDirectory.open(Paths.get(tempDirPath));
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    return tmpBuildDir;
  }
}
