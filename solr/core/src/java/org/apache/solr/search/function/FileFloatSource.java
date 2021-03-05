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
package org.apache.solr.search.function;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.VersionedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Obtains float field values from an external file.
 *
 * @see org.apache.solr.schema.ExternalFileField
 * @see org.apache.solr.schema.ExternalFileFieldReloader
 */

public class FileFloatSource extends ValueSource {

  private SchemaField field;
  private final SchemaField keyField;
  private final float defVal;
  private final String dataDir;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Creates a new FileFloatSource
   * @param field the source's SchemaField
   * @param keyField the field to use as a key
   * @param defVal the default value to use if a field has no entry in the external file
   * @param datadir the directory in which to look for the external file
   */
  public FileFloatSource(SchemaField field, SchemaField keyField, float defVal, String datadir) {
    this.field = field;
    this.keyField = keyField;
    this.defVal = defVal;
    this.dataDir = datadir;
  }

  @Override
  public String description() {
    return "float(" + field + ')';
  }

  @Override
  @SuppressWarnings({"rawtypes"})

  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final int off = readerContext.docBase;
    IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(readerContext);

    final float[] arr = getCachedFloats(topLevelContext.reader());
    return new FloatDocValues(this) {
      @Override
      public float floatVal(int doc) {
        return arr[doc + off];
      }

      @Override
      public Object objectVal(int doc) {
        return floatVal(doc);   // TODO: keep track of missing values
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (o.getClass() !=  FileFloatSource.class) return false;
    FileFloatSource other = (FileFloatSource)o;
    return this.field.getName().equals(other.field.getName())
            && this.keyField.getName().equals(other.keyField.getName())
            && this.defVal == other.defVal
            && this.dataDir.equals(other.dataDir);
  }

  @Override
  public int hashCode() {
    return FileFloatSource.class.hashCode() + field.getName().hashCode();
  };

  @Override
  public String toString() {
    return "FileFloatSource(field="+field.getName()+",keyField="+keyField.getName()
            + ",defVal="+defVal+",dataDir="+dataDir+")";

  }

  /**
   * Remove all cached entries.  Values are lazily loaded next time getValues() is
   * called.
   */
  public static void resetCache(){
    floatCache.resetCache();
  }

  /**
   * Refresh the cache for an IndexReader.  The new values are loaded in the background
   * and then swapped in, so queries against the cache should not block while the reload
   * is happening.
   * @param reader the IndexReader whose cache needs refreshing
   */
  public void refreshCache(IndexReader reader) {
    if (log.isInfoEnabled()) {
      log.info("Refreshing FileFloatSource cache for field {}", this.field.getName());
    }
    floatCache.refresh(reader, new Entry(this));
    if (log.isInfoEnabled()) {
      log.info("FileFloatSource cache for field {} reloaded", this.field.getName());
    }
  }

  private final float[] getCachedFloats(IndexReader reader) {
    return (float[])floatCache.get(reader, new Entry(this));
  }

  static Cache floatCache = new Cache() {
    @Override
    protected Object createValue(IndexReader reader, Object key) {
      return getFloats(((Entry)key).ffs, reader);
    }
  };

  /** Internal cache. (from lucene FieldCache) */
  abstract static class Cache {
    @SuppressWarnings({"rawtypes"})
    private final Map readerCache = new WeakHashMap();

    protected abstract Object createValue(IndexReader reader, Object key);

    @SuppressWarnings({"unchecked"})
    public void refresh(IndexReader reader, Object key) {
      Object refreshedValues = createValue(reader, key);
      synchronized (readerCache) {
        @SuppressWarnings({"rawtypes"})
        Map innerCache = (Map) readerCache.get(reader);
        if (innerCache == null) {
          innerCache = new HashMap<>();
          readerCache.put(reader, innerCache);
        }
        innerCache.put(key, refreshedValues);
      }
    }

    @SuppressWarnings({"unchecked"})
    public Object get(IndexReader reader, Object key) {
      @SuppressWarnings({"rawtypes"})
      Map innerCache;
      Object value;
      synchronized (readerCache) {
        innerCache = (Map) readerCache.get(reader);
        if (innerCache == null) {
          innerCache = new HashMap<>();
          readerCache.put(reader, innerCache);
          value = null;
        } else {
          value = innerCache.get(key);
        }
        if (value == null) {
          value = new CreationPlaceholder();
          innerCache.put(key, value);
        }
      }
      if (value instanceof CreationPlaceholder) {
        synchronized (value) {
          CreationPlaceholder progress = (CreationPlaceholder) value;
          if (progress.value == null) {
            progress.value = createValue(reader, key);
            synchronized (readerCache) {
              innerCache.put(key, progress.value);
              onlyForTesting = progress.value;
            }
          }
          return progress.value;
        }
      }

      return value;
    }
    
    public void resetCache(){
      synchronized(readerCache){
        // Map.clear() is optional and can throw UnsupportedOperationException,
        // but readerCache is WeakHashMap and it supports clear().
        readerCache.clear();
      }
    }
  }

  static Object onlyForTesting; // set to the last value

  static final class CreationPlaceholder {
    Object value;
  }

    /** Expert: Every composite-key in the internal cache is of this type. */
  private static class Entry {
    final FileFloatSource ffs;
    public Entry(FileFloatSource ffs) {
      this.ffs = ffs;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Entry)) return false;
      Entry other = (Entry)o;
      return ffs.equals(other.ffs);
    }

    @Override
    public int hashCode() {
      return ffs.hashCode();
    }
  }



  private static float[] getFloats(FileFloatSource ffs, IndexReader reader) {
    float[] vals = new float[reader.maxDoc()];
    if (ffs.defVal != 0) {
      Arrays.fill(vals, ffs.defVal);
    }
    InputStream is;
    String fname = "external_" + ffs.field.getName();
    try {
      is = VersionedFile.getLatestFile(ffs.dataDir, fname);
    } catch (IOException e) {
      // log, use defaults
      log.error("Error opening external value source file: ", e);
      return vals;
    }

    BufferedReader r = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));

    String idName = ffs.keyField.getName();
    FieldType idType = ffs.keyField.getType();

    // warning: lucene's termEnum.skipTo() is not optimized... it simply does a next()
    // because of this, simply ask the reader for a new termEnum rather than
    // trying to use skipTo()

    List<String> notFound = new ArrayList<>();
    int notFoundCount=0;
    int otherErrors=0;

    char delimiter='=';

    BytesRefBuilder internalKey = new BytesRefBuilder();

    try {
      TermsEnum termsEnum = MultiTerms.getTerms(reader, idName).iterator();
      PostingsEnum postingsEnum = null;

      // removing deleted docs shouldn't matter
      // final Bits liveDocs = MultiLeafReader.getLiveDocs(reader);

      for (String line; (line=r.readLine())!=null;) {
        int delimIndex = line.lastIndexOf(delimiter);
        if (delimIndex < 0) continue;

        int endIndex = line.length();
        String key = line.substring(0, delimIndex);
        String val = line.substring(delimIndex+1, endIndex);

        float fval;
        try {
          idType.readableToIndexed(key, internalKey);
          fval=Float.parseFloat(val);
        } catch (Exception e) {
          if (++otherErrors<=10) {
            log.error("Error loading external value source + fileName + {}{}", e
                , (otherErrors < 10 ? "" : "\tSkipping future errors for this file."));
          }
          continue;  // go to next line in file.. leave values as default.
        }

        if (!termsEnum.seekExact(internalKey.get())) {
          if (notFoundCount<10) {  // collect first 10 not found for logging
            notFound.add(key);
          }
          notFoundCount++;
          continue;
        }

        postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
        int doc;
        while ((doc = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          vals[doc] = fval;
        }
      }

    } catch (IOException e) {
      // log, use defaults
      log.error("Error loading external value source: ", e);
    } finally {
      // swallow exceptions on close so we don't override any
      // exceptions that happened in the loop
      try{r.close();}catch(Exception e){}
    }
    if (log.isInfoEnabled()) {
      String tmp = (notFoundCount == 0 ? "" : " :" + notFoundCount + " missing keys " + notFound);
      log.info("Loaded external value source {}{}", fname, tmp);
    }
    return vals;
  }

  public static class ReloadCacheRequestHandler extends RequestHandlerBase {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
        throws Exception {
      FileFloatSource.resetCache();
      log.debug("readerCache has been reset.");

      UpdateRequestProcessor processor = req.getCore().getUpdateProcessingChain(null).createProcessor(req, rsp);
      try {
        RequestHandlerUtils.handleCommit(req, processor, req.getParams(), true);
      } finally {
        try {
          processor.finish();
        } finally {
          processor.close();
        }
      }
    }

    @Override
    public String getDescription() {
      return "Reload readerCache request handler";
    }
  }
}
