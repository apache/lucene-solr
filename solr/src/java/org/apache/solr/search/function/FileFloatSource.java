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
package org.apache.solr.search.function;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.StringHelper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexReader;
import org.apache.solr.util.VersionedFile;

import java.io.*;
import java.util.*;

/**
 * Obtains float field values from an external file.
 * @version $Id$
 */

public class FileFloatSource extends ValueSource {
  private SchemaField field;
  private final SchemaField keyField;
  private final float defVal;

  private final String dataDir;

  public FileFloatSource(SchemaField field, SchemaField keyField, float defVal, QParser parser) {
    this.field = field;
    this.keyField = keyField;
    this.defVal = defVal;
    this.dataDir = parser.getReq().getCore().getDataDir();
  }

  @Override
  public String description() {
    return "float(" + field + ')';
  }

  @Override
  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    int offset = 0;
    if (reader instanceof SolrIndexReader) {
      SolrIndexReader r = (SolrIndexReader)reader;
      while (r.getParent() != null) {
        offset += r.getBase();
        r = r.getParent();
      }
      reader = r;
    }
    final int off = offset;

    final float[] arr = getCachedFloats(reader);
    return new DocValues() {
      @Override
      public float floatVal(int doc) {
        return arr[doc + off];
      }

      @Override
      public int intVal(int doc) {
        return (int)arr[doc + off];
      }

      @Override
      public long longVal(int doc) {
        return (long)arr[doc + off];
      }

      @Override
      public double doubleVal(int doc) {
        return (double)arr[doc + off];
      }

      @Override
      public String strVal(int doc) {
        return Float.toString(arr[doc + off]);
      }

      @Override
      public String toString(int doc) {
        return description() + '=' + floatVal(doc);
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
    private final Map readerCache = new WeakHashMap();

    protected abstract Object createValue(IndexReader reader, Object key);

    public Object get(IndexReader reader, Object key) {
      Map innerCache;
      Object value;
      synchronized (readerCache) {
        innerCache = (Map) readerCache.get(reader);
        if (innerCache == null) {
          innerCache = new HashMap();
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
      SolrCore.log.error("Error opening external value source file: " +e);
      return vals;
    }

    BufferedReader r = new BufferedReader(new InputStreamReader(is));

    String idName = StringHelper.intern(ffs.keyField.getName());
    FieldType idType = ffs.keyField.getType();
    boolean sorted=true;   // assume sorted until we discover it's not


    // warning: lucene's termEnum.skipTo() is not optimized... it simply does a next()
    // because of this, simply ask the reader for a new termEnum rather than
    // trying to use skipTo()

    List<String> notFound = new ArrayList<String>();
    int notFoundCount=0;
    int otherErrors=0;

    TermDocs termDocs = null;
    Term protoTerm = new Term(idName, "");
    TermEnum termEnum = null;
    // Number of times to try termEnum.next() before resorting to skip
    int numTimesNext = 10;

    char delimiter='=';
    String termVal;
    boolean hasNext=true;
    String prevKey="";

    String lastVal="\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF";

    try {
      termDocs = reader.termDocs();
      termEnum = reader.terms(protoTerm);
      Term t = termEnum.term();
      if (t != null && t.field() == idName) { // intern'd comparison
        termVal = t.text();
      } else {
        termVal = lastVal;
      }


      for (String line; (line=r.readLine())!=null;) {
        int delimIndex = line.indexOf(delimiter);
        if (delimIndex < 0) continue;

        int endIndex = line.length();
        /* EOLs should already be removed for BufferedReader.readLine()
        for(int endIndex = line.length();endIndex>delimIndex+1; endIndex--) {
          char ch = line.charAt(endIndex-1);
          if (ch!='\n' && ch!='\r') break;
        }
        */
        String key = line.substring(0, delimIndex);
        String val = line.substring(delimIndex+1, endIndex);

        String internalKey = idType.toInternal(key);
        float fval;
        try {
          fval=Float.parseFloat(val);
        } catch (Exception e) {
          if (++otherErrors<=10) {
            SolrCore.log.error( "Error loading external value source + fileName + " + e
              + (otherErrors<10 ? "" : "\tSkipping future errors for this file.")                    
            );
          }
          continue;  // go to next line in file.. leave values as default.
        }

        if (sorted) {
          // make sure this key is greater than the previous key
          sorted = internalKey.compareTo(prevKey) >= 0;
          prevKey = internalKey;

          if (sorted) {
            int countNext = 0;
            for(;;) {
              int cmp = internalKey.compareTo(termVal);
              if (cmp == 0) {
                termDocs.seek(termEnum);
                while (termDocs.next()) {
                  vals[termDocs.doc()] = fval;
                }
                break;
              } else if (cmp < 0) {
                // term enum has already advanced past current key... we didn't find it.
                if (notFoundCount<10) {  // collect first 10 not found for logging
                  notFound.add(key);
                }
                notFoundCount++;
                break;
              } else {
                // termEnum is less than our current key, so skip ahead

                // try next() a few times to see if we hit or pass the target.
                // Lucene's termEnum.skipTo() is currently unoptimized (it just does next())
                // so the best thing is to simply ask the reader for a new termEnum(target)
                // if we really need to skip.
                if (++countNext > numTimesNext) {
                  termEnum = reader.terms(protoTerm.createTerm(internalKey));
                  t = termEnum.term();
                } else {
                  hasNext = termEnum.next();
                  t = hasNext ? termEnum.term() : null;
                }

                if (t != null && t.field() == idName) { // intern'd comparison
                  termVal = t.text();
                } else {
                  termVal = lastVal;
                }
              }
            } // end for(;;)
          }
        }

        if (!sorted) {
          termEnum = reader.terms(protoTerm.createTerm(internalKey));
          t = termEnum.term();
          if (t != null && t.field() == idName  // intern'd comparison
                  && internalKey.equals(t.text()))
          {
            termDocs.seek (termEnum);
            while (termDocs.next()) {
              vals[termDocs.doc()] = fval;
            }
          } else {
            if (notFoundCount<10) {  // collect first 10 not found for logging
              notFound.add(key);
            }
            notFoundCount++;
          }
        }
      }
    } catch (IOException e) {
      // log, use defaults
      SolrCore.log.error("Error loading external value source: " +e);
    } finally {
      // swallow exceptions on close so we don't override any
      // exceptions that happened in the loop
      if (termDocs!=null) try{termDocs.close();}catch(Exception e){}
      if (termEnum!=null) try{termEnum.close();}catch(Exception e){}
      try{r.close();}catch(Exception e){}
    }

    SolrCore.log.info("Loaded external value source " + fname
      + (notFoundCount==0 ? "" : " :"+notFoundCount+" missing keys "+notFound)
    );

    return vals;
  }


}
