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
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.QParser;

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

  private final String indexDir;

  public FileFloatSource(SchemaField field, SchemaField keyField, float defVal, QParser parser) {
    this.field = field;
    this.keyField = keyField;
    this.defVal = defVal;
    this.indexDir = parser.getReq().getCore().getIndexDir();
  }

  public String description() {
    return "float(" + field + ')';
  }

  public DocValues getValues(IndexReader reader) throws IOException {
    final float[] arr = getCachedFloats(reader);
    return new DocValues() {
      public float floatVal(int doc) {
        return arr[doc];
      }

      public int intVal(int doc) {
        return (int)arr[doc];
      }

      public long longVal(int doc) {
        return (long)arr[doc];
      }

      public double doubleVal(int doc) {
        return (double)arr[doc];
      }

      public String strVal(int doc) {
        return Float.toString(arr[doc]);
      }

      public String toString(int doc) {
        return description() + '=' + floatVal(doc);
      }
    };
  }

  public boolean equals(Object o) {
    if (o.getClass() !=  FileFloatSource.class) return false;
    FileFloatSource other = (FileFloatSource)o;
    return this.field.getName().equals(other.field.getName())
            && this.keyField.getName().equals(other.keyField.getName())
            && this.defVal == other.defVal
            && this.indexDir.equals(other.indexDir);
  }

  public int hashCode() {
    return FileFloatSource.class.hashCode() + field.getName().hashCode();
  };

  public String toString() {
    return "FileFloatSource(field="+field.getName()+",keyField="+keyField.getName()
            + ",defVal="+defVal+",indexDir="+indexDir+")";

  }

  private final float[] getCachedFloats(IndexReader reader) {
    return (float[])floatCache.get(reader, new Entry(this));
  }

  static Cache floatCache = new Cache() {
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

    public boolean equals(Object o) {
      if (!(o instanceof Entry)) return false;
      Entry other = (Entry)o;
      return ffs.equals(other.ffs);
    }

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
      is = getLatestFile(ffs.indexDir, fname);
    } catch (IOException e) {
      // log, use defaults
      SolrCore.log.severe("Error opening external value source file: " +e);
      return vals;
    }

    BufferedReader r = new BufferedReader(new InputStreamReader(is));

    String idName = ffs.keyField.getName().intern();
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
            SolrCore.log.severe( "Error loading external value source + fileName + " + e
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
      SolrCore.log.severe("Error loading external value source: " +e);
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


  // Future: refactor/pull out into VersionedFile class

  /* Open the latest version of a file... fileName if that exists, or
   * the last fileName.* after being sorted lexicographically.
   * Older versions of the file are deleted (and queued for deletion if
   * that fails).
   */
  private static InputStream getLatestFile(String dirName, String fileName) throws FileNotFoundException {
    Collection<File> oldFiles=null;
    final String prefix = fileName+'.';
    File f = new File(dirName, fileName);
    InputStream is = null;

    // there can be a race between checking for a file and opening it...
    // the user may have just put a new version in and deleted an old version.
    // try multiple times in a row.
    for (int retry=0; retry<10; retry++) {
      try {
        if (!f.exists()) {
          File dir = new File(dirName);
          String[] names = dir.list(new FilenameFilter() {
            public boolean accept(File dir, String name) {
              return name.startsWith(prefix);
            }
          });
          Arrays.sort(names);
          f = new File(dir, names[names.length-1]);
          oldFiles = new ArrayList<File>();
          for (int i=0; i<names.length-1; i++) {
            oldFiles.add(new File(dir, names[i]));
          }
        }

        is = new FileInputStream(f);
      } catch (Exception e) {
        // swallow exception for now
      }
    }

    // allow exception to be thrown from the final try.
    is = new FileInputStream(f);

    // delete old files only after we have successfuly opened the newest
    if (oldFiles != null) {
      delete(oldFiles);
    }

    return is;
  }



  private static final Set<File> deleteList = new HashSet<File>();
  private static synchronized void delete(Collection<File> files) {
    synchronized (deleteList) {
      deleteList.addAll(files);
      List<File> deleted = new ArrayList<File>();
      for (File df : deleteList) {
        try {
          df.delete();
          // deleteList.remove(df);
          deleted.add(df);
        } catch (SecurityException e) {
          if (!df.exists()) {
            deleted.add(df);
          }
        }
      }
      deleteList.removeAll(deleted);
    }
  }


}
