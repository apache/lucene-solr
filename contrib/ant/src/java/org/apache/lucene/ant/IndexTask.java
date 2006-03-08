package org.apache.lucene.ant;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TermQuery;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.DynamicConfigurator;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.FileSet;
import org.apache.tools.ant.types.EnumeratedAttribute;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.ArrayList;
import java.text.ParseException;

/**
 *  Ant task to index files with Lucene
 *
 *@author Erik Hatcher
 */
public class IndexTask extends Task {
  /**
   *  file list
   */
  private ArrayList filesets = new ArrayList();

  /**
   *  overwrite index?
   */
  private boolean overwrite = false;

  /**
   *  index path
   */
  private File indexDir;

  /**
   *  document handler classname
   */
  private String handlerClassName =
    FileExtensionDocumentHandler.class.getName();

  /**
   *  document handler instance
   */
  private DocumentHandler handler;


  /**
   *
   */
  private String analyzerClassName =
    StandardAnalyzer.class.getName();

  /**
   *  analyzer instance
   */
  private Analyzer analyzer;

  /**
   *  Lucene merge factor
   */
  private int mergeFactor = 20;

  private HandlerConfig handlerConfig;

  private boolean useCompoundIndex = true;


  /**
   *  Creates new instance
   */
  public IndexTask() {
  }


  /**
   *  Specifies the directory where the index will be stored
   */
  public void setIndex(File indexDir) {
    this.indexDir = indexDir;
  }


  /**
   *  Sets the mergeFactor attribute of the IndexTask object
   *
   *@param  mergeFactor  The new mergeFactor value
   */
  public void setMergeFactor(int mergeFactor) {
    this.mergeFactor = mergeFactor;
  }


  /**
   *  Sets the overwrite attribute of the IndexTask object
   *
   *@param  overwrite  The new overwrite value
   */
  public void setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }


  /**
   * If creating a new index and this is set to true, the
   * index will be created in compound format.
   */
  public void setUseCompoundIndex(boolean useCompoundIndex) {
    this.useCompoundIndex = useCompoundIndex;
  }

  /**
   *  Sets the documentHandler attribute of the IndexTask object
   *
   *@param  classname  The new documentHandler value
   */
  public void setDocumentHandler(String classname) {
    handlerClassName = classname;
  }

  /**
   * Sets the analyzer based on the builtin Lucene analyzer types.
   *
   * @todo Enforce analyzer and analyzerClassName to be mutually exclusive
   */
  public void setAnalyzer(AnalyzerType type) {
    analyzerClassName = type.getClassname();
  }

  public void setAnalyzerClassName(String classname) {
    analyzerClassName = classname;
  }

  /**
   *  Adds a set of files (nested fileset attribute).
   *
   *@param  set  FileSet to be added
   */
  public void addFileset(FileSet set) {
    filesets.add(set);
  }

  /**
   * Sets custom properties for a configurable document handler.
   */
  public void addConfig(HandlerConfig config) throws BuildException {
    if (handlerConfig != null) {
      throw new BuildException("Only one config element allowed");
    }

    handlerConfig = config;
  }


  /**
   *  Begins the indexing
   *
   *@exception  BuildException  If an error occurs indexing the
   *      fileset
   */
  public void execute() throws BuildException {

    // construct handler and analyzer dynamically
    try {
      Class clazz = Class.forName(handlerClassName);
      handler = (DocumentHandler) clazz.newInstance();

      clazz = Class.forName(analyzerClassName);
      analyzer = (Analyzer) clazz.newInstance();
    } catch (ClassNotFoundException cnfe) {
      throw new BuildException(cnfe);
    } catch (InstantiationException ie) {
      throw new BuildException(ie);
    } catch (IllegalAccessException iae) {
      throw new BuildException(iae);
    }

    log("Document handler = " + handler.getClass(), Project.MSG_VERBOSE);
    log("Analyzer = " + analyzer.getClass(), Project.MSG_VERBOSE);

    if (handler instanceof ConfigurableDocumentHandler) {
      ((ConfigurableDocumentHandler) handler).configure(handlerConfig.getProperties());
    }

    try {
      indexDocs();
    } catch (IOException e) {
      throw new BuildException(e);
    }
  }


  /**
   * Index the fileset.
   *
   *@exception  IOException if Lucene I/O exception
   *@todo refactor!!!!!
   */
  private void indexDocs() throws IOException {
    Date start = new Date();

    boolean create = overwrite;
    // If the index directory doesn't exist,
    // create it and force create mode
    if (indexDir.mkdirs() && !overwrite) {
      create = true;
    }

    Searcher searcher = null;
    boolean checkLastModified = false;
    if (!create) {
      try {
        searcher = new IndexSearcher(indexDir.getAbsolutePath());
        checkLastModified = true;
      } catch (IOException ioe) {
        log("IOException: " + ioe.getMessage());
        // Empty - ignore, which indicates to index all
        // documents
      }
    }

    log("checkLastModified = " + checkLastModified, Project.MSG_VERBOSE);

    IndexWriter writer =
      new IndexWriter(indexDir, analyzer, create);

    writer.setUseCompoundFile(useCompoundIndex);
    int totalFiles = 0;
    int totalIndexed = 0;
    int totalIgnored = 0;
    try {
      writer.setMergeFactor(mergeFactor);

      for (int i = 0; i < filesets.size(); i++) {
        FileSet fs = (FileSet) filesets.get(i);
        if (fs != null) {
          DirectoryScanner ds =
            fs.getDirectoryScanner(getProject());
          String[] dsfiles = ds.getIncludedFiles();
          File baseDir = ds.getBasedir();

          for (int j = 0; j < dsfiles.length; j++) {
            File file = new File(baseDir, dsfiles[j]);
            totalFiles++;

            if (!file.exists() || !file.canRead()) {
              throw new BuildException("File \"" +
                                       file.getAbsolutePath()
                                       + "\" does not exist or is not readable.");
            }

            boolean indexIt = true;

            if (checkLastModified) {
              Term pathTerm =
                new Term("path", file.getPath());
              TermQuery query =
                new TermQuery(pathTerm);
              Hits hits = searcher.search(query);

              // if document is found, compare the
              // indexed last modified time with the
              // current file
              // - don't index if up to date
              if (hits.length() > 0) {
                Document doc = hits.doc(0);
                String indexModified =
                  doc.get("modified").trim();
                if (indexModified != null) {
                  long lastModified = 0;
                  try {
                    lastModified = DateTools.stringToTime(indexModified);
                  } catch (ParseException e) {
                    // if modified time is not parsable, skip
                  }
                  if (lastModified == file.lastModified()) {
                    // TODO: remove existing document
                    indexIt = false;
                  }
                }
              }
            }

            if (indexIt) {
              try {
                log("Indexing " + file.getPath(),
                    Project.MSG_VERBOSE);
                Document doc =
                  handler.getDocument(file);

                if (doc == null) {
                  totalIgnored++;
                } else {
                  // Add the path of the file as a field named "path".  Use a Keyword field, so
                  // that the index stores the path, and so that the path is searchable
                  doc.add(new Field("path", file.getPath(), Field.Store.YES, Field.Index.UN_TOKENIZED));

                  // Add the last modified date of the file a field named "modified".  Use a
                  // Keyword field, so that it's searchable, but so that no attempt is made
                  // to tokenize the field into words.
                  doc.add(new Field("modified", DateTools.timeToString(file.lastModified(), DateTools.Resolution.MILLISECOND), Field.Store.YES, Field.Index.UN_TOKENIZED));

                  writer.addDocument(doc);
                  totalIndexed++;
                }
              } catch (DocumentHandlerException e) {
                throw new BuildException(e);
              }
            }
          }
          // for j
        }
        // if (fs != null)
      }
      // for i

      writer.optimize();
    }
      //try
    finally {
      // always make sure everything gets closed,
      // no matter how we exit.
      writer.close();
      if (searcher != null) {
        searcher.close();
      }
    }

    Date end = new Date();

    log(totalIndexed + " out of " + totalFiles + " indexed (" +
        totalIgnored + " ignored) in " + (end.getTime() - start.getTime()) +
        " milliseconds");
  }

  public static class HandlerConfig implements DynamicConfigurator {
    Properties props = new Properties();

    public void setDynamicAttribute(String attributeName, String value) throws BuildException {
      props.setProperty(attributeName, value);
    }

    public Object createDynamicElement(String elementName) throws BuildException {
      throw new BuildException("Sub elements not supported");
    }

    public Properties getProperties() {
      return props;
    }
  }

 public static class AnalyzerType extends EnumeratedAttribute {
    private static Map analyzerLookup = new HashMap();

    static {
      analyzerLookup.put("simple", SimpleAnalyzer.class.getName());
      analyzerLookup.put("standard", StandardAnalyzer.class.getName());
      analyzerLookup.put("stop", StopAnalyzer.class.getName());
      analyzerLookup.put("whitespace", WhitespaceAnalyzer.class.getName());
    }

    /**
     * @see EnumeratedAttribute#getValues
     */
    public String[] getValues() {
      Set keys = analyzerLookup.keySet();
      return (String[]) keys.toArray(new String[0]);
    }

    public String getClassname() {
      return (String) analyzerLookup.get(getValue());
    }
  }
}

