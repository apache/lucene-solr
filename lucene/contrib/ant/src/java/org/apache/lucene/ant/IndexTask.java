package org.apache.lucene.ant;

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

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.lang.reflect.Constructor;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DynamicConfigurator;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.EnumeratedAttribute;
import org.apache.tools.ant.types.FileSet;
import org.apache.tools.ant.types.Resource;
import org.apache.tools.ant.types.ResourceCollection;
import org.apache.tools.ant.types.resources.FileResource;

/**
 *  Ant task to index files with Lucene
 *
 */
public class IndexTask extends Task {
  /**
   *  resources
   */
  protected Vector<ResourceCollection> rcs = new Vector<ResourceCollection>();

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
   * TODO: Enforce analyzer and analyzerClassName to be mutually exclusive
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
    add(set);
  }

    /**
     * Add a collection of files to copy.
     * @param res a resource collection to copy.
     * @since Ant 1.7
     */
    public void add(ResourceCollection res) {
        rcs.add(res);
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

  private static final Analyzer createAnalyzer(String className) throws Exception{
    final Class<? extends Analyzer> clazz = Class.forName(className).asSubclass(Analyzer.class);
    try {
      // first try to use a ctor with version parameter (needed for many new Analyzers that have no default one anymore
      Constructor<? extends Analyzer> cnstr = clazz.getConstructor(Version.class);
      return cnstr.newInstance(Version.LUCENE_CURRENT);
    } catch (NoSuchMethodException nsme) {
      // otherwise use default ctor
      return clazz.newInstance();
    }
  }

  /**
   *  Begins the indexing
   *
   *@exception  BuildException  If an error occurs indexing the
   *      fileset
   */
  @Override
  public void execute() throws BuildException {

    // construct handler and analyzer dynamically
    try {
      handler = Class.forName(handlerClassName).asSubclass(DocumentHandler.class).newInstance();

      analyzer = IndexTask.createAnalyzer(analyzerClassName);
    } catch (Exception e) {
      throw new BuildException(e);
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
   *TODO: refactor!!!!!
   */
  private void indexDocs() throws IOException {
    Date start = new Date();

    boolean create = overwrite;
    // If the index directory doesn't exist,
    // create it and force create mode
    if (indexDir.mkdirs() && !overwrite) {
      create = true;
    }

    FSDirectory dir = FSDirectory.open(indexDir);
    try {
      IndexSearcher searcher = null;
      boolean checkLastModified = false;
      if (!create) {
        try {
          searcher = new IndexSearcher(dir, true);
          checkLastModified = true;
        } catch (IOException ioe) {
          log("IOException: " + ioe.getMessage());
          // Empty - ignore, which indicates to index all
          // documents
        }
      }

      log("checkLastModified = " + checkLastModified, Project.MSG_VERBOSE);

      IndexWriterConfig conf = new IndexWriterConfig(
          Version.LUCENE_CURRENT, analyzer).setOpenMode(
          create ? OpenMode.CREATE : OpenMode.APPEND);
      TieredMergePolicy tmp = (TieredMergePolicy) conf.getMergePolicy();
      tmp.setUseCompoundFile(useCompoundIndex);
      tmp.setMaxMergeAtOnce(mergeFactor);
      IndexWriter writer = new IndexWriter(dir, conf);
      int totalFiles = 0;
      int totalIndexed = 0;
      int totalIgnored = 0;
      try {

        for (int i = 0; i < rcs.size(); i++) {
          ResourceCollection rc = rcs.elementAt(i);
          if (rc.isFilesystemOnly()) {
            Iterator resources = rc.iterator();
            while (resources.hasNext()) {
              Resource r = (Resource) resources.next();
              if (!r.isExists() || !(r instanceof FileResource)) {
                continue;
              }
              
              totalFiles++;

              File file = ((FileResource) r).getFile();
              
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
                ScoreDoc[] hits = searcher.search(query, null, 1).scoreDocs;

                // if document is found, compare the
                // indexed last modified time with the
                // current file
                // - don't index if up to date
                if (hits.length > 0) {
                  Document doc = searcher.doc(hits[0].doc);
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
                    doc.add(new Field("path", file.getPath(), Field.Store.YES, Field.Index.NOT_ANALYZED));

                    // Add the last modified date of the file a field named "modified".  Use a
                    // Keyword field, so that it's searchable, but so that no attempt is made
                    // to tokenize the field into words.
                    doc.add(new Field("modified", DateTools.timeToString(file.lastModified(), DateTools.Resolution.MILLISECOND), Field.Store.YES, Field.Index.NOT_ANALYZED));

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
    } finally {
      dir.close();
    }
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
    private static Map<String,String> analyzerLookup = new HashMap<String,String>();

    static {
      analyzerLookup.put("simple", SimpleAnalyzer.class.getName());
      analyzerLookup.put("standard", StandardAnalyzer.class.getName());
      analyzerLookup.put("stop", StopAnalyzer.class.getName());
      analyzerLookup.put("whitespace", WhitespaceAnalyzer.class.getName());
    }

    /**
     * @see EnumeratedAttribute#getValues
     */
    @Override
    public String[] getValues() {
      Set<String> keys = analyzerLookup.keySet();
      return keys.toArray(new String[0]);
    }

    public String getClassname() {
      return analyzerLookup.get(getValue());
    }
  }
}

