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

package org.apache.solr.update;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.text.DateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IndexWriter that is configured via Solr config mechanisms.
 *
 * @since solr 0.9
 */

public class SolrIndexWriter extends IndexWriter {
  private static Logger log = LoggerFactory.getLogger(SolrIndexWriter.class);
  // These should *only* be used for debugging or monitoring purposes
  public static final AtomicLong numOpens = new AtomicLong();
  public static final AtomicLong numCloses = new AtomicLong();

  String name;
  private DirectoryFactory directoryFactory;

  public SolrIndexWriter(String name, String path, DirectoryFactory directoryFactory, boolean create, IndexSchema schema, SolrIndexConfig config, IndexDeletionPolicy delPolicy, Codec codec, boolean forceNewDirectory) throws IOException {
    super(
        directoryFactory.get(path, config.lockType, forceNewDirectory),
        config.toIndexWriterConfig(schema).
            setOpenMode(create ? IndexWriterConfig.OpenMode.CREATE : IndexWriterConfig.OpenMode.APPEND).
            setIndexDeletionPolicy(delPolicy).setCodec(codec).setInfoStream(toInfoStream(config))
    );
    log.debug("Opened Writer " + name);
    this.name = name;

    this.directoryFactory = directoryFactory;
    numOpens.incrementAndGet();
  }
  
  private static InfoStream toInfoStream(SolrIndexConfig config) throws IOException {
    String infoStreamFile = config.infoStreamFile;
    if (infoStreamFile != null) {
      File f = new File(infoStreamFile);
      File parent = f.getParentFile();
      if (parent != null) parent.mkdirs();
      FileOutputStream fos = new FileOutputStream(f, true);
      return new PrintStreamInfoStream(new TimeLoggingPrintStream(fos, true));
    } else {
      return null;
    }
  }


  /**
   * use DocumentBuilder now...
   * private final void addField(Document doc, String name, String val) {
   * SchemaField ftype = schema.getField(name);
   * <p/>
   * // we don't check for a null val ourselves because a solr.FieldType
   * // might actually want to map it to something.  If createField()
   * // returns null, then we don't store the field.
   * <p/>
   * Field field = ftype.createField(val, boost);
   * if (field != null) doc.add(field);
   * }
   * <p/>
   * <p/>
   * public void addRecord(String[] fieldNames, String[] fieldValues) throws IOException {
   * Document doc = new Document();
   * for (int i=0; i<fieldNames.length; i++) {
   * String name = fieldNames[i];
   * String val = fieldNames[i];
   * <p/>
   * // first null is end of list.  client can reuse arrays if they want
   * // and just write a single null if there is unused space.
   * if (name==null) break;
   * <p/>
   * addField(doc,name,val);
   * }
   * addDocument(doc);
   * }
   * ****
   */
  private volatile boolean isClosed = false;

  
  @Override
  public void close() throws IOException {
    log.debug("Closing Writer " + name);
    Directory directory = getDirectory();
    final InfoStream infoStream = isClosed ? null : getConfig().getInfoStream();    
    try {
      super.close();
      if(infoStream != null) {
        infoStream.close();
      }
    } finally {
      isClosed = true;

      directoryFactory.release(directory);
     
      numCloses.incrementAndGet();
    }
  }

  @Override
  public void rollback() throws IOException {
    try {
      super.rollback();
    } finally {
      isClosed = true;
    }
  }

  @Override
  protected void finalize() throws Throwable {
    try {
      if(!isClosed){
        assert false : "SolrIndexWriter was not closed prior to finalize()";
        log.error("SolrIndexWriter was not closed prior to finalize(), indicates a bug -- POSSIBLE RESOURCE LEAK!!!");
        close();
      }
    } finally { 
      super.finalize();
    }
    
  }
  
  // Helper class for adding timestamps to infoStream logging
  static class TimeLoggingPrintStream extends PrintStream {
    private DateFormat dateFormat;
    public TimeLoggingPrintStream(OutputStream underlyingOutputStream,
        boolean autoFlush) {
      super(underlyingOutputStream, autoFlush);
      this.dateFormat = DateFormat.getDateTimeInstance();
    }

    // We might ideally want to override print(String) as well, but
    // looking through the code that writes to infoStream, it appears
    // that all the classes except CheckIndex just use println.
    @Override
    public void println(String x) {
      print(dateFormat.format(new Date()) + " ");
      super.println(x);
    }
  }
}
