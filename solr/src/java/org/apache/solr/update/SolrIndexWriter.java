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

import org.apache.lucene.index.*;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.store.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.schema.IndexSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * An IndexWriter that is configured via Solr config mechanisms.
 *
 * @since solr 0.9
 */

public class SolrIndexWriter extends IndexWriter {
  private static Logger log = LoggerFactory.getLogger(SolrIndexWriter.class);

  String name;
  private PrintStream infoStream;

  public static Directory getDirectory(String path, DirectoryFactory directoryFactory, SolrIndexConfig config) throws IOException {
    
    Directory d = directoryFactory.open(path);

    String rawLockType = (null == config) ? null : config.lockType;
    if (null == rawLockType) {
      // we default to "simple" for backwards compatibility
      log.warn("No lockType configured for " + path + " assuming 'simple'");
      rawLockType = "simple";
    }
    final String lockType = rawLockType.toLowerCase(Locale.ENGLISH).trim();

    if ("simple".equals(lockType)) {
      // multiple SimpleFSLockFactory instances should be OK
      d.setLockFactory(new SimpleFSLockFactory(path));
    } else if ("native".equals(lockType)) {
      d.setLockFactory(new NativeFSLockFactory(path));
    } else if ("single".equals(lockType)) {
      if (!(d.getLockFactory() instanceof SingleInstanceLockFactory))
        d.setLockFactory(new SingleInstanceLockFactory());
    } else if ("none".equals(lockType)) {
      // Recipe for disaster
      log.error("CONFIGURATION WARNING: locks are disabled on " + path);      
      d.setLockFactory(NoLockFactory.getNoLockFactory());
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Unrecognized lockType: " + rawLockType);
    }
    return d;
  }
  
  public SolrIndexWriter(String name, String path, DirectoryFactory dirFactory, boolean create, IndexSchema schema, SolrIndexConfig config, IndexDeletionPolicy delPolicy, CodecProvider codecProvider) throws IOException {
    super(
        getDirectory(path, dirFactory, config),
        config.toIndexWriterConfig(schema).
            setOpenMode(create ? IndexWriterConfig.OpenMode.CREATE : IndexWriterConfig.OpenMode.APPEND).
            setIndexDeletionPolicy(delPolicy).setCodecProvider(codecProvider)
    );
    log.debug("Opened Writer " + name);
    this.name = name;

    setInfoStream(config);
  }

  private void setInfoStream(SolrIndexConfig config)
      throws IOException {
    String infoStreamFile = config.infoStreamFile;
    if (infoStreamFile != null) {
      File f = new File(infoStreamFile);
      File parent = f.getParentFile();
      if (parent != null) parent.mkdirs();
      FileOutputStream fos = new FileOutputStream(f, true);
      infoStream = new TimeLoggingPrintStream(fos, true);
      setInfoStream(infoStream);
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
    try {
      super.close();
      if(infoStream != null) {
        infoStream.close();
      }
    } finally {
      isClosed = true;
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
        log.error("SolrIndexWriter was not closed prior to finalize(), indicates a bug -- POSSIBLE RESOURCE LEAK!!!");
        close();
      }
    } finally { 
      super.finalize();
    }
    
  }
  
  // Helper class for adding timestamps to infoStream logging
  class TimeLoggingPrintStream extends PrintStream {
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
