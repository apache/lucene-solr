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

import org.apache.lucene.index.IndexWriter;
import org.apache.solr.schema.IndexSchema;

import java.util.logging.Logger;
import java.io.IOException;

/**
 * An IndexWriter that is configured via Solr config mechanisms.
 *
* @version $Id$
* @since solr 0.9
*/


public class SolrIndexWriter extends IndexWriter {
  private static Logger log = Logger.getLogger(SolrIndexWriter.class.getName());

  String name;
  IndexSchema schema;

  private void init(String name, IndexSchema schema, SolrIndexConfig config) {
    log.fine("Opened Writer " + name);
    this.name = name;
    this.schema = schema;
    setSimilarity(schema.getSimilarity());
    // setUseCompoundFile(false);

    if (config != null) {
      setUseCompoundFile(config.useCompoundFile);
      if (config.maxBufferedDocs != -1) setMaxBufferedDocs(config.maxBufferedDocs);
      if (config.maxMergeDocs != -1) setMaxMergeDocs(config.maxMergeDocs);
      if (config.mergeFactor != -1)  setMergeFactor(config.mergeFactor);
      if (config.maxFieldLength != -1) setMaxFieldLength(config.maxFieldLength);
      //if (config.commitLockTimeout != -1) setWriteLockTimeout(config.commitLockTimeout);
    }

  }

  public SolrIndexWriter(String name, String path, boolean create, IndexSchema schema) throws IOException {
    super(path, schema.getAnalyzer(), create);
    init(name, schema, null);
  }

  public SolrIndexWriter(String name, String path, boolean create, IndexSchema schema, SolrIndexConfig config) throws IOException {
    super(path, schema.getAnalyzer(), create);
    init(name, schema,config);
  }

  /*** use DocumentBuilder now...
  private final void addField(Document doc, String name, String val) {
      SchemaField ftype = schema.getField(name);

      // we don't check for a null val ourselves because a solr.FieldType
      // might actually want to map it to something.  If createField()
      // returns null, then we don't store the field.

      Field field = ftype.createField(val, boost);
      if (field != null) doc.add(field);
  }


  public void addRecord(String[] fieldNames, String[] fieldValues) throws IOException {
    Document doc = new Document();
    for (int i=0; i<fieldNames.length; i++) {
      String name = fieldNames[i];
      String val = fieldNames[i];

      // first null is end of list.  client can reuse arrays if they want
      // and just write a single null if there is unused space.
      if (name==null) break;

      addField(doc,name,val);
    }
    addDocument(doc);
  }
  ******/

  public void close() throws IOException {
    log.fine("Closing Writer " + name);
    super.close();
  }

  @Override
  protected void finalize() {
    try {super.close();} catch (IOException e) {}
  }

}
