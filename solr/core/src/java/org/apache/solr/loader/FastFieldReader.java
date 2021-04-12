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

package org.apache.solr.loader;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.solr.common.util.DataEntry;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

/**An interface that field types should implement that can optimally read data from a stream of bytes
 * and write to lucene index
 *
 */
public interface FastFieldReader {

  void addFields(Ctx ctx);
  class Ctx {
    final PackedBytes packedBytes = new PackedBytes();
    SchemaField field;
    DataEntry data;
    Document document;
    IndexWriter iw;
    IndexSchema schema;


    public SchemaField getSchemaField(){return field;}
    public DataEntry getDataEntry(){return data;}
    public Document getDoc(){return document;}
  }
}
