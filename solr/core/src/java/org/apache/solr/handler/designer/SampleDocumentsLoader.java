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

package org.apache.solr.handler.designer;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.handler.designer.SampleDocuments;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

public interface SampleDocumentsLoader extends NamedListInitializedPlugin {
  SampleDocuments parseDocsFromStream(SolrParams params, ContentStream stream, int maxDocsToLoad) throws IOException;

  /**
   * Ensure every sample document has a unique ID, but only applies if the unique key field is a string.
   */
  default boolean ensureUniqueKey(final SchemaField idField, List<SolrInputDocument> docs) {
    boolean updatedDocs = false;
    // if the unique key field is a string, we can supply a UUID if needed, otherwise must come from the user.
    if (StrField.class.equals(idField.getType().getClass())) {
      String idFieldName = idField.getName();
      for (SolrInputDocument d : docs) {
        if (d.getFieldValue(idFieldName) == null) {
          d.setField(idFieldName, UUID.randomUUID().toString().toLowerCase(Locale.ROOT));
          updatedDocs = true;
        }
      }
    }
    return updatedDocs;
  }
}
