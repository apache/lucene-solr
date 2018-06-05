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

package org.apache.solr.update.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.AddUpdateCommand;

public class DeeplyNestedUpdateProcessor extends UpdateRequestProcessor {
  String[] fields;

  protected DeeplyNestedUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp, List<String> fields, UpdateRequestProcessor next) {
    super(next);
    this.fields = fields.toArray(new String[0]);
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    SolrInputDocument doc = cmd.getSolrInputDocument();
    processDocChildren(doc, null);
  }

  private void processDocChildren(SolrInputDocument doc, String fullPath) {
    for(SolrInputField field: doc.values()) {
      if(field.getFirstValue() instanceof SolrInputDocument) {
        Object val = field.getValue();
        fullPath = String.format("%s%s", Objects.toString(fullPath + ".", ""), field.getName());
        if (val instanceof Collection) {
          for(Object childDoc: (Collection) val) {
            processChildDoc((SolrInputDocument) childDoc, doc, fullPath);
          }
        } else {
          processChildDoc((SolrInputDocument) val, doc, fullPath);
        }
      }
    }
  }

  private void processChildDoc(SolrInputDocument sdoc, SolrInputDocument parent, String fullPath) {
    sdoc.addField(IndexSchema.PATH_FIELD_NAME, fullPath);
    sdoc.addField(IndexSchema.PARENT_FIELD_NAME, parent.getFieldValue("id"));
    sdoc.addField(IndexSchema.LEVEL_FIELD_NAME, fullPath.split(".").length +1);
  }

}
