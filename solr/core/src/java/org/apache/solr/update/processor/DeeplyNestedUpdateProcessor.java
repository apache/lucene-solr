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
import java.util.Collection;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Objects;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.AddUpdateCommand;
import static org.apache.solr.update.processor.DeeplyNestedUpdateProcessorFactory.NestedFlag;

public class DeeplyNestedUpdateProcessor extends UpdateRequestProcessor {
  private EnumSet<NestedFlag> fields;
  SolrQueryRequest req;


  protected DeeplyNestedUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp, EnumSet<NestedFlag> fields, UpdateRequestProcessor next) {
    super(next);
    this.req = req;
    this.fields = fields;
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    SolrInputDocument doc = cmd.getSolrInputDocument();
    processDocChildren(doc, null);
    super.processAdd(cmd);
  }

  private void processDocChildren(SolrInputDocument doc, String fullPath) {
    for(SolrInputField field: doc.values()) {
      if(field.getFirstValue() instanceof SolrInputDocument) {
        Object val = field.getValue();
        fullPath = Objects.isNull(fullPath) ? field.getName(): String.format(Locale.ROOT,"%s.%s", fullPath, field.getName());
        if (val instanceof Collection) {
          for(Object childDoc: (Collection) val) {
            if(childDoc instanceof SolrInputDocument) {
              processChildDoc((SolrInputDocument) childDoc, doc, fullPath);
            }
          }
        } else {
          processChildDoc((SolrInputDocument) val, doc, fullPath);
        }
      }
    }
  }

  private void processChildDoc(SolrInputDocument sdoc, SolrInputDocument parent, String fullPath) {
    if(fields == NestedFlag.ALL) {
      setPathField(sdoc, fullPath);
      setParentKey(sdoc, parent);
      setLevelKey(sdoc, fullPath);
    } else {
      if(fields.contains(NestedFlag.PATH)) {
        setPathField(sdoc, fullPath);
      }
      if (fields.contains(NestedFlag.PARENT)) {
        setParentKey(sdoc, parent);
      }
      if(fields.contains(NestedFlag.LEVEL)) {
        setLevelKey(sdoc, fullPath);
      }
    }
    processDocChildren(sdoc, fullPath);
  }

  private void setLevelKey(SolrInputDocument sdoc, String fullPath) {
    sdoc.addField(IndexSchema.LEVEL_FIELD_NAME, fullPath.split("\\.").length);
  }

  private void setParentKey(SolrInputDocument sdoc, SolrInputDocument parent) {
    sdoc.addField(IndexSchema.PARENT_FIELD_NAME, parent.getFieldValue(req.getSchema().getUniqueKeyField().getName()));
  }

  private void setPathField(SolrInputDocument sdoc, String fullPath) {
    sdoc.addField(IndexSchema.PATH_FIELD_NAME, fullPath);
  }

}