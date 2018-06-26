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
import java.util.EnumSet;
import java.util.Objects;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.AddUpdateCommand;
import static org.apache.solr.update.processor.DeeplyNestedUpdateProcessorFactory.NestedFlag;

public class DeeplyNestedUpdateProcessor extends UpdateRequestProcessor {
  public static final String splitChar = ".";
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
      for(Object val: field) {
        if(val instanceof SolrInputDocument) {
          if(field.getName().contains(splitChar)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Field name: '" + field.getName()
                + "' contains: '" + splitChar + "' , which is reserved for the nested URP");
          }
          final String jointPath = Objects.isNull(fullPath) ? field.getName(): String.join(splitChar, fullPath, field.getName());
          processChildDoc((SolrInputDocument) val, doc, jointPath);
        }
      }
    }
  }

  private void processChildDoc(SolrInputDocument sdoc, SolrInputDocument parent, String fullPath) {
    if(fields.contains(NestedFlag.PATH)) {
      setPathField(sdoc, fullPath);
    }
    if (fields.contains(NestedFlag.PARENT)) {
      setParentKey(sdoc, parent);
    }
    processDocChildren(sdoc, fullPath);
  }

  private void setParentKey(SolrInputDocument sdoc, SolrInputDocument parent) {
    sdoc.addField(IndexSchema.PARENT_FIELD_NAME, parent.getFieldValue(req.getSchema().getUniqueKeyField().getName()));
  }

  private void setPathField(SolrInputDocument sdoc, String fullPath) {
    sdoc.addField(IndexSchema.PATH_FIELD_NAME, fullPath);
  }

}