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
import java.util.Objects;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.AddUpdateCommand;

public class NestedUpdateProcessor extends UpdateRequestProcessor {
  private static final String PATH_SEP_CHAR = "/";
  private boolean storePath;
  private boolean storeParent;
  private String uniqueKeyFieldName;


  protected NestedUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp, boolean storeParent, boolean storePath, UpdateRequestProcessor next) {
    super(next);
    this.storeParent = storeParent;
    this.storePath = storePath;
    this.uniqueKeyFieldName = req.getSchema().getUniqueKeyField().getName();
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    SolrInputDocument doc = cmd.getSolrInputDocument();
    String rootId = doc.getField(uniqueKeyFieldName).getFirstValue().toString();
    processDocChildren(doc, rootId, null);
    super.processAdd(cmd);
  }

  private void processDocChildren(SolrInputDocument doc, String rootId, String fullPath) {
    int childNum = 0;
    for(SolrInputField field: doc.values()) {
      for(Object val: field) {
        if(val instanceof SolrInputDocument) {
          if(field.getName().contains(PATH_SEP_CHAR)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Field name: '" + field.getName()
                + "' contains: '" + PATH_SEP_CHAR + "' , which is reserved for the nested URP");
          }
          final String jointPath = Objects.isNull(fullPath) ? field.getName(): String.join(PATH_SEP_CHAR, fullPath, field.getName());
          SolrInputDocument cDoc = (SolrInputDocument) val;
          if(!cDoc.containsKey(uniqueKeyFieldName)) {
            cDoc.setField(uniqueKeyFieldName, generateChildUniqueId(rootId, jointPath, childNum));
          }
          processChildDoc((SolrInputDocument) val, doc, rootId, jointPath);
        }
        ++childNum;
      }
    }
  }

  private void processChildDoc(SolrInputDocument sdoc, SolrInputDocument parent, String rootId, String fullPath) {
    if(storePath) {
      setPathField(sdoc, fullPath);
    }
    if (storeParent) {
      setParentKey(sdoc, parent);
    }
    processDocChildren(sdoc, rootId, fullPath);
  }

  private String generateChildUniqueId(String rootId, String childPath, int childNum) {
    return String.join(PATH_SEP_CHAR, rootId, childPath, Integer.toString(childNum));
  }

  private void setParentKey(SolrInputDocument sdoc, SolrInputDocument parent) {
    sdoc.setField(IndexSchema.PARENT_FIELD_NAME, parent.getFieldValue(uniqueKeyFieldName));
  }

  private void setPathField(SolrInputDocument sdoc, String fullPath) {
    sdoc.setField(IndexSchema.PATH_FIELD_NAME, fullPath);
  }

}