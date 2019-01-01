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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.core.KeywordTokenizerFactory;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.pattern.PatternReplaceFilterFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SortableTextField;
import org.apache.solr.update.AddUpdateCommand;

/**
 * Adds fields to nested documents to support some nested search requirements.
 * It can even generate uniqueKey fields for nested docs.
 *
 * @see IndexSchema#NEST_PARENT_FIELD_NAME
 * @see IndexSchema#NEST_PATH_FIELD_NAME
 *
 * @since 7.5.0
 */
public class NestedUpdateProcessorFactory extends UpdateRequestProcessorFactory {

  /** Called by the schema if referenced. */
  public static FieldType createNestPathFieldType(IndexSchema indexSchema) {
    FieldType ft = new SortableTextField();
    try {
      ft.setIndexAnalyzer(
          CustomAnalyzer.builder(indexSchema.getResourceLoader())
              .withDefaultMatchVersion(indexSchema.getDefaultLuceneMatchVersion())
              .withTokenizer(KeywordTokenizerFactory.class)
              .addTokenFilter(PatternReplaceFilterFactory.class,
                  "pattern", NestedUpdateProcessor.NUM_SEP_CHAR + "\\d*",
                  "replace", "all")
              .build());
      // the default query analyzer is verbatim, which is fine.
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e); // impossible?
    }
    Map<String, String> args = new HashMap<>();
    args.put("stored", "false");
    args.put("omitTermFreqAndPositions", "true");
    args.put("omitNorms", "true");
    args.put("maxCharsForDocValues", "-1");
    ft.setArgs(indexSchema, args);
    return ft;
  }

  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next ) {
    boolean storeParent = shouldStoreDocParent(req.getSchema());
    boolean storePath = shouldStoreDocPath(req.getSchema());
    if(!(storeParent || storePath)) {
      return next;
    }
    return new NestedUpdateProcessor(req, shouldStoreDocParent(req.getSchema()), shouldStoreDocPath(req.getSchema()), next);
  }

  private static boolean shouldStoreDocParent(IndexSchema schema) {
    return schema.getFields().containsKey(IndexSchema.NEST_PARENT_FIELD_NAME);
  }

  private static boolean shouldStoreDocPath(IndexSchema schema) {
    return schema.getFields().containsKey(IndexSchema.NEST_PATH_FIELD_NAME);
  }

  private static class NestedUpdateProcessor extends UpdateRequestProcessor {
    private static final String PATH_SEP_CHAR = "/";
    private static final String NUM_SEP_CHAR = "#";
    private static final String SINGULAR_VALUE_CHAR = "";
    private boolean storePath;
    private boolean storeParent;
    private String uniqueKeyFieldName;


    NestedUpdateProcessor(SolrQueryRequest req, boolean storeParent, boolean storePath, UpdateRequestProcessor next) {
      super(next);
      this.storeParent = storeParent;
      this.storePath = storePath;
      this.uniqueKeyFieldName = req.getSchema().getUniqueKeyField().getName();
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      SolrInputDocument doc = cmd.getSolrInputDocument();
      processDocChildren(doc, null);
      super.processAdd(cmd);
    }

    private void processDocChildren(SolrInputDocument doc, String fullPath) {
      for(SolrInputField field: doc.values()) {
        int childNum = 0;
        boolean isSingleVal = !(field.getValue() instanceof Collection);
        for(Object val: field) {
          if(!(val instanceof SolrInputDocument)) {
            // either all collection items are child docs or none are.
            break;
          }
          final String fieldName = field.getName();

          if(fieldName.contains(PATH_SEP_CHAR)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Field name: '" + fieldName
                + "' contains: '" + PATH_SEP_CHAR + "' , which is reserved for the nested URP");
          }
          final String sChildNum = isSingleVal ? SINGULAR_VALUE_CHAR : String.valueOf(childNum);
          SolrInputDocument cDoc = (SolrInputDocument) val;
          if(!cDoc.containsKey(uniqueKeyFieldName)) {
            String parentDocId = doc.getField(uniqueKeyFieldName).getFirstValue().toString();
            cDoc.setField(uniqueKeyFieldName, generateChildUniqueId(parentDocId, fieldName, sChildNum));
          }
          final String lastKeyPath = fieldName + NUM_SEP_CHAR + sChildNum;
          // concat of all paths children.grandChild => children#1/grandChild#
          final String childDocPath = fullPath == null ? lastKeyPath : fullPath + PATH_SEP_CHAR + lastKeyPath;
          processChildDoc((SolrInputDocument) val, doc, childDocPath);
          ++childNum;
        }
      }
    }

    private void processChildDoc(SolrInputDocument sdoc, SolrInputDocument parent, String fullPath) {
      if(storePath) {
        setPathField(sdoc, fullPath);
      }
      if (storeParent) {
        setParentKey(sdoc, parent);
      }
      processDocChildren(sdoc, fullPath);
    }

    private String generateChildUniqueId(String parentId, String childKey, String childNum) {
      // combines parentId with the child's key and childNum. e.g. "10/footnote#1"
      return parentId + PATH_SEP_CHAR + childKey + NUM_SEP_CHAR + childNum;
    }

    private void setParentKey(SolrInputDocument sdoc, SolrInputDocument parent) {
      sdoc.setField(IndexSchema.NEST_PARENT_FIELD_NAME, parent.getFieldValue(uniqueKeyFieldName));
    }

    private void setPathField(SolrInputDocument sdoc, String fullPath) {
      sdoc.setField(IndexSchema.NEST_PATH_FIELD_NAME, fullPath);
    }

  }

}
