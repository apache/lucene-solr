package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.StoredFieldVisitor.Status;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.TermVectorParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.SolrCoreAware;

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


/**
 * Return term vectors for the documents in a query result set.
 * <p/>
 * Info available:
 * term, frequency, position, offset, IDF.
 * <p/>
 * <b>Note</b> Returning IDF can be expensive.
 * 
 * <pre class="prettyprint">
 * &lt;searchComponent name="tvComponent" class="solr.TermVectorComponent"/&gt;
 * 
 * &lt;requestHandler name="/terms" class="solr.SearchHandler"&gt;
 *   &lt;lst name="defaults"&gt;
 *     &lt;bool name="tv"&gt;true&lt;/bool&gt;
 *   &lt;/lst&gt;
 *   &lt;arr name="last-component"&gt;
 *     &lt;str&gt;tvComponent&lt;/str&gt;
 *   &lt;/arr&gt;
 * &lt;/requestHandler&gt;</pre>
 *
 *
 */
public class TermVectorComponent extends SearchComponent implements SolrCoreAware {


  public static final String COMPONENT_NAME = "tv";

  protected NamedList initParams;
  public static final String TERM_VECTORS = "termVectors";


  @Override
  public void process(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false)) {
      return;
    }

    NamedList<Object> termVectors = new NamedList<Object>();
    rb.rsp.add(TERM_VECTORS, termVectors);
    FieldOptions allFields = new FieldOptions();
    //figure out what options we have, and try to get the appropriate vector
    allFields.termFreq = params.getBool(TermVectorParams.TF, false);
    allFields.positions = params.getBool(TermVectorParams.POSITIONS, false);
    allFields.offsets = params.getBool(TermVectorParams.OFFSETS, false);
    allFields.docFreq = params.getBool(TermVectorParams.DF, false);
    allFields.tfIdf = params.getBool(TermVectorParams.TF_IDF, false);
    //boolean cacheIdf = params.getBool(TermVectorParams.IDF, false);
    //short cut to all values.
    if (params.getBool(TermVectorParams.ALL, false)) {
      allFields.termFreq = true;
      allFields.positions = true;
      allFields.offsets = true;
      allFields.docFreq = true;
      allFields.tfIdf = true;
    }

    String fldLst = params.get(TermVectorParams.FIELDS);
    if (fldLst == null) {
      fldLst = params.get(CommonParams.FL);
    }

    //use this to validate our fields
    IndexSchema schema = rb.req.getSchema();
    //Build up our per field mapping
    Map<String, FieldOptions> fieldOptions = new HashMap<String, FieldOptions>();
    NamedList<List<String>> warnings = new NamedList<List<String>>();
    List<String>  noTV = new ArrayList<String>();
    List<String>  noPos = new ArrayList<String>();
    List<String>  noOff = new ArrayList<String>();

    //we have specific fields to retrieve
    if (fldLst != null) {
      String [] fields = SolrPluginUtils.split(fldLst);
      for (String field : fields) {
        SchemaField sf = schema.getFieldOrNull(field);
        if (sf != null) {
          if (sf.storeTermVector()) {
            FieldOptions option = fieldOptions.get(field);
            if (option == null) {
              option = new FieldOptions();
              option.fieldName = field;
              fieldOptions.put(field, option);
            }
            //get the per field mappings
            option.termFreq = params.getFieldBool(field, TermVectorParams.TF, allFields.termFreq);
            option.docFreq = params.getFieldBool(field, TermVectorParams.DF, allFields.docFreq);
            option.tfIdf = params.getFieldBool(field, TermVectorParams.TF_IDF, allFields.tfIdf);
            //Validate these are even an option
            option.positions = params.getFieldBool(field, TermVectorParams.POSITIONS, allFields.positions);
            if (option.positions && !sf.storeTermPositions()){
              noPos.add(field);
            }
            option.offsets = params.getFieldBool(field, TermVectorParams.OFFSETS, allFields.offsets);
            if (option.offsets && !sf.storeTermOffsets()){
              noOff.add(field);
            }
          } else {//field doesn't have term vectors
            noTV.add(field);
          }
        } else {
          //field doesn't exist
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "undefined field: " + field);
        }
      }
    } //else, deal with all fields
    boolean hasWarnings = false;
    if (!noTV.isEmpty()) {
      warnings.add("noTermVectors", noTV);
      hasWarnings = true;
    }
    if (!noPos.isEmpty()) {
      warnings.add("noPositions", noPos);
      hasWarnings = true;
    }
    if (!noOff.isEmpty()) {
      warnings.add("noOffsets", noOff);
      hasWarnings = true;
    }
    if (hasWarnings) {
      termVectors.add("warnings", warnings);
    }

    DocListAndSet listAndSet = rb.getResults();
    List<Integer> docIds = getInts(params.getParams(TermVectorParams.DOC_IDS));
    Iterator<Integer> iter;
    if (docIds != null && !docIds.isEmpty()) {
      iter = docIds.iterator();
    } else {
      DocList list = listAndSet.docList;
      iter = list.iterator();
    }
    SolrIndexSearcher searcher = rb.req.getSearcher();

    IndexReader reader = searcher.getIndexReader();
    //the TVMapper is a TermVectorMapper which can be used to optimize loading of Term Vectors
    SchemaField keyField = schema.getUniqueKeyField();
    String uniqFieldName = null;
    if (keyField != null) {
      uniqFieldName = keyField.getName();
    }
    //Only load the id field to get the uniqueKey of that
    //field

    final String finalUniqFieldName = uniqFieldName;

    final List<String> uniqValues = new ArrayList<String>();
    
    // TODO: is this required to be single-valued? if so, we should STOP
    // once we find it...
    final StoredFieldVisitor getUniqValue = new StoredFieldVisitor() {
      @Override 
      public void stringField(FieldInfo fieldInfo, String value) throws IOException {
        uniqValues.add(value);
      }

      @Override 
      public void intField(FieldInfo fieldInfo, int value) throws IOException {
        uniqValues.add(Integer.toString(value));
      }

      @Override 
      public void longField(FieldInfo fieldInfo, long value) throws IOException {
        uniqValues.add(Long.toString(value));
      }

      @Override
      public Status needsField(FieldInfo fieldInfo) throws IOException {
        return (fieldInfo.name.equals(finalUniqFieldName)) ? Status.YES : Status.NO;
      }
    };

    TermsEnum termsEnum = null;

    while (iter.hasNext()) {
      Integer docId = iter.next();
      NamedList<Object> docNL = new NamedList<Object>();
      termVectors.add("doc-" + docId, docNL);

      if (keyField != null) {
        reader.document(docId, getUniqValue);
        String uniqVal = null;
        if (uniqValues.size() != 0) {
          uniqVal = uniqValues.get(0);
          uniqValues.clear();
          docNL.add("uniqueKey", uniqVal);
          termVectors.add("uniqueKeyFieldName", uniqFieldName);
        }
      }
      if (!fieldOptions.isEmpty()) {
        for (Map.Entry<String, FieldOptions> entry : fieldOptions.entrySet()) {
          final String field = entry.getKey();
          final Terms vector = reader.getTermVector(docId, field);
          if (vector != null) {
            termsEnum = vector.iterator(termsEnum);
            mapOneVector(docNL, entry.getValue(), reader, docId, vector.iterator(termsEnum), field);
          }
        }
      } else {
        // extract all fields
        final Fields vectors = reader.getTermVectors(docId);
        final FieldsEnum fieldsEnum = vectors.iterator();
        String field;
        while((field = fieldsEnum.next()) != null) {
          Terms terms = fieldsEnum.terms();
          if (terms != null) {
            termsEnum = terms.iterator(termsEnum);
            mapOneVector(docNL, allFields, reader, docId, termsEnum, field);
          }
        }
      }
    }
  }

  private void mapOneVector(NamedList<Object> docNL, FieldOptions fieldOptions, IndexReader reader, int docID, TermsEnum termsEnum, String field) throws IOException {
    NamedList<Object> fieldNL = new NamedList<Object>();
    docNL.add(field, fieldNL);

    BytesRef text;
    DocsAndPositionsEnum dpEnum = null;
    while((text = termsEnum.next()) != null) {
      String term = text.utf8ToString();
      NamedList<Object> termInfo = new NamedList<Object>();
      fieldNL.add(term, termInfo);
      final int freq = (int) termsEnum.totalTermFreq();
      if (fieldOptions.termFreq == true) {
        termInfo.add("tf", freq);
      }

      dpEnum = termsEnum.docsAndPositions(null, dpEnum, fieldOptions.offsets);
      boolean useOffsets = fieldOptions.offsets;
      if (dpEnum == null) {
        useOffsets = false;
        dpEnum = termsEnum.docsAndPositions(null, dpEnum, false);
      }

      boolean usePositions = false;
      if (dpEnum != null) {
        dpEnum.nextDoc();
        usePositions = fieldOptions.positions;
      }

      NamedList<Number> theOffsets = null;
      if (useOffsets) {
        theOffsets = new NamedList<Number>();
        termInfo.add("offsets", theOffsets);
      }

      NamedList<Integer> positionsNL = null;

      if (usePositions || theOffsets != null) {
        for (int i = 0; i < freq; i++) {
          final int pos = dpEnum.nextPosition();
          if (usePositions && pos >= 0) {
            if (positionsNL == null) {
              positionsNL = new NamedList<Integer>();
              termInfo.add("positions", positionsNL);
            }
            positionsNL.add("position", pos);
          }

          if (theOffsets != null) {
            theOffsets.add("start", dpEnum.startOffset());
            theOffsets.add("end", dpEnum.endOffset());
          }
        }
      }

      if (fieldOptions.docFreq) {
        termInfo.add("df", getDocFreq(reader, field, text));
      }

      if (fieldOptions.tfIdf) {
        double tfIdfVal = ((double) freq) / getDocFreq(reader, field, text);
        termInfo.add("tf-idf", tfIdfVal);
      }
    }
  }

  private List<Integer> getInts(String[] vals) {
    List<Integer> result = null;
    if (vals != null && vals.length > 0) {
      result = new ArrayList<Integer>(vals.length);
      for (int i = 0; i < vals.length; i++) {
        try {
          result.add(new Integer(vals[i]));
        } catch (NumberFormatException e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e.getMessage(), e);
        }
      }
    }
    return result;
  }

  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    int result = ResponseBuilder.STAGE_DONE;
    if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      //Go ask each shard for it's vectors
      // for each shard, collect the documents for that shard.
      HashMap<String, Collection<ShardDoc>> shardMap = new HashMap<String, Collection<ShardDoc>>();
      for (ShardDoc sdoc : rb.resultIds.values()) {
        Collection<ShardDoc> shardDocs = shardMap.get(sdoc.shard);
        if (shardDocs == null) {
          shardDocs = new ArrayList<ShardDoc>();
          shardMap.put(sdoc.shard, shardDocs);
        }
        shardDocs.add(sdoc);
      }
      // Now create a request for each shard to retrieve the stored fields
      for (Collection<ShardDoc> shardDocs : shardMap.values()) {
        ShardRequest sreq = new ShardRequest();
        sreq.purpose = ShardRequest.PURPOSE_GET_FIELDS;

        sreq.shards = new String[]{shardDocs.iterator().next().shard};

        sreq.params = new ModifiableSolrParams();

        // add original params
        sreq.params.add(rb.req.getParams());
        sreq.params.remove(CommonParams.Q);//remove the query
        ArrayList<String> ids = new ArrayList<String>(shardDocs.size());
        for (ShardDoc shardDoc : shardDocs) {
          ids.add(shardDoc.id.toString());
        }
        sreq.params.add(TermVectorParams.DOC_IDS, StrUtils.join(ids, ','));

        rb.addRequest(this, sreq);
      }
      result = ResponseBuilder.STAGE_DONE;
    }
    return result;
  }

  private static int getDocFreq(IndexReader reader, String field, BytesRef term) {
    int result = 1;
    try {
      Terms terms = MultiFields.getTerms(reader, field);
      if (terms != null) {
        TermsEnum termsEnum = terms.iterator(null);
        if (termsEnum.seekExact(term, true)) {
          result = termsEnum.docFreq();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {

  }

  //////////////////////// NamedListInitializedPlugin methods //////////////////////

  @Override
  public void init(NamedList args) {
    super.init(args);
    this.initParams = args;
  }

  public void inform(SolrCore core) {

  }

  @Override
  public String getVersion() {
    return "$Revision$";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }

  @Override
  public String getDescription() {
    return "A Component for working with Term Vectors";
  }
}

class FieldOptions {
  String fieldName;
  boolean termFreq, positions, offsets, docFreq, tfIdf;
}
