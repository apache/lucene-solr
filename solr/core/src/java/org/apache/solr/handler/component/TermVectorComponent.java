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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.TermVectorParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrDocumentFetcher;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * Return term vectors for the documents in a query result set.
 * <p>
 * Info available:
 * term, frequency, position, offset, payloads, IDF.
 * <p>
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

  public static final String TERM_VECTORS = "termVectors";

  private static final String TV_KEY_WARNINGS = "warnings";

  @SuppressWarnings({"rawtypes"})
  protected NamedList initParams;

  /**
   * Helper method for determining the list of fields that we should 
   * try to find term vectors on.  
   * <p>
   * Does simple (non-glob-supporting) parsing on the 
   * {@link TermVectorParams#FIELDS} param if specified, otherwise it returns 
   * the concrete field values specified in {@link CommonParams#FL} -- 
   * ignoring functions, transformers, or literals.  
   * </p>
   * <p>
   * If "fl=*" is used, or neither param is specified, then <code>null</code> 
   * will be returned.  If the empty set is returned, it means the "fl" 
   * specified consisted entirely of things that are not real fields 
   * (ie: functions, transformers, partial-globs, score, etc...) and not 
   * supported by this component. 
   * </p>
   */
  private Set<String> getFields(ResponseBuilder rb) {
    SolrParams params = rb.req.getParams();
    String[] fldLst = params.getParams(TermVectorParams.FIELDS);
    if (null == fldLst || 0 == fldLst.length || 
        (1 == fldLst.length && 0 == fldLst[0].length())) {

      // no tv.fl, parse the main fl
      ReturnFields rf = new SolrReturnFields
        (params.getParams(CommonParams.FL), rb.req);

      if (rf.wantsAllFields()) {
        return null;
      }

      Set<String> fieldNames = rf.getLuceneFieldNames();
      return (null != fieldNames) ?
        fieldNames :
        // return empty set indicating no fields should be used
        Collections.<String>emptySet();
    }

    // otherwise us the raw fldList as is, no special parsing or globs
    Set<String> fieldNames = new LinkedHashSet<>();
    for (String fl : fldLst) {
      fieldNames.addAll(Arrays.asList(SolrPluginUtils.split(fl)));
    }
    return fieldNames;
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false)) {
      return;
    }

    NamedList<Object> termVectors = new NamedList<>();
    rb.rsp.add(TERM_VECTORS, termVectors);

    IndexSchema schema = rb.req.getSchema();
    SchemaField keyField = schema.getUniqueKeyField();
    String uniqFieldName = null;
    if (keyField != null) {
      uniqFieldName = keyField.getName();
    }

    FieldOptions allFields = new FieldOptions();
    //figure out what options we have, and try to get the appropriate vector
    allFields.termFreq = params.getBool(TermVectorParams.TF, false);
    allFields.positions = params.getBool(TermVectorParams.POSITIONS, false);
    allFields.offsets = params.getBool(TermVectorParams.OFFSETS, false);
    allFields.payloads = params.getBool(TermVectorParams.PAYLOADS, false);
    allFields.docFreq = params.getBool(TermVectorParams.DF, false);
    allFields.tfIdf = params.getBool(TermVectorParams.TF_IDF, false);
    //boolean cacheIdf = params.getBool(TermVectorParams.IDF, false);
    //short cut to all values.
    if (params.getBool(TermVectorParams.ALL, false)) {
      allFields.termFreq = true;
      allFields.positions = true;
      allFields.offsets = true;
      allFields.payloads = true;
      allFields.docFreq = true;
      allFields.tfIdf = true;
    }

    //Build up our per field mapping
    Map<String, FieldOptions> fieldOptions = new HashMap<>();
    NamedList<List<String>> warnings = new NamedList<>();
    List<String>  noTV = new ArrayList<>();
    List<String>  noPos = new ArrayList<>();
    List<String>  noOff = new ArrayList<>();
    List<String>  noPay = new ArrayList<>();

    Set<String> fields = getFields(rb);
    if ( null != fields ) {
      //we have specific fields to retrieve, or no fields
      for (String field : fields) {

        // workaround SOLR-3523
        if (null == field || "score".equals(field)) continue; 

        // we don't want to issue warnings about the uniqueKey field
        // since it can cause lots of confusion in distributed requests
        // where the uniqueKey field is injected into the fl for merging
        final boolean fieldIsUniqueKey = field.equals(uniqFieldName);

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
            if (option.positions && !sf.storeTermPositions() && !fieldIsUniqueKey){
              noPos.add(field);
            }
            option.offsets = params.getFieldBool(field, TermVectorParams.OFFSETS, allFields.offsets);
            if (option.offsets && !sf.storeTermOffsets() && !fieldIsUniqueKey){
              noOff.add(field);
            }
            option.payloads = params.getFieldBool(field, TermVectorParams.PAYLOADS, allFields.payloads);
            if (option.payloads && !sf.storeTermPayloads() && !fieldIsUniqueKey){
              noPay.add(field);
            }
          } else {//field doesn't have term vectors
            if (!fieldIsUniqueKey) noTV.add(field);
          }
        } else {
          //field doesn't exist
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "undefined field: " + field);
        }
      }
    } //else, deal with all fields

    // NOTE: currently all types of warnings are schema driven, and guaranteed
    // to be consistent across all shards - if additional types of warnings 
    // are added that might be different between shards, finishStage() needs
    // to be changed to account for that.
    if (!noTV.isEmpty()) {
      warnings.add("noTermVectors", noTV);
    }
    if (!noPos.isEmpty()) {
      warnings.add("noPositions", noPos);
    }
    if (!noOff.isEmpty()) {
      warnings.add("noOffsets", noOff);
    }
    if (!noPay.isEmpty()) {
      warnings.add("noPayloads", noPay);
    }
    if (warnings.size() > 0) {
      termVectors.add(TV_KEY_WARNINGS, warnings);
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

    //Only load the id field to get the uniqueKey of that
    //field
    SolrDocumentFetcher docFetcher = searcher.getDocFetcher();
    SolrReturnFields srf = new SolrReturnFields(uniqFieldName, rb.req);

    while (iter.hasNext()) {
      Integer docId = iter.next();
      NamedList<Object> docNL = new NamedList<>();

      if (keyField != null) {
        // guaranteed to be one and only one since this is uniqueKey!
        SolrDocument solrDoc = docFetcher.solrDoc(docId, srf);
        String uKey = schema.printableUniqueKey(solrDoc);
        assert null != uKey;
        docNL.add("uniqueKey", uKey);
        termVectors.add(uKey, docNL);
      } else {
        // support for schemas w/o a unique key,
        termVectors.add("doc-" + docId, docNL);
      }

      if (null != fields) {
        for (Map.Entry<String, FieldOptions> entry : fieldOptions.entrySet()) {
          final String field = entry.getKey();
          final Terms vector = reader.getTermVector(docId, field);
          if (vector != null) {
            TermsEnum termsEnum = vector.iterator();
            mapOneVector(docNL, entry.getValue(), reader, docId, termsEnum, field);
          }
        }
      } else {
        // extract all fields
        final Fields vectors = reader.getTermVectors(docId);
        // There can be no documents with vectors
        if (vectors != null) {
          for (String field : vectors) {
            Terms terms = vectors.terms(field);
            if (terms != null) {
              TermsEnum termsEnum = terms.iterator();
              mapOneVector(docNL, allFields, reader, docId, termsEnum, field);
            }
          }
        }
      }
    }
  }

  private void mapOneVector(NamedList<Object> docNL, FieldOptions fieldOptions, IndexReader reader, int docID, TermsEnum termsEnum, String field) throws IOException {
    NamedList<Object> fieldNL = new NamedList<>();
    docNL.add(field, fieldNL);

    BytesRef text;
    PostingsEnum dpEnum = null;
    while((text = termsEnum.next()) != null) {
      String term = text.utf8ToString();
      NamedList<Object> termInfo = new NamedList<>();
      fieldNL.add(term, termInfo);
      final int freq = (int) termsEnum.totalTermFreq();
      if (fieldOptions.termFreq == true) {
        termInfo.add("tf", freq);
      }

      int dpEnumFlags = 0;
      dpEnumFlags |= fieldOptions.positions ? PostingsEnum.POSITIONS : 0;
      //payloads require offsets
      dpEnumFlags |= (fieldOptions.offsets || fieldOptions.payloads) ? PostingsEnum.OFFSETS : 0;
      dpEnumFlags |= fieldOptions.payloads ? PostingsEnum.PAYLOADS : 0;
      dpEnum = termsEnum.postings(dpEnum, dpEnumFlags);

      boolean atNextDoc = false;
      if (dpEnum != null) {
        dpEnum.nextDoc();
        atNextDoc = true;
      }

      if (atNextDoc && dpEnumFlags != 0) {
        NamedList<Integer> positionsNL = null;
        NamedList<Number> theOffsets = null;
        NamedList<String> thePayloads = null;

        for (int i = 0; i < freq; i++) {
          final int pos = dpEnum.nextPosition();
          if (fieldOptions.positions && pos >= 0) {
            if (positionsNL == null) {
              positionsNL = new NamedList<>();
              termInfo.add("positions", positionsNL);
            }
            positionsNL.add("position", pos);
          }

          int startOffset = fieldOptions.offsets ? dpEnum.startOffset() : -1;
          if (startOffset >= 0) {
            if (theOffsets == null) {
              theOffsets = new NamedList<>();
              termInfo.add("offsets", theOffsets);
            }
            theOffsets.add("start", dpEnum.startOffset());
            theOffsets.add("end", dpEnum.endOffset());
          }

          BytesRef payload = fieldOptions.payloads ? dpEnum.getPayload() : null;
          if (payload != null) {
            if (thePayloads == null) {
              thePayloads = new NamedList<>();
              termInfo.add("payloads", thePayloads);
            }
            thePayloads.add("payload", Base64.byteArrayToBase64(payload.bytes, payload.offset, payload.length));
          }
        }
      }
      
      int df = 0;
      if (fieldOptions.docFreq || fieldOptions.tfIdf) {
        df = reader.docFreq(new Term(field, text));
      }

      if (fieldOptions.docFreq) {
        termInfo.add("df", df);
      }

      // TODO: this is not TF/IDF by anyone's definition!
      if (fieldOptions.tfIdf) {
        double tfIdfVal = ((double) freq) / df;
        termInfo.add("tf-idf", tfIdfVal);
      }
    }
  }

  private List<Integer> getInts(String[] vals) {
    List<Integer> result = null;
    if (vals != null && vals.length > 0) {
      result = new ArrayList<>(vals.length);
      for (int i = 0; i < vals.length; i++) {
        try {
          result.add(Integer.valueOf(vals[i]));
        } catch (NumberFormatException e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e.getMessage(), e);
        }
      }
    }
    return result;
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {

  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      
      NamedList<Object> termVectorsNL = new NamedList<>();

      @SuppressWarnings({"unchecked", "rawtypes"})
      Map.Entry<String, Object>[] arr = new NamedList.NamedListEntry[rb.resultIds.size()];

      for (ShardRequest sreq : rb.finished) {
        if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) == 0 || !sreq.params.getBool(COMPONENT_NAME, false)) {
          continue;
        }
        for (ShardResponse srsp : sreq.responses) {
          @SuppressWarnings({"unchecked"})
          NamedList<Object> nl = (NamedList<Object>)srsp.getSolrResponse().getResponse().get(TERM_VECTORS);

          // Add metadata (that which isn't a uniqueKey value):
          Object warningsNL = nl.get(TV_KEY_WARNINGS);
          // assume if that if warnings is already present; we don't need to merge.
          if (warningsNL != null && termVectorsNL.indexOf(TV_KEY_WARNINGS, 0) < 0) {
            termVectorsNL.add(TV_KEY_WARNINGS, warningsNL);
          }

          // UniqueKey data
          SolrPluginUtils.copyNamedListIntoArrayByDocPosInResponse(nl, rb.resultIds, arr);
        }
      }
      // remove nulls in case not all docs were able to be retrieved
      SolrPluginUtils.removeNulls(arr, termVectorsNL);
      rb.rsp.add(TERM_VECTORS, termVectorsNL);
    }
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false)) return;
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) == 0) {
      sreq.params.set(COMPONENT_NAME, "false");
    }
  }

  //////////////////////// NamedListInitializedPlugin methods //////////////////////

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    super.init(args);
    this.initParams = args;
  }

  @Override
  public void inform(SolrCore core) {

  }

  @Override
  public String getDescription() {
    return "A Component for working with Term Vectors";
  }

  @Override
  public Category getCategory() {
    return Category.QUERY;
  }
}

class FieldOptions {
  String fieldName;
  boolean termFreq, positions, offsets, payloads, docFreq, tfIdf;
}
