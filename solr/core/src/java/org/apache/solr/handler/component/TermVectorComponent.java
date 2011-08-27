package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.TermVectorMapper;
import org.apache.lucene.index.TermVectorOffsetInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexInput;
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
    final StoredFieldVisitor getUniqValue = new StoredFieldVisitor() {
      @Override 
      public boolean stringField(FieldInfo fieldInfo, IndexInput in, int numUTF8Bytes) throws IOException {
        if (fieldInfo.name.equals(finalUniqFieldName)) {
          final byte[] b = new byte[numUTF8Bytes];
          in.readBytes(b, 0, b.length);
          uniqValues.add(new String(b, "UTF-8"));
        } else {
          in.seek(in.getFilePointer() + numUTF8Bytes);
        }
        return false;
      }

      @Override 
      public boolean intField(FieldInfo fieldInfo, int value) throws IOException {
        if (fieldInfo.name.equals(finalUniqFieldName)) {
          uniqValues.add(Integer.toString(value));
        }
        return false;
      }

      @Override 
      public boolean longField(FieldInfo fieldInfo, long value) throws IOException {
        if (fieldInfo.name.equals(finalUniqFieldName)) {
          uniqValues.add(Long.toString(value));
        }
        return false;
      }
    };

    TVMapper mapper = new TVMapper(reader);
    mapper.fieldOptions = allFields; //this will only stay set if fieldOptions.isEmpty() (in other words, only if the user didn't set any fields)
    while (iter.hasNext()) {
      Integer docId = iter.next();
      NamedList<Object> docNL = new NamedList<Object>();
      mapper.docNL = docNL;
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
          mapper.fieldOptions = entry.getValue();
          reader.getTermFreqVector(docId, entry.getKey(), mapper);
        }
      } else {
        //deal with all fields by using the allFieldMapper
        reader.getTermFreqVector(docId, mapper);
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

  private static class TVMapper extends TermVectorMapper {
    private IndexReader reader;
    private NamedList<Object> docNL;

    //needs to be set for each new field
    FieldOptions fieldOptions;

    //internal vars not passed in by construction
    private boolean useOffsets, usePositions;
    //private Map<String, Integer> idfCache;
    private NamedList<Object> fieldNL;
    private String field;


    public TVMapper(IndexReader reader) {
      this.reader = reader;
    }

    @Override
    public void map(BytesRef term, int frequency, TermVectorOffsetInfo[] offsets, int[] positions) {
      NamedList<Object> termInfo = new NamedList<Object>();
      fieldNL.add(term.utf8ToString(), termInfo);
      if (fieldOptions.termFreq == true) {
        termInfo.add("tf", frequency);
      }
      if (useOffsets) {
        NamedList<Number> theOffsets = new NamedList<Number>();
        termInfo.add("offsets", theOffsets);
        for (int i = 0; i < offsets.length; i++) {
          TermVectorOffsetInfo offset = offsets[i];
          theOffsets.add("start", offset.getStartOffset());
          theOffsets.add("end", offset.getEndOffset());
        }
      }
      if (usePositions) {
        NamedList<Integer> positionsNL = new NamedList<Integer>();
        for (int i = 0; i < positions.length; i++) {
          positionsNL.add("position", positions[i]);
        }
        termInfo.add("positions", positionsNL);
      }
      if (fieldOptions.docFreq) {
        termInfo.add("df", getDocFreq(term));
      }
      if (fieldOptions.tfIdf) {
        double tfIdfVal = ((double) frequency) / getDocFreq(term);
        termInfo.add("tf-idf", tfIdfVal);
      }
    }

    private int getDocFreq(BytesRef term) {
      int result = 1;
      try {
        Terms terms = MultiFields.getTerms(reader, field);
        if (terms != null) {
          TermsEnum termsEnum = terms.iterator();
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
    public void setExpectations(String field, int numTerms, boolean storeOffsets, boolean storePositions) {
      this.field = field;
      useOffsets = storeOffsets && fieldOptions.offsets;
      usePositions = storePositions && fieldOptions.positions;
      fieldNL = new NamedList<Object>();
      docNL.add(field, fieldNL);
    }

    @Override
    public boolean isIgnoringPositions() {
      return !fieldOptions.positions;  // if we are not interested in positions, then return true telling Lucene to skip loading them
    }

    @Override
    public boolean isIgnoringOffsets() {
      return !fieldOptions.offsets;  //  if we are not interested in offsets, then return true telling Lucene to skip loading them
    }
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
