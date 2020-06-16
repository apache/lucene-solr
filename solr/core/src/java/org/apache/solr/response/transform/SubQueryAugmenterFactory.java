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
package org.apache.solr.response.transform;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResultContext;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.JoinQParserPlugin;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.TermsQParserPlugin;

/**
 *
 * This transformer executes subquery per every result document. It must be given an unique name. 
 * There might be a few of them, eg <code>fl=*,foo:[subquery],bar:[subquery]</code>. 
 * Every [subquery] occurrence adds a field into a result document with the given name, 
 * the value of this field is a document list, which is a result of executing subquery using 
 * document fields as an input.
 * 
 * <h3>Subquery Parameters Shift</h3>
 * if subquery is declared as <code>fl=*,foo:[subquery]</code>, subquery parameters 
 * are prefixed with the given name and period. eg <br>
 * <code>q=*:*&amp;fl=*,foo:[subquery]&amp;foo.q=to be continued&amp;foo.rows=10&amp;foo.sort=id desc</code>
 * 
 * <h3>Document Field As An Input For Subquery Parameters</h3>
 * 
 * It's necessary to pass some document field value as a parameter for subquery. It's supported via 
 * implicit <code>row.<i>fieldname</i></code> parameters, and can be (but might not only) referred via
 *  Local Parameters syntax.<br>
 * <code>q=name:john&amp;fl=name,id,depts:[subquery]&amp;depts.q={!terms f=id v=$row.dept_id}&amp;depts.rows=10</code>
 * Here departments are retrieved per every employee in search result. We can say that it's like SQL
 * <code> join ON emp.dept_id=dept.id </code><br>
 * Note, when document field has multiple values they are concatenated with comma by default, it can be changed by
 * <code>foo:[subquery separator=' ']</code> local parameter, this mimics {@link TermsQParserPlugin} to work smoothly with.
 * 
 * <h3>Cores And Collections In SolrCloud</h3>
 * use <code>foo:[subquery fromIndex=departments]</code> invoke subquery on another core on the same node, it's like
 *  {@link JoinQParserPlugin} for non SolrCloud mode. <b>But for SolrCloud</b> just (and only) <b>explicitly specify</b> 
 * its' native parameters like <code>collection, shards</code> for subquery, eg<br>
 *  <code>q=*:*&amp;fl=*,foo:[subquery]&amp;foo.q=cloud&amp;foo.collection=departments</code>
 *
 * <h3>When used in Real Time Get</h3>
 * <p>
 * When used in the context of a Real Time Get, the <i>values</i> from each document that are used 
 * in the subquery are the "real time" values (possibly from the transaction log), but the query
 * itself is still executed against the currently open searcher.  Note that this means if a 
 * document is updated but not yet committed, an RTG request for that document that uses 
 * <code>[subquery]</code> could include the older (committed) version of that document, 
 * with different field values, in the subquery results.
 * </p>
 */
public class SubQueryAugmenterFactory extends TransformerFactory{

  @Override
  public DocTransformer create(String field, SolrParams params, SolrQueryRequest req) {

    if (field.contains("[") || field.contains("]")) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
          "please give an explicit name for [subquery] column ie fl=relation:[subquery ..]");
    }
    
    checkThereIsNoDupe(field, req.getContext());
    
    String fromIndex = params.get("fromIndex");
    final SolrClient solrClient;

    solrClient = new EmbeddedSolrServer(req.getCore());

    SolrParams subParams = retainAndShiftPrefix(req.getParams(), field+".");
    

    return new SubQueryAugmenter(solrClient, fromIndex, field,
        field,
        subParams,
        params.get(TermsQParserPlugin.SEPARATOR, ","));
  }

  @SuppressWarnings("unchecked")
  private void checkThereIsNoDupe(String field, Map<Object,Object> context) {
    // find a map
    @SuppressWarnings({"rawtypes"})
    final Map conflictMap;
    final String conflictMapKey = getClass().getSimpleName();
    if (context.containsKey(conflictMapKey)) {
      conflictMap = (Map) context.get(conflictMapKey);
    } else {
      conflictMap = new HashMap<>();
      context.put(conflictMapKey, conflictMap);
    }
    // check entry absence 
    if (conflictMap.containsKey(field)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, 
          "[subquery] name "+field+" is duplicated");
    } else {
      conflictMap.put(field, true);
    }
  }

  private SolrParams retainAndShiftPrefix(SolrParams params, String subPrefix) {
    ModifiableSolrParams out = new ModifiableSolrParams();
    Iterator<String> baseKeyIt = params.getParameterNamesIterator();
    while (baseKeyIt.hasNext()) {
      String key = baseKeyIt.next();

      if (key.startsWith(subPrefix)) {
        out.set(key.substring(subPrefix.length()), params.getParams(key));
      }
    }
    return out;
  }
  
}

class SubQueryAugmenter extends DocTransformer {
  
  private static final class Result extends ResultContext {
    private final SolrDocumentList docList;
    final SolrReturnFields justWantAllFields = new SolrReturnFields();

    private Result(SolrDocumentList docList) {
      this.docList = docList;
    }

    @Override
    public ReturnFields getReturnFields() {
      return justWantAllFields;
    }

    @Override
    public Iterator<SolrDocument> getProcessedDocuments(){
      return  docList.iterator();
    }

    @Override
    public boolean wantsScores() {
      return justWantAllFields.wantsScore();
    }

    @Override
    public DocList getDocList() {
      return new DocSlice((int)docList.getStart(), 
          docList.size(), new int[0], new float[docList.size()],
          (int) docList.getNumFound(), 
          docList.getMaxScore() == null ?  Float.NaN : docList.getMaxScore(),
              docList.getNumFoundExact() ? TotalHits.Relation.EQUAL_TO : TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
    }

    @Override
    public SolrIndexSearcher getSearcher() {
      return null;
    }

    @Override
    public SolrQueryRequest getRequest() {
      return null;
    }

    @Override
    public Query getQuery() {
      return null;
    }
  }

  /** project document values to prefixed parameters
   * multivalues are joined with a separator, it always return single value */
  static final class DocRowParams extends SolrParams {
    
    final private SolrDocument doc;
    final private String prefixDotRowDot;
    final private String separator;

    public DocRowParams(SolrDocument doc, String prefix, String separator ) {
      this.doc = doc;
      this.prefixDotRowDot = "row.";//prefix+ ".row.";
      this.separator = separator;
    }

    @Override
    public String[] getParams(String param) {
      
      final Collection<Object> vals = mapToDocField(param);
      
      if (vals != null) {
        StringBuilder rez = new StringBuilder();
        for (@SuppressWarnings({"rawtypes"})Iterator iterator = vals.iterator(); iterator.hasNext();) {
          Object object = iterator.next();
          rez.append(convertFieldValue(object));
          if (iterator.hasNext()) {
            rez.append(separator);
          }
        } 
        return new String[]{rez.toString()};
      }
      return null;
    }
    
    
    @Override
    public String get(String param) {
      
      final String[] aVal = this.getParams(param);
      
      if (aVal != null) {
        assert aVal.length == 1 : "that's how getParams is written" ;
        return aVal[0];
      }
      return null;
    }

    /** @return null if prefix doesn't match, field is absent or empty */
    protected Collection<Object> mapToDocField(String param) {
      
      if (param.startsWith(prefixDotRowDot)) {
        final String docFieldName = param.substring(prefixDotRowDot.length());
        final Collection<Object> vals = doc.getFieldValues(docFieldName);
        
        if (vals == null || vals.isEmpty()) {
          return null;
        } else {
          return vals; 
        } 
      }
      return null;
    }
    

    protected String convertFieldValue(Object val) {
      
      if (val instanceof IndexableField) {
        IndexableField f = (IndexableField)val;
        return f.stringValue();
      }
      return val.toString();
      
    }

    @Override
    public Iterator<String> getParameterNamesIterator() {
      final Iterator<String> fieldNames = doc.getFieldNames().iterator();
      return new Iterator<String>() {

        @Override
        public boolean hasNext() {
          return fieldNames.hasNext();
        }

        @Override
        public String next() {
          final String fieldName = fieldNames.next();
          return prefixDotRowDot + fieldName;
        }
        
      };
    }

  }

  final private String name;
  final private SolrParams baseSubParams;
  final private String prefix;
  final private String separator;
  final private SolrClient server;
  final private String coreName;

  public SubQueryAugmenter(SolrClient server, String coreName,
      String name,String prefix, SolrParams baseSubParams, String separator) {
    this.name = name;
    this.prefix = prefix;
    this.baseSubParams = baseSubParams;
    this.separator = separator;
    this.server = server;
    this.coreName = coreName;
  }

  @Override
  public String getName() {
    return name;
  }
  
  /**
   * Returns false -- this transformer does use an IndexSearcher, but it does not (necessarily) need
   * the searcher from the ResultContext of the document being returned.  Instead we use the current 
   * "live" searcher for the specified core.
   */
  @Override
  public boolean needsSolrIndexSearcher() { return false; }

  @Override
  public void transform(SolrDocument doc, int docid) {

    final SolrParams docWithDeprefixed = SolrParams.wrapDefaults(
        new DocRowParams(doc, prefix, separator), baseSubParams);
    try {
      QueryResponse rsp = server.query(coreName, docWithDeprefixed);
      SolrDocumentList docList = rsp.getResults();
      doc.setField(getName(), new Result(docList));
    } catch (Exception e) {
      String docString = doc.toString();
      throw new SolrException(ErrorCode.BAD_REQUEST, "while invoking " +
          name + ":[subquery"+ (coreName!=null ? "fromIndex="+coreName : "") +"] on doc=" +
            docString.substring(0, Math.min(100, docString.length())), e.getCause());
    }
  }
}
