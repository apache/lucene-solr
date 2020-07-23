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

package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;

import static org.apache.solr.common.params.CommonParams.DISTRIB;

/**
 * @since 6.5.0
 */
public class SignificantTermsStream extends TupleStream implements Expressible{

  private static final long serialVersionUID = 1;

  protected String zkHost;
  protected String collection;
  protected Map<String,String> params;
  protected Iterator<Tuple> tupleIterator;
  protected String field;
  protected int numTerms;
  protected float minDocFreq;
  protected float maxDocFreq;
  protected int minTermLength;

  protected transient SolrClientCache cache;
  protected transient boolean isCloseCache;
  protected transient StreamContext streamContext;
  protected ExecutorService executorService;

  public SignificantTermsStream(String zkHost,
                                 String collectionName,
                                 @SuppressWarnings({"rawtypes"})Map params,
                                 String field,
                                 float minDocFreq,
                                 float maxDocFreq,
                                 int minTermLength,
                                 int numTerms) throws IOException {

    init(collectionName,
         zkHost,
         params,
         field,
         minDocFreq,
         maxDocFreq,
         minTermLength,
         numTerms);
  }

  public SignificantTermsStream(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");

    // Validate there are no unknown parameters - zkHost and alias are namedParameter so we don't need to count it twice
    if(expression.getParameters().size() != 1 + namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - unknown operands found",expression));
    }

    // Collection Name
    if(null == collectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }

    // Named parameters - passed directly to solr as solrparams
    if(0 == namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - at least one named parameter expected. eg. 'q=*:*'",expression));
    }

    Map<String,String> params = new HashMap<String,String>();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost")) {
        params.put(namedParam.getName(), namedParam.getParameter().toString().trim());
      }
    }

    String fieldParam = params.get("field");
    if(fieldParam != null) {
      params.remove("field");
    } else {
      throw new IOException("field param cannot be null for SignificantTermsStream");
    }

    String numTermsParam = params.get("limit");
    int numTerms = 20;
    if(numTermsParam != null) {
      numTerms = Integer.parseInt(numTermsParam);
      params.remove("limit");
    }

    String minTermLengthParam = params.get("minTermLength");
    int minTermLength = 4;
    if(minTermLengthParam != null) {
      minTermLength = Integer.parseInt(minTermLengthParam);
      params.remove("minTermLength");
    }


    String minDocFreqParam = params.get("minDocFreq");
    float minDocFreq = 5.0F;
    if(minDocFreqParam != null) {
      minDocFreq = Float.parseFloat(minDocFreqParam);
      params.remove("minDocFreq");
    }

    String maxDocFreqParam = params.get("maxDocFreq");
    float maxDocFreq = .3F;
    if(maxDocFreqParam != null) {
      maxDocFreq = Float.parseFloat(maxDocFreqParam);
      params.remove("maxDocFreq");
    }


    // zkHost, optional - if not provided then will look into factory list to get
    String zkHost = null;
    if(null == zkHostExpression){
      zkHost = factory.getCollectionZkHost(collectionName);
    } else if(zkHostExpression.getParameter() instanceof StreamExpressionValue) {
      zkHost = ((StreamExpressionValue)zkHostExpression.getParameter()).getValue();
    }

    if(zkHost == null){
      zkHost = factory.getDefaultZkHost();
    }

    // We've got all the required items
    init(collectionName, zkHost, params, fieldParam, minDocFreq, maxDocFreq, minTermLength, numTerms);
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    // functionName(collectionName, param1, param2, ..., paramN, sort="comp", [aliases="field=alias,..."])

    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    // collection
    expression.addParameter(collection);

    // parameters
    for(Map.Entry<String,String> param : params.entrySet()){
      expression.addParameter(new StreamExpressionNamedParameter(param.getKey(), param.getValue()));
    }

    expression.addParameter(new StreamExpressionNamedParameter("field", field));
    expression.addParameter(new StreamExpressionNamedParameter("minDocFreq", Float.toString(minDocFreq)));
    expression.addParameter(new StreamExpressionNamedParameter("maxDocFreq", Float.toString(maxDocFreq)));
    expression.addParameter(new StreamExpressionNamedParameter("numTerms", String.valueOf(numTerms)));
    expression.addParameter(new StreamExpressionNamedParameter("minTermLength", String.valueOf(minTermLength)));

    // zkHost
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));

    return expression;
  }

  @SuppressWarnings({"unchecked"})
  private void init(String collectionName,
                    String zkHost,
                    @SuppressWarnings({"rawtypes"})Map params,
                    String field,
                    float minDocFreq,
                    float maxDocFreq,
                    int minTermLength,
                    int numTerms) throws IOException {
    this.zkHost = zkHost;
    this.collection = collectionName;
    this.params = params;
    this.field = field;
    this.minDocFreq = minDocFreq;
    this.maxDocFreq = maxDocFreq;
    this.numTerms = numTerms;
    this.minTermLength = minTermLength;
  }

  public void setStreamContext(StreamContext context) {
    this.cache = context.getSolrClientCache();
    this.streamContext = context;
  }

  public void open() throws IOException {
    if (cache == null) {
      isCloseCache = true;
      cache = new SolrClientCache();
    } else {
      isCloseCache = false;
    }

    this.executorService = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("SignificantTermsStream"));
  }

  public List<TupleStream> children() {
    return null;
  }

  @SuppressWarnings({"rawtypes"})
  private List<Future<NamedList>> callShards(List<String> baseUrls) throws IOException {

    List<Future<NamedList>> futures = new ArrayList<>();
    for (String baseUrl : baseUrls) {
      SignificantTermsCall lc = new SignificantTermsCall(baseUrl,
          this.params,
          this.field,
          this.minDocFreq,
          this.maxDocFreq,
          this.minTermLength,
          this.numTerms);

      @SuppressWarnings({"rawtypes"})
      Future<NamedList> future = executorService.submit(lc);
      futures.add(future);
    }

    return futures;
  }

  public void close() throws IOException {
    if (isCloseCache) {
      cache.close();
    }

    executorService.shutdown();
  }

  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return null;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new StreamExplanation(getStreamNodeId().toString())
        .withFunctionName(factory.getFunctionName(this.getClass()))
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(Explanation.ExpressionType.STREAM_DECORATOR)
        .withExpression(toExpression(factory).toString());
  }

  @SuppressWarnings({"unchecked"})
  public Tuple read() throws IOException {
    try {
      if (tupleIterator == null) {
        Map<String, int[]> mergeFreqs = new HashMap<>();
        long numDocs = 0;
        long resultCount = 0;
        for (@SuppressWarnings({"rawtypes"})Future<NamedList> getTopTermsCall : callShards(getShards(zkHost, collection, streamContext))) {
          @SuppressWarnings({"rawtypes"})
          NamedList fullResp = getTopTermsCall.get();
          @SuppressWarnings({"rawtypes"})
          Map stResp = (Map)fullResp.get("significantTerms");

          List<String> terms = (List<String>)stResp.get("sterms");
          List<Integer> docFreqs = (List<Integer>)stResp.get("docFreq");
          List<Integer> queryDocFreqs = (List<Integer>)stResp.get("queryDocFreq");
          numDocs += (Integer)stResp.get("numDocs");

          SolrDocumentList searchResp = (SolrDocumentList) fullResp.get("response");
          resultCount += searchResp.getNumFound();

          for (int i = 0; i < terms.size(); i++) {
            String term = terms.get(i);
            int docFreq = docFreqs.get(i);
            int queryDocFreq = queryDocFreqs.get(i);
            if(!mergeFreqs.containsKey(term)) {
              mergeFreqs.put(term, new int[2]);
            }

            int[] freqs = mergeFreqs.get(term);
            freqs[0] += docFreq;
            freqs[1] += queryDocFreq;
          }
        }

        List<Map<String, Object>> maps = new ArrayList<>();

        for(Map.Entry<String, int[]> entry : mergeFreqs.entrySet()) {
          int[] freqs = entry.getValue();
          Map<String, Object> map = new HashMap<>();
          map.put("term", entry.getKey());
          map.put("background", freqs[0]);
          map.put("foreground", freqs[1]);

          float score = (float)(Math.log(freqs[1])+1.0) * (float) (Math.log(((float)(numDocs + 1)) / (freqs[0] + 1)) + 1.0);

          map.put("score", score);
          maps.add(map);
        }

        Collections.sort(maps, new ScoreComp());
        List<Tuple> tuples = new ArrayList<>();
        for (Map<String, Object> map : maps) {
          if (tuples.size() == numTerms) break;
          tuples.add(new Tuple(map));
        }

        tuples.add(Tuple.EOF());
        tupleIterator = tuples.iterator();
      }

      return tupleIterator.next();
    } catch(Exception e) {
      throw new IOException(e);
    }
  }

  @SuppressWarnings({"rawtypes"})
  private static class ScoreComp implements Comparator<Map> {
    public int compare(Map a, Map b) {
      Float scorea = (Float)a.get("score");
      Float scoreb = (Float)b.get("score");
      return scoreb.compareTo(scorea);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected class SignificantTermsCall implements Callable<NamedList> {

    private String baseUrl;
    private String field;
    private float minDocFreq;
    private float maxDocFreq;
    private int numTerms;
    private int minTermLength;
    private Map<String, String> paramsMap;

    public SignificantTermsCall(String baseUrl,
                                 Map<String, String> paramsMap,
                                 String field,
                                 float minDocFreq,
                                 float maxDocFreq,
                                 int minTermLength,
                                 int numTerms) {

      this.baseUrl = baseUrl;
      this.field = field;
      this.minDocFreq = minDocFreq;
      this.maxDocFreq = maxDocFreq;
      this.paramsMap = paramsMap;
      this.numTerms = numTerms;
      this.minTermLength = minTermLength;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public NamedList<Double> call() throws Exception {
      ModifiableSolrParams params = new ModifiableSolrParams();
      HttpSolrClient solrClient = cache.getHttpSolrClient(baseUrl);

      params.add(DISTRIB, "false");
      params.add("fq","{!significantTerms}");

      for(Map.Entry<String, String> entry : paramsMap.entrySet()) {
        params.add(entry.getKey(), entry.getValue());
      }

      params.add("minDocFreq", Float.toString(minDocFreq));
      params.add("maxDocFreq", Float.toString(maxDocFreq));
      params.add("minTermLength", Integer.toString(minTermLength));
      params.add("field", field);
      params.add("numTerms", String.valueOf(numTerms*5));

      QueryRequest request= new QueryRequest(params, SolrRequest.METHOD.POST);
      QueryResponse response = request.process(solrClient);
      NamedList res = response.getResponse();
      return res;
    }
  }
}
