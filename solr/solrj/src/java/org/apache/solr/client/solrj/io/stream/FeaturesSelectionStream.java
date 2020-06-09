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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
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
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.ID;

/**
 * @since 6.2.0
 */
public class FeaturesSelectionStream extends TupleStream implements Expressible{

  private static final long serialVersionUID = 1;

  protected String zkHost;
  protected String collection;
  protected Map<String,String> params;
  protected Iterator<Tuple> tupleIterator;
  protected String field;
  protected String outcome;
  protected String featureSet;
  protected int positiveLabel;
  protected int numTerms;



  protected transient SolrClientCache cache;
  protected transient boolean isCloseCache;
  protected transient CloudSolrClient cloudSolrClient;

  protected transient StreamContext streamContext;
  protected ExecutorService executorService;


  public FeaturesSelectionStream(String zkHost,
                     String collectionName,
                     @SuppressWarnings({"rawtypes"})Map params,
                     String field,
                     String outcome,
                     String featureSet,
                     int positiveLabel,
                     int numTerms) throws IOException {

    init(collectionName, zkHost, params, field, outcome, featureSet, positiveLabel, numTerms);
  }

  /**
   *   logit(collection, zkHost="", features="a,b,c,d,e,f,g", outcome="y", maxIteration="20")
   **/

  public FeaturesSelectionStream(StreamExpression expression, StreamFactory factory) throws IOException{
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
      throw new IOException("field param cannot be null for FeaturesSelectionStream");
    }

    String outcomeParam = params.get("outcome");
    if(outcomeParam != null) {
      params.remove("outcome");
    } else {
      throw new IOException("outcome param cannot be null for FeaturesSelectionStream");
    }

    String featureSetParam = params.get("featureSet");
    if(featureSetParam != null) {
      params.remove("featureSet");
    } else {
      throw new IOException("featureSet param cannot be null for FeaturesSelectionStream");
    }

    String positiveLabelParam = params.get("positiveLabel");
    int positiveLabel = 1;
    if(positiveLabelParam != null) {
      params.remove("positiveLabel");
      positiveLabel = Integer.parseInt(positiveLabelParam);
    }

    String numTermsParam = params.get("numTerms");
    int numTerms = 1;
    if(numTermsParam != null) {
      numTerms = Integer.parseInt(numTermsParam);
      params.remove("numTerms");
    } else {
      throw new IOException("numTerms param cannot be null for FeaturesSelectionStream");
    }

    // zkHost, optional - if not provided then will look into factory list to get
    String zkHost = null;
    if(null == zkHostExpression){
      zkHost = factory.getCollectionZkHost(collectionName);
    }
    else if(zkHostExpression.getParameter() instanceof StreamExpressionValue){
      zkHost = ((StreamExpressionValue)zkHostExpression.getParameter()).getValue();
    }
    if(null == zkHost){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - zkHost not found for collection '%s'",expression,collectionName));
    }

    // We've got all the required items
    init(collectionName, zkHost, params, fieldParam, outcomeParam, featureSetParam, positiveLabel, numTerms);
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
    expression.addParameter(new StreamExpressionNamedParameter("outcome", outcome));
    expression.addParameter(new StreamExpressionNamedParameter("featureSet", featureSet));
    expression.addParameter(new StreamExpressionNamedParameter("positiveLabel", String.valueOf(positiveLabel)));
    expression.addParameter(new StreamExpressionNamedParameter("numTerms", String.valueOf(numTerms)));

    // zkHost
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));

    return expression;
  }

  @SuppressWarnings({"unchecked"})
  private void init(String collectionName,
                    String zkHost,
                    @SuppressWarnings({"rawtypes"})Map params,
                    String field,
                    String outcome,
                    String featureSet,
                    int positiveLabel, int numTopTerms) throws IOException {
    this.zkHost = zkHost;
    this.collection = collectionName;
    this.params = params;
    this.field = field;
    this.outcome = outcome;
    this.featureSet = featureSet;
    this.positiveLabel = positiveLabel;
    this.numTerms = numTopTerms;
  }

  public void setStreamContext(StreamContext context) {
    this.cache = context.getSolrClientCache();
    this.streamContext = context;
  }

  /**
   * Opens the CloudSolrStream
   *
   ***/
  public void open() throws IOException {
    if (cache == null) {
      isCloseCache = true;
      cache = new SolrClientCache();
    } else {
      isCloseCache = false;
    }

    this.cloudSolrClient = this.cache.getCloudSolrClient(zkHost);
    this.executorService = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("FeaturesSelectionStream"));
  }

  public List<TupleStream> children() {
    return null;
  }

  private List<String> getShardUrls() throws IOException {
    try {
      ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();

      Slice[] slices = CloudSolrStream.getSlices(this.collection, zkStateReader, false);

      ClusterState clusterState = zkStateReader.getClusterState();
      Set<String> liveNodes = clusterState.getLiveNodes();

      List<String> baseUrls = new ArrayList<>();
      for(Slice slice : slices) {
        Collection<Replica> replicas = slice.getReplicas();
        List<Replica> shuffler = new ArrayList<>();
        for(Replica replica : replicas) {
          if(replica.getState() == Replica.State.ACTIVE && liveNodes.contains(replica.getNodeName())) {
            shuffler.add(replica);
          }
        }

        Collections.shuffle(shuffler, new Random());
        Replica rep = shuffler.get(0);
        ZkCoreNodeProps zkProps = new ZkCoreNodeProps(rep);
        String url = zkProps.getCoreUrl();
        baseUrls.add(url);
      }

      return baseUrls;

    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @SuppressWarnings({"rawtypes"})
  private List<Future<NamedList>> callShards(List<String> baseUrls) throws IOException {

    List<Future<NamedList>> futures = new ArrayList<>();
    for (String baseUrl : baseUrls) {
      FeaturesSelectionCall lc = new FeaturesSelectionCall(baseUrl,
          this.params,
          this.field,
          this.outcome);

      Future<NamedList> future = executorService.submit(lc);
      futures.add(future);
    }

    return futures;
  }

  public void close() throws IOException {
    if (isCloseCache && cache != null) {
      cache.close();
    }

    if (executorService != null) {
      executorService.shutdown();
    }
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

  public Tuple read() throws IOException {
    try {
      if (tupleIterator == null) {
        Map<String, Double> termScores = new HashMap<>();
        Map<String, Long> docFreqs = new HashMap<>();


        long numDocs = 0;
        for (@SuppressWarnings({"rawtypes"})Future<NamedList> getTopTermsCall : callShards(getShardUrls())) {
          @SuppressWarnings({"rawtypes"})
          NamedList resp = getTopTermsCall.get();

          @SuppressWarnings({"unchecked"})
          NamedList<Double> shardTopTerms = (NamedList<Double>)resp.get("featuredTerms");
          @SuppressWarnings({"unchecked"})
          NamedList<Integer> shardDocFreqs = (NamedList<Integer>)resp.get("docFreq");

          numDocs += (Integer)resp.get("numDocs");

          for (int i = 0; i < shardTopTerms.size(); i++) {
            String term = shardTopTerms.getName(i);
            double score = shardTopTerms.getVal(i);
            int docFreq = shardDocFreqs.get(term);
            double prevScore = termScores.containsKey(term) ? termScores.get(term) : 0;
            long prevDocFreq = docFreqs.containsKey(term) ? docFreqs.get(term) : 0;
            termScores.put(term, prevScore + score);
            docFreqs.put(term, prevDocFreq + docFreq);

          }
        }

        List<Tuple> tuples = new ArrayList<>(numTerms);
        termScores = sortByValue(termScores);
        int index = 0;
        for (Map.Entry<String, Double> termScore : termScores.entrySet()) {
          if (tuples.size() == numTerms) break;
          index++;
          Tuple tuple = new Tuple();
          tuple.put(ID, featureSet + "_" + index);
          tuple.put("index_i", index);
          tuple.put("term_s", termScore.getKey());
          tuple.put("score_f", termScore.getValue());
          tuple.put("featureSet_s", featureSet);
          long docFreq = docFreqs.get(termScore.getKey());
          double d = Math.log(((double)numDocs / (double)(docFreq + 1)));
          tuple.put("idf_d", d);
          tuples.add(tuple);
        }

        tuples.add(Tuple.EOF());

        tupleIterator = tuples.iterator();
      }

      return tupleIterator.next();
    } catch(Exception e) {
      throw new IOException(e);
    }
  }

  private  <K, V extends Comparable<? super V>> Map<K, V> sortByValue( Map<K, V> map )
  {
    Map<K, V> result = new LinkedHashMap<>();
    Stream<Map.Entry<K, V>> st = map.entrySet().stream();

    st.sorted( Map.Entry.comparingByValue(
        (c1, c2) -> c2.compareTo(c1)
    ) ).forEachOrdered( e -> result.put(e.getKey(), e.getValue()) );

    return result;
  }

  @SuppressWarnings({"rawtypes"})
  protected class FeaturesSelectionCall implements Callable<NamedList> {

    private String baseUrl;
    private String outcome;
    private String field;
    private Map<String, String> paramsMap;

    public FeaturesSelectionCall(String baseUrl,
                                 Map<String, String> paramsMap,
                                 String field,
                                 String outcome) {

      this.baseUrl = baseUrl;
      this.outcome = outcome;
      this.field = field;
      this.paramsMap = paramsMap;
    }

    @SuppressWarnings({"unchecked"})
    public NamedList<Double> call() throws Exception {
      ModifiableSolrParams params = new ModifiableSolrParams();
      HttpSolrClient solrClient = cache.getHttpSolrClient(baseUrl);

      params.add(DISTRIB, "false");
      params.add("fq","{!igain}");

      for(Map.Entry<String, String> entry : paramsMap.entrySet()) {
        params.add(entry.getKey(), entry.getValue());
      }

      params.add("outcome", outcome);
      params.add("positiveLabel", Integer.toString(positiveLabel));
      params.add("field", field);
      params.add("numTerms", String.valueOf(numTerms));

      QueryRequest request= new QueryRequest(params);
      QueryResponse response = request.process(solrClient);
      NamedList res = response.getResponse();
      return res;
    }
  }
}
