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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.ClassificationEvaluation;
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
import org.apache.solr.common.util.SolrjNamedThreadFactory;

public class TextLogitStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  protected String zkHost;
  protected String collection;
  protected Map<String,String> params;
  protected String field;
  protected String name;
  protected String outcome;
  protected int positiveLabel;
  protected double threshold;
  protected List<Double> weights;
  protected int maxIterations;
  protected int iteration;
  protected double error;
  protected List<Double> idfs;
  protected ClassificationEvaluation evaluation;

  protected transient SolrClientCache cache;
  protected transient boolean isCloseCache;
  protected transient CloudSolrClient cloudSolrClient;

  protected transient StreamContext streamContext;
  protected ExecutorService executorService;
  protected TupleStream termsStream;
  private List<String> terms;

  private double learningRate = 0.01;
  private double lastError = 0;

  public TextLogitStream(String zkHost,
                     String collectionName,
                     Map params,
                     String name,
                     String field,
                     TupleStream termsStream,
                     List<Double> weights,
                     String outcome,
                     int positiveLabel,
                     double threshold,
                     int maxIterations) throws IOException {

    init(collectionName, zkHost, params, name, field, termsStream, weights, outcome, positiveLabel, threshold, maxIterations, iteration);
  }

  /**
   *   logit(collection, zkHost="", features="a,b,c,d,e,f,g", outcome="y", maxIteration="20")
   **/

  public TextLogitStream(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);

    // Validate there are no unknown parameters - zkHost and alias are namedParameter so we don't need to count it twice
    if(expression.getParameters().size() != 1 + namedParams.size() + streamExpressions.size()){
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

    String name = params.get("name");
    if (name != null) {
      params.remove("name");
    } else {
      throw new IOException("name param cannot be null for TextLogitStream");
    }

    String feature = params.get("field");
    if (feature != null) {
      params.remove("field");
    } else {
      throw new IOException("field param cannot be null for TextLogitStream");
    }

    TupleStream stream = null;

    if (streamExpressions.size() > 0) {
      stream = factory.constructStream(streamExpressions.get(0));
    } else {
      throw new IOException("features must be present for TextLogitStream");
    }

    String maxIterationsParam = params.get("maxIterations");
    int maxIterations = 0;
    if(maxIterationsParam != null) {
      maxIterations = Integer.parseInt(maxIterationsParam);
      params.remove("maxIterations");
    } else {
      throw new IOException("maxIterations param cannot be null for TextLogitStream");
    }

    String outcomeParam = params.get("outcome");

    if(outcomeParam != null) {
      params.remove("outcome");
    } else {
      throw new IOException("outcome param cannot be null for TextLogitStream");
    }

    String positiveLabelParam = params.get("positiveLabel");
    int positiveLabel = 1;
    if(positiveLabelParam != null) {
      positiveLabel = Integer.parseInt(positiveLabelParam);
      params.remove("positiveLabel");
    }

    String thresholdParam = params.get("threshold");
    double threshold = 0.5;
    if(thresholdParam != null) {
      threshold = Double.parseDouble(thresholdParam);
      params.remove("threshold");
    }

    int iteration = 0;
    String iterationParam = params.get("iteration");
    if(iterationParam != null) {
      iteration = Integer.parseInt(iterationParam);
      params.remove("iteration");
    }

    List<Double> weights = null;
    String weightsParam = params.get("weights");
    if(weightsParam != null) {
      weights = new ArrayList<>();
      String[] weightsArray = weightsParam.split(",");
      for(String weightString : weightsArray) {
        weights.add(Double.parseDouble(weightString));
      }
      params.remove("weights");
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
    init(collectionName, zkHost, params, name, feature, stream, weights, outcomeParam, positiveLabel, threshold, maxIterations, iteration);
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    // collection
    expression.addParameter(collection);

    if (includeStreams && !(termsStream instanceof TermsStream)) {
      if (termsStream instanceof Expressible) {
        expression.addParameter(((Expressible)termsStream).toExpression(factory));
      } else {
        throw new IOException("This TextLogitStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }

    // parameters
    for(Entry<String,String> param : params.entrySet()){
      expression.addParameter(new StreamExpressionNamedParameter(param.getKey(), param.getValue()));
    }

    expression.addParameter(new StreamExpressionNamedParameter("field", field));
    expression.addParameter(new StreamExpressionNamedParameter("name", name));
    if (termsStream instanceof TermsStream) {
      loadTerms();
      expression.addParameter(new StreamExpressionNamedParameter("terms", toString(terms)));
    }

    expression.addParameter(new StreamExpressionNamedParameter("outcome", outcome));
    if(weights != null) {
      expression.addParameter(new StreamExpressionNamedParameter("weights", toString(weights)));
    }
    expression.addParameter(new StreamExpressionNamedParameter("maxIterations", Integer.toString(maxIterations)));

    if(iteration > 0) {
      expression.addParameter(new StreamExpressionNamedParameter("iteration", Integer.toString(iteration)));
    }

    expression.addParameter(new StreamExpressionNamedParameter("positiveLabel", Integer.toString(positiveLabel)));
    expression.addParameter(new StreamExpressionNamedParameter("threshold", Double.toString(threshold)));

    // zkHost
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));

    return expression;
  }

  private void init(String collectionName,
                    String zkHost,
                    Map params,
                    String name,
                    String feature,
                    TupleStream termsStream,
                    List<Double> weights,
                    String outcome,
                    int positiveLabel,
                    double threshold,
                    int maxIterations,
                    int iteration) throws IOException {
    this.zkHost = zkHost;
    this.collection = collectionName;
    this.params = params;
    this.name = name;
    this.field = feature;
    this.termsStream = termsStream;
    this.outcome = outcome;
    this.positiveLabel = positiveLabel;
    this.threshold = threshold;
    this.weights = weights;
    this.maxIterations = maxIterations;
    this.iteration = iteration;
  }

  public void setStreamContext(StreamContext context) {
    this.cache = context.getSolrClientCache();
    this.streamContext = context;
    this.termsStream.setStreamContext(context);
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
    this.executorService = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrjNamedThreadFactory("TextLogitSolrStream"));
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList();
    l.add(termsStream);
    return l;
  }

  protected List<String> getShardUrls() throws IOException {
    try {
      ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();

      Collection<Slice> slices = CloudSolrStream.getSlices(this.collection, zkStateReader, false);

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

  private List<Future<Tuple>> callShards(List<String> baseUrls) throws IOException {

    List<Future<Tuple>> futures = new ArrayList();
    for (String baseUrl : baseUrls) {
      LogitCall lc = new LogitCall(baseUrl,
          this.params,
          this.field,
          this.terms,
          this.weights,
          this.outcome,
          this.positiveLabel,
          this.learningRate,
          this.iteration);

      Future<Tuple> future = executorService.submit(lc);
      futures.add(future);
    }

    return futures;
  }

  public void close() throws IOException {
    if (isCloseCache) {
      cache.close();
    }

    executorService.shutdown();
    termsStream.close();
  }

  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return null;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(Explanation.ExpressionType.MACHINE_LEARNING_MODEL);
    explanation.setExpression(toExpression(factory).toString());

    explanation.addChild(termsStream.toExplanation(factory));

    return explanation;
  }

  public void loadTerms() throws IOException {
    if (this.terms == null) {
      termsStream.open();
      this.terms = new ArrayList<>();
      this.idfs = new ArrayList();

      while (true) {
        Tuple termTuple = termsStream.read();
        if (termTuple.EOF) {
          break;
        } else {
          terms.add(termTuple.getString("term_s"));
          idfs.add(termTuple.getDouble("idf_d"));
        }
      }
      termsStream.close();
    }
  }

  public Tuple read() throws IOException {
    try {

      if(++iteration > maxIterations) {
        Map map = new HashMap();
        map.put("EOF", true);
        return new Tuple(map);
      } else {

        if (this.idfs == null) {
          loadTerms();

          if (weights != null && terms.size() + 1 != weights.size()) {
            throw new IOException(String.format(Locale.ROOT,"invalid expression %s - the number of weights must be %d, found %d", terms.size()+1, weights.size()));
          }
        }

        List<List<Double>> allWeights = new ArrayList();
        this.evaluation = new ClassificationEvaluation();

        this.error = 0;
        for (Future<Tuple> logitCall : callShards(getShardUrls())) {

          Tuple tuple = logitCall.get();
          List<Double> shardWeights = (List<Double>) tuple.get("weights");
          allWeights.add(shardWeights);
          this.error += tuple.getDouble("error");
          Map shardEvaluation = (Map) tuple.get("evaluation");
          this.evaluation.addEvaluation(shardEvaluation);
        }

        this.weights = averageWeights(allWeights);
        Map map = new HashMap();
        map.put("id", name+"_"+iteration);
        map.put("name_s", name);
        map.put("field_s", field);
        map.put("terms_ss", terms);
        map.put("iteration_i", iteration);

        if(weights != null) {
          map.put("weights_ds", weights);
        }

        map.put("error_d", error);
        evaluation.putToMap(map);
        map.put("alpha_d", this.learningRate);
        map.put("idfs_ds", this.idfs);

        if (iteration != 1) {
          if (lastError <= error) {
            this.learningRate *= 0.5;
          } else {
            this.learningRate *= 1.05;
          }
        }

        lastError = error;

        return new Tuple(map);
      }

    } catch(Exception e) {
      throw new IOException(e);
    }
  }

  private List<Double> averageWeights(List<List<Double>> allWeights) {
    double[] working = new double[allWeights.get(0).size()];
    for(List<Double> shardWeights: allWeights) {
      for(int i=0; i<working.length; i++) {
        working[i] += shardWeights.get(i);
      }
    }

    for(int i=0; i<working.length; i++) {
      working[i] = working[i] / allWeights.size();
    }

    List<Double> ave = new ArrayList();
    for(double d : working) {
      ave.add(d);
    }

    return ave;
  }

  static String toString(List items) {
    StringBuilder buf = new StringBuilder();
    for(Object item : items) {
      if(buf.length() > 0) {
        buf.append(",");
      }

      buf.append(item.toString());
    }

    return buf.toString();
  }

  protected class TermsStream extends TupleStream {

    private List<String> terms;
    private Iterator<String> it;

    public TermsStream(List<String> terms) {
      this.terms = terms;
    }

    @Override
    public void setStreamContext(StreamContext context) {}

    @Override
    public List<TupleStream> children() { return new ArrayList<>(); }

    @Override
    public void open() throws IOException { this.it = this.terms.iterator();}

    @Override
    public void close() throws IOException {}

    @Override
    public Tuple read() throws IOException {
      HashMap map = new HashMap();
      if(it.hasNext()) {
        map.put("term_s",it.next());
        map.put("score_f",1.0);
        return new Tuple(map);
      } else {
        map.put("EOF", true);
        return new Tuple(map);
      }
    }

    @Override
    public StreamComparator getStreamSort() {return null;}

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {
      return new StreamExplanation(getStreamNodeId().toString())
          .withFunctionName("non-expressible")
          .withImplementingClass(this.getClass().getName())
          .withExpressionType(Explanation.ExpressionType.STREAM_SOURCE)
          .withExpression("non-expressible");
    }
  }

  protected class LogitCall implements Callable<Tuple> {

    private String baseUrl;
    private String feature;
    private List<String> terms;
    private List<Double> weights;
    private int iteration;
    private String outcome;
    private int positiveLabel;
    private double learningRate;
    private Map<String, String> paramsMap;

    public LogitCall(String baseUrl,
                     Map<String, String> paramsMap,
                     String feature,
                     List<String> terms,
                     List<Double> weights,
                     String outcome,
                     int positiveLabel,
                     double learningRate,
                     int iteration) {

      this.baseUrl = baseUrl;
      this.feature = feature;
      this.terms = terms;
      this.weights = weights;
      this.iteration = iteration;
      this.outcome = outcome;
      this.positiveLabel = positiveLabel;
      this.learningRate = learningRate;
      this.paramsMap = paramsMap;
    }

    public Tuple call() throws Exception {
      ModifiableSolrParams params = new ModifiableSolrParams();
      HttpSolrClient solrClient = cache.getHttpSolrClient(baseUrl);

      params.add("distrib", "false");
      params.add("fq","{!tlogit}");
      params.add("feature", feature);
      params.add("terms", TextLogitStream.toString(terms));
      params.add("idfs", TextLogitStream.toString(idfs));

      for(String key : paramsMap.keySet()) {
        params.add(key, paramsMap.get(key));
      }

      if(weights != null) {
        params.add("weights", TextLogitStream.toString(weights));
      }

      params.add("iteration", Integer.toString(iteration));
      params.add("outcome", outcome);
      params.add("positiveLabel", Integer.toString(positiveLabel));
      params.add("threshold", Double.toString(threshold));
      params.add("alpha", Double.toString(learningRate));

      QueryRequest  request= new QueryRequest(params, SolrRequest.METHOD.POST);
      QueryResponse response = request.process(solrClient);
      NamedList res = response.getResponse();

      NamedList logit = (NamedList)res.get("logit");

      List<Double> shardWeights = (List<Double>)logit.get("weights");
      double shardError = (double)logit.get("error");

      Map map = new HashMap();

      map.put("error", shardError);
      map.put("weights", shardWeights);
      map.put("evaluation", logit.get("evaluation"));

      return new Tuple(map);
    }
  }
}
