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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.InputStreamResponseParser;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/**
*  Queries a single Solr instance and maps SolrDocs to a Stream of Tuples.
* @since 5.1.0
**/

public class SolrStream extends TupleStream {

  private static final long serialVersionUID = 1;

  private String baseUrl;
  private SolrParams params;
  private int numWorkers;
  private int workerID;
  private boolean trace;
  private Map<String, String> fieldMappings;
  private transient TupleStreamParser tupleStreamParser;
  private transient HttpSolrClient client;
  private transient SolrClientCache cache;
  private String slice;
  private long checkpoint = -1;
  private CloseableHttpResponse closeableHttpResponse;
  private boolean distrib = true;
  private String user;
  private String password;

  /**
   * @param baseUrl Base URL of the stream.
   * @param params  Map&lt;String, String&gt; of parameters
   */

  public SolrStream(String baseUrl, SolrParams params) {
    this.baseUrl = baseUrl;
    this.params = params;
  }

  public void setFieldMappings(Map<String, String> fieldMappings) {
    this.fieldMappings = fieldMappings;
  }

  public List<TupleStream> children() {
    return new ArrayList();
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  public void setStreamContext(StreamContext context) {
    this.distrib = !context.isLocal();
    this.numWorkers = context.numWorkers;
    this.workerID = context.workerID;
    this.cache = context.getSolrClientCache();
  }

  public void setCredentials(String user, String password) {
    this.user = user;
    this.password = password;
  }

  /**
  * Opens the stream to a single Solr instance.
  **/

  public void open() throws IOException {
    if(cache == null) {
      client = new HttpSolrClient.Builder(baseUrl).build();
    } else {
      client = cache.getHttpSolrClient(baseUrl);
    }

    try {
      SolrParams requestParams = loadParams(params);
      if (!distrib) {
        ((ModifiableSolrParams) requestParams).add("distrib","false");
      }
      tupleStreamParser = constructParser(client, requestParams);
    } catch (Exception e) {
      throw new IOException("params " + params, e);
    }
  }

  /**
   *  Setting trace to true will include the "_CORE_" field in each Tuple emitted by the stream.
   **/

  public void setTrace(boolean trace) {
    this.trace = trace;
  }

  public void setSlice(String slice) {
    this.slice = slice;
  }

  public void setCheckpoint(long checkpoint) {
    this.checkpoint = checkpoint;
  }

  private ModifiableSolrParams loadParams(SolrParams paramsIn) throws IOException {
    ModifiableSolrParams solrParams = new ModifiableSolrParams(paramsIn);
    if (params.get("partitionKeys") != null) {
      if(!params.get("partitionKeys").equals("none") && numWorkers > 1) {
        String partitionFilter = getPartitionFilter();
        solrParams.add("fq", partitionFilter);
      }
    } else if(numWorkers > 1) {
        throw new IOException("When numWorkers > 1 partitionKeys must be set. Set partitionKeys=none to send the entire stream to each worker.");
    }

    if(checkpoint > 0) {
      solrParams.add("fq", "{!frange cost=100 incl=false l="+checkpoint+"}_version_");
    }

    return solrParams;
  }

  private String getPartitionFilter() {
    StringBuilder buf = new StringBuilder("{!hash workers=");
    buf.append(this.numWorkers);
    buf.append(" worker=");
    buf.append(this.workerID);
    buf.append("}");
    return buf.toString();
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    return new StreamExplanation(getStreamNodeId().toString())
      .withFunctionName("non-expressible")
      .withImplementingClass(this.getClass().getName())
      .withExpressionType(ExpressionType.STREAM_SOURCE)
      .withExpression("non-expressible");
  }

  /**
  *  Closes the Stream to a single Solr Instance
  * */

  public void close() throws IOException {
    if (closeableHttpResponse != null) {
      closeableHttpResponse.close();
    }
    if(cache == null) {
      client.close();
    }
  }

  /**
  * Reads a Tuple from the stream. The Stream is completed when Tuple.EOF == true.
  **/

  public Tuple read() throws IOException {
    try {
      Map fields = tupleStreamParser.next();

      if (fields == null) {
        //Return the EOF tuple.
        Map m = new HashMap();
        m.put("EOF", true);
        return new Tuple(m);
      } else {

        String msg = (String) fields.get("EXCEPTION");
        if (msg != null) {
          HandledException ioException = new HandledException(msg);
          throw ioException;
        }

        if (trace) {
          fields.put("_CORE_", this.baseUrl);
          if(slice != null) {
            fields.put("_SLICE_", slice);
          }
        }

        if (fieldMappings != null) {
          fields = mapFields(fields, fieldMappings);
        }
        return new Tuple(fields);
      }
    } catch (HandledException e) {
      throw new IOException("--> "+this.baseUrl+":"+e.getMessage());
    } catch (Exception e) {
      //The Stream source did not provide an exception in a format that the SolrStream could propagate.
      throw new IOException("--> "+this.baseUrl+": An exception has occurred on the server, refer to server log for details.", e);
    }
  }

  public void setDistrib(boolean distrib) {
    this.distrib = distrib;
  }

  public boolean getDistrib() {
    return distrib;
  }

  public static class HandledException extends IOException {
    public HandledException(String msg) {
      super(msg);
    }
  }

  /** There is no known sort applied to a SolrStream */
  public StreamComparator getStreamSort(){
    return null;
  }

  private Map mapFields(Map fields, Map<String,String> mappings) {

    Iterator<Map.Entry<String,String>> it = mappings.entrySet().iterator();
    while(it.hasNext()) {
      Map.Entry<String,String> entry = it.next();
      String mapFrom = entry.getKey();
      String mapTo = entry.getValue();
      Object o = fields.get(mapFrom);
      fields.remove(mapFrom);
      fields.put(mapTo, o);
    }

    return fields;
  }

  // temporary...
  public TupleStreamParser constructParser(SolrClient server, SolrParams requestParams) throws IOException, SolrServerException {
    String p = requestParams.get("qt");
    if (p != null) {
      ModifiableSolrParams modifiableSolrParams = (ModifiableSolrParams) requestParams;
      modifiableSolrParams.remove("qt");
      //performance optimization - remove extra whitespace by default when streaming
      modifiableSolrParams.set("indent", modifiableSolrParams.get("indent", "off"));
    }

    String wt = requestParams.get(CommonParams.WT, "json");
    QueryRequest query = new QueryRequest(requestParams);
    query.setPath(p);
    query.setResponseParser(new InputStreamResponseParser(wt));
    query.setMethod(SolrRequest.METHOD.POST);

    if(user != null && password != null) {
      query.setBasicAuthCredentials(user, password);
    }

    NamedList<Object> genericResponse = server.request(query);
    InputStream stream = (InputStream) genericResponse.get("stream");
    this.closeableHttpResponse = (CloseableHttpResponse)genericResponse.get("closeableResponse");
    if (CommonParams.JAVABIN.equals(wt)) {
      return new JavabinTupleStreamParser(stream, true);
    } else {
      InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8);
      return new JSONTupleStream(reader);
    }
  }
}
