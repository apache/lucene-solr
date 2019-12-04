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
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class SolrLogStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;
  private String currentFile;
  private int lineNumber;
  private Pattern handler;

  protected TupleStream originalStream;

  public SolrLogStream(StreamExpression expression,StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);

    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }

    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }

    init(factory.constructStream(streamExpressions.get(0)));
  }

  private void init(TupleStream stream) throws IOException{
    handler = Pattern.compile("collection=c:([A-Za-z]),");
    this.originalStream = stream;
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    if(includeStreams){
      // streams
      if(originalStream instanceof Expressible){
        expression.addParameter(((Expressible)originalStream).toExpression(factory));
      }
      else{
        throw new IOException("This CsvStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }
    else{
      expression.addParameter("<stream>");
    }

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    return new StreamExplanation(getStreamNodeId().toString())
        .withChildren(new Explanation[] {
            originalStream.toExplanation(factory)
            // we're not including that this is wrapped with a ReducerStream stream because that's just an implementation detail
        })
        .withFunctionName(factory.getFunctionName(this.getClass()))
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(ExpressionType.STREAM_DECORATOR)
        .withExpression(toExpression(factory, false).toString());
  }

  public void setStreamContext(StreamContext context) {
    this.originalStream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<TupleStream>();
    l.add(originalStream);
    return l;
  }

  public void open() throws IOException {
    originalStream.open();
  }

  public void close() throws IOException {
    originalStream.close();
  }

  public Tuple read() throws IOException {
    while(true) {
      Tuple tuple = originalStream.read();
      ++lineNumber;
      if (tuple.EOF) {
        return tuple;
      } else {
        String file = formatFile(tuple.getString("file"));
        String line = tuple.getString("line");
        if (!file.equals(currentFile)) {
          this.currentFile = file;
          this.lineNumber = 1; //New file so reset the lineNumber
        }

        Tuple t = null;

        if (line.contains("QTime=")) {
          t = parseQueryRecord(line);
        } else if(line.contains("Registered new searcher")) {
          t = parseNewSearch(line);
        } else if(line.contains("path=/update")) {
          t = parseUpdate(line);
        } else {
          continue;
        }

        t.put("id", this.currentFile + "_" + this.lineNumber);
        t.put("file_s", this.currentFile);
        t.put("line_number_l", this.lineNumber);

        return t;
      }
    }
  }


  private String formatFile(String file) {
    //We don't want the ./ which carries no information but can lead to problems in creating the id for the field.
    if(file.startsWith("./")) {
      return file.substring(2);
    } else {
      return file;
    }
  }

  private Tuple parseQueryRecord(String line) {

    String[] parts = line.split("\\s+");
    Tuple tuple = new Tuple();
    tuple.put("date_dt", parts[0]);
    String qtime = parts[parts.length-1];
    tuple.put("qtime_i", qtime.split("=")[1]);

    String status = parts[parts.length-2];
    tuple.put("status_s", status.split("=")[1]);

    if(line.contains("hits=")) {
      String hits = parts[parts.length - 3];
      tuple.put("hits_l", hits.split("=")[1]);

      String params = parts[parts.length-4];
      tuple.put("params_t", params.substring(7));
      addParams(tuple, params);

    }


    String ll = parts[2];
    tuple.put("log_level_s", ll);

    if(line.contains("distrib=false")) {
      tuple.put("distrib_s", "false");
    } else {
      tuple.put("distrib_s", "true");

    }

    if(line.contains("facet=true")) {
      tuple.put("facet_s", "true");
    }

    tuple.put("collection_s", parseCollection(line));
    tuple.put("core_s", parseCore(line));
    tuple.put("node_s", parseNode(line));

    return tuple;
  }

  private Tuple parseNewSearch(String line) {

    String[] parts = line.split("\\s+");
    Tuple tuple = new Tuple();
    tuple.put("date_dt", parts[0]);
    tuple.put("core_s", parseNewSearcherCollection(line));
    tuple.put("new_searcher", "true");

    return tuple;
  }

  private String parseCollection(String line) {
    char[] ca = {',', '}'};
    String parts[] = line.split("collection=c:");
    if(parts.length == 2) {
      return readUntil(parts[1], ca);
    } else {
      return null;
    }
  }

  private Tuple parseUpdate(String line) {
    String[] parts = line.split("\\s+");
    Tuple tuple = new Tuple();
    tuple.put("date_dt", parts[0]);
    tuple.put("update_s", "true");
    tuple.put("collection_s", parseCollection(line));
    tuple.put("core_s", parseCore(line));
    tuple.put("node_s", parseNode(line));
    return tuple;
  }

  private String parseNewSearcherCollection(String line) {
    char[] ca = {']'};
    String parts[] = line.split("\\[");
    if(parts.length > 3) {
      return readUntil(parts[2], ca);
    } else {
      return null;
    }
  }

  private String parseCore(String line) {
    char[] ca = {',', '}'};
    String parts[] = line.split("core=x:");
    if(parts.length == 2) {
      return readUntil(parts[1], ca);
    } else {
      return null;
    }
  }

  private String parseNode(String line) {
    char[] ca = {',', '}'};
    String parts[] = line.split("node_name=n:");
    if(parts.length == 2) {
      return readUntil(parts[1], ca);
    } else {
      return null;
    }
  }

  private String readUntil(String s, char[] chars) {
    StringBuilder builder = new StringBuilder();
    for(int i=0; i<s.length(); i++) {
      char a = s.charAt(i);
      for(char c : chars) {
        //System.out.println(a+":"+c);
        if(a == c) {
          return builder.toString();
        }
      }
      builder.append(a);
    }

    return builder.toString();
  }

  private void addParams(Tuple tuple,  String params) {
    params = params.substring(7, params.length()-1);
    String[] pairs = params.split("&");
    for(String pair : pairs) {
      String[] parts = pair.split("=");
      if(parts[0].equals("q")) {
        String dq = URLDecoder.decode(parts[1]);
        tuple.put("q_s", dq);
        tuple.put("q_t", dq);

      }

      if(parts[0].equals("rows")) {
        String dr = URLDecoder.decode(parts[1]);
        tuple.put("rows_i", dr);
      }
    }
  }

  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return originalStream.getStreamSort();
  }

  public int getCost() {
    return 0;
  }

}
