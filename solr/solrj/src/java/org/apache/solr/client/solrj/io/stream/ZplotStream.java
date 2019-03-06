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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.random.EmpiricalDistribution;
import org.apache.commons.math3.stat.Frequency;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.util.Precision;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class ZplotStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;
  private StreamContext streamContext;
  private Map letParams = new LinkedHashMap();
  private Iterator<Tuple> out;

  public ZplotStream(StreamExpression expression, StreamFactory factory) throws IOException {

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    //Get all the named params

    for(StreamExpressionParameter np : namedParams) {
      String name = ((StreamExpressionNamedParameter)np).getName();
      StreamExpressionParameter param = ((StreamExpressionNamedParameter)np).getParameter();
      if(param instanceof StreamExpressionValue) {
        String paramValue = ((StreamExpressionValue) param).getValue();
        letParams.put(name, factory.constructPrimitiveObject(paramValue));
      } else if(factory.isEvaluator((StreamExpression)param)) {
        StreamEvaluator evaluator = factory.constructEvaluator((StreamExpression) param);
        letParams.put(name, evaluator);
      }
    }
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_DECORATOR);
    explanation.setExpression(toExpression(factory, false).toString());

    return explanation;
  }

  public void setStreamContext(StreamContext context) {
    this.streamContext = context;
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<TupleStream>();
    return l;
  }

  public Tuple read() throws IOException {
    if(out.hasNext()) {
      return out.next();
    } else {
      Map m = new HashMap();
      m.put("EOF", true);
      Tuple t = new Tuple(m);
      return t;
    }
  }

  public void close() throws IOException {
  }

  public void open() throws IOException {
    Map<String, Object> lets = streamContext.getLets();
    Set<Map.Entry<String, Object>> entries = letParams.entrySet();
    Map<String, Object> evaluated = new HashMap();

    //Load up the StreamContext with the data created by the letParams.
    int numTuples = -1;
    int columns = 0;
    boolean table = false;
    boolean distribution = false;
    for(Map.Entry<String, Object> entry : entries) {
      ++columns;

      String name = entry.getKey();
      if(name.equals("table")) {
        table = true;
      } else if(name.equals("dist")) {
        distribution = true;
      }

      Object o = entry.getValue();
      if(o instanceof StreamEvaluator) {
        Tuple eTuple = new Tuple(lets);
        StreamEvaluator evaluator = (StreamEvaluator)o;
        evaluator.setStreamContext(streamContext);
        Object eo = evaluator.evaluate(eTuple);
        if(eo instanceof List) {
          List l = (List)eo;
          if(numTuples == -1) {
            numTuples = l.size();
          } else {
            if(l.size() != numTuples) {
              throw new IOException("All lists provided to the zplot function must be the same length.");
            }
          }
          evaluated.put(name, l);
        } else if (eo instanceof Tuple) {
          evaluated.put(name, eo);
        } else {
          evaluated.put(name, eo);
        }
      } else {
        Object eval = lets.get(o);
        if(eval instanceof List) {
          List l = (List)eval;
          if(numTuples == -1) {
            numTuples = l.size();
          } else {
            if(l.size() != numTuples) {
              throw new IOException("All lists provided to the zplot function must be the same length.");
            }
          }
          evaluated.put(name, l);
        } else if(eval instanceof Tuple) {
          evaluated.put(name, eval);
        }
      }
    }

    if(columns > 1 && (table || distribution)) {
      throw new IOException("If the table or dist parameter is set there can only be one parameter.");
    }
    //Load the values into tuples

    List<Tuple> outTuples = new ArrayList();
    if(!table && !distribution) {
      //Handle the vectors
      for (int i = 0; i < numTuples; i++) {
        Tuple tuple = new Tuple(new HashMap());
        for (String key : evaluated.keySet()) {
          List l = (List) evaluated.get(key);
          tuple.put(key, l.get(i));
        }

        outTuples.add(tuple);
      }
    } else if(distribution) {
      Object o = evaluated.get("dist");
      if(o instanceof RealDistribution) {
        RealDistribution realDistribution = (RealDistribution) o;
        List<SummaryStatistics> binStats = null;
        if(realDistribution instanceof  EmpiricalDistribution) {
          EmpiricalDistribution empiricalDistribution = (EmpiricalDistribution)realDistribution;
          binStats = empiricalDistribution.getBinStats();
        } else {
          double[] samples = realDistribution.sample(500000);
          EmpiricalDistribution empiricalDistribution = new EmpiricalDistribution(32);
          empiricalDistribution.load(samples);
          binStats = empiricalDistribution.getBinStats();
        }
        double[] x = new double[binStats.size()];
        double[] y = new double[binStats.size()];
        for (int i = 0; i < binStats.size(); i++) {
          x[i] = binStats.get(i).getMean();
          y[i] = realDistribution.density(x[i]);
        }

        for (int i = 0; i < x.length; i++) {
          Tuple tuple = new Tuple(new HashMap());
          if(!Double.isNaN(x[i])) {
            tuple.put("x", Precision.round(x[i], 2));
            if(y[i] == Double.NEGATIVE_INFINITY || y[i] == Double.POSITIVE_INFINITY) {
              tuple.put("y", 0);

            } else {
              tuple.put("y", y[i]);
            }
            outTuples.add(tuple);
          }
        }
      } else if(o instanceof IntegerDistribution) {
        IntegerDistribution integerDistribution = (IntegerDistribution)o;
        int[] samples = integerDistribution.sample(50000);
        Frequency frequency = new Frequency();
        for(int i : samples) {
          frequency.addValue(i);
        }

        Iterator it = frequency.valuesIterator();
        List<Long> values = new ArrayList();
        while(it.hasNext()) {
          values.add((Long)it.next());
        }
        System.out.println(values);
        int[] x = new int[values.size()];
        double[] y = new double[values.size()];
        for(int i=0; i<values.size(); i++) {
          x[i] = values.get(i).intValue();
          y[i] = integerDistribution.probability(x[i]);
        }

        for (int i = 0; i < x.length; i++) {
          Tuple tuple = new Tuple(new HashMap());
          tuple.put("x", x[i]);
          tuple.put("y", y[i]);
          outTuples.add(tuple);
        }
      } else if(o instanceof List) {
        List list = (List)o;
        if(list.get(0) instanceof Tuple) {
          List<Tuple> tlist = (List<Tuple>)o;
          Tuple tuple = tlist.get(0);
          if(tuple.fields.containsKey("N")) {
            for(Tuple t : tlist) {
              Tuple outtuple = new Tuple(new HashMap());
              outtuple.put("x", Precision.round(((double)t.get("mean")), 2));
              outtuple.put("y", t.get("prob"));
              outTuples.add(outtuple);
            }
          } else if(tuple.fields.containsKey("count")) {
            for(Tuple t : tlist) {
              Tuple outtuple = new Tuple(new HashMap());
              outtuple.put("x", t.get("value"));
              outtuple.put("y", t.get("pct"));
              outTuples.add(outtuple);
            }
          }
        }
      }
    } else if(table){
      //Handle the Tuple and List of Tuples
      Object o = evaluated.get("table");
      if(o instanceof List) {
        List<Tuple> tuples = (List<Tuple>)o;
        outTuples.addAll(tuples);
      } else if(o instanceof Tuple) {
        outTuples.add((Tuple)o);
      }
    }

    this.out = outTuples.iterator();
  }

  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return null;
  }

  public int getCost() {
    return 0;
  }


}
