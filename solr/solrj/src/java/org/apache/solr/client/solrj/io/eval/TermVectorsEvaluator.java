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

package org.apache.solr.client.solrj.io.eval;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class TermVectorsEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  private int minTermLength = 3;
  private double minDocFreq = .05; // 5% of the docs min
  private double maxDocFreq = .5;  // 50% of the docs max
  private String[] excludes;

  public TermVectorsEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);

    for (StreamExpressionNamedParameter namedParam : namedParams) {
      if (namedParam.getName().equals("minTermLength")) {
        this.minTermLength = Integer.parseInt(namedParam.getParameter().toString().trim());
      } else if (namedParam.getName().equals("minDocFreq")) {
        this.minDocFreq = Double.parseDouble(namedParam.getParameter().toString().trim());
        if (minDocFreq < 0 || minDocFreq > 1) {
          throw new IOException("Doc frequency percentage must be between 0 and 1");
        }
      } else if (namedParam.getName().equals("maxDocFreq")) {
        this.maxDocFreq = Double.parseDouble(namedParam.getParameter().toString().trim());
        if (maxDocFreq < 0 || maxDocFreq > 1) {
          throw new IOException("Doc frequency percentage must be between 0 and 1");
        }
      } else if(namedParam.getName().equals("exclude")) {
        this.excludes = namedParam.getParameter().toString().split(",");
      } else {
        throw new IOException("Unexpected named parameter:" + namedParam.getName());
      }
    }
  }

  @Override
  public Object doWork(Object... objects) throws IOException {

    if (objects.length == 1) {
      //Just docs
      if(!(objects[0] instanceof List)) {
        throw new IOException("The termVectors function expects a list of Tuples as a parameter.");
      } else {
        @SuppressWarnings({"rawtypes"})
        List list = (List)objects[0];
        if(list.size() > 0) {
          Object o = list.get(0);
          if(!(o instanceof Tuple)) {
            throw new IOException("The termVectors function expects a list of Tuples as a parameter.");
          }
        } else {
          throw new IOException("Empty list was passed as a parameter to termVectors function.");
        }
      }

      @SuppressWarnings({"unchecked"})
      List<Tuple> tuples = (List<Tuple>) objects[0];
      TreeMap<String, Integer> docFreqs = new TreeMap<>();
      List<String> rowLabels = new ArrayList<>();

      for (Tuple tuple : tuples) {

        Set<String> docTerms = new HashSet<>();

        if (tuple.get("terms") == null) {
          throw new IOException("The document tuples must contain a terms field");
        }

        @SuppressWarnings({"unchecked"})
        List<String> terms = (List<String>) tuple.get("terms");

        String id = tuple.getString("id");
        rowLabels.add(id);

        OUTER:
        for (String term : terms) {

          if (term.length() < minTermLength) {
            //Eliminate terms due to length
            continue;
          }

          if(excludes != null) {
            for (String exclude : excludes) {
              if (term.indexOf(exclude) > -1) {
                continue OUTER;
              }
            }
          }

          if (!docTerms.contains(term)) {
            docTerms.add(term);
            if (docFreqs.containsKey(term)) {
              int count = docFreqs.get(term).intValue();
              docFreqs.put(term, ++count);
            } else {
              docFreqs.put(term, 1);
            }
          }
        }
      }

      //Eliminate terms based on frequency

      int min = (int) (tuples.size() * minDocFreq);
      int max = (int) (tuples.size() * maxDocFreq);

      Set<Map.Entry<String, Integer>> entries = docFreqs.entrySet();
      Iterator<Map.Entry<String, Integer>> it = entries.iterator();
      while (it.hasNext()) {
        Map.Entry<String, Integer> entry = it.next();
        int count = entry.getValue().intValue();

        if (count < min || count > max) {
          it.remove();
        }
      }
      int totalTerms = docFreqs.size();
      Set<String> keys = docFreqs.keySet();
      List<String> features = new ArrayList<>(keys);
      double[][] docVec = new double[tuples.size()][];
      for (int t = 0; t < tuples.size(); t++) {
        Tuple tuple = tuples.get(t);
        @SuppressWarnings({"unchecked"})
        List<String> terms = (List<String>) tuple.get("terms");
        Map<String, Integer> termFreq = new HashMap<>();

        for (String term : terms) {
          if (docFreqs.containsKey(term)) {
            if (termFreq.containsKey(term)) {
              int count = termFreq.get(term).intValue();
              termFreq.put(term, ++count);
            } else {
              termFreq.put(term, 1);
            }
          }
        }

        double[] termVec = new double[totalTerms];
        for (int i = 0; i < totalTerms; i++) {
          String feature = features.get(i);
          int df = docFreqs.get(feature);
          int tf = termFreq.containsKey(feature) ? termFreq.get(feature) : 0;
          termVec[i] = Math.sqrt(tf) * (Math.log((tuples.size() + 1) / (double) (df + 1)) + 1.0);
        }
        docVec[t] = termVec;
      }

      Matrix matrix = new Matrix(docVec);
      matrix.setColumnLabels(features);
      matrix.setRowLabels(rowLabels);
      matrix.setAttribute("docFreqs", docFreqs);
      return matrix;
    } else {
      throw new IOException("The termVectors function takes a single positional parameter.");
    }
  }
}
