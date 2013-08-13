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
package org.apache.lucene.benchmark.quality;

import java.util.Map;

/**
 * A QualityQuery has an ID and some name-value pairs.
 * <p> 
 * The ID allows to map the quality query with its judgements.
 * <p>
 * The name-value pairs are used by a 
 * {@link org.apache.lucene.benchmark.quality.QualityQueryParser}
 * to create a Lucene {@link org.apache.lucene.search.Query}.
 * <p>
 * It is very likely that name-value-pairs would be mapped into fields in a Lucene query,
 * but it is up to the QualityQueryParser how to map - e.g. all values in a single field, 
 * or each pair as its own field, etc., - and this of course must match the way the 
 * searched index was constructed.
 */
public class QualityQuery implements Comparable<QualityQuery> {
  private String queryID;
  private Map<String,String> nameValPairs;

  /**
   * Create a QualityQuery with given ID and name-value pairs.
   * @param queryID ID of this quality query.
   * @param nameValPairs the contents of this quality query.
   */
  public QualityQuery(String queryID, Map<String,String> nameValPairs) {
    this.queryID = queryID;
    this.nameValPairs = nameValPairs;
  }
  
  /**
   * Return all the names of name-value-pairs in this QualityQuery.
   */
  public String[] getNames() {
    return nameValPairs.keySet().toArray(new String[0]);
  }

  /**
   * Return the value of a certain name-value pair.
   * @param name the name whose value should be returned. 
   */
  public String getValue(String name) {
    return nameValPairs.get(name);
  }

  /**
   * Return the ID of this query.
   * The ID allows to map the quality query with its judgements.
   */
  public String getQueryID() {
    return queryID;
  }

  /* for a nicer sort of input queries before running them.
   * Try first as ints, fall back to string if not int. */ 
  @Override
  public int compareTo(QualityQuery other) {
    try {
      // compare as ints when ids ints
      int n = Integer.parseInt(queryID);
      int nOther = Integer.parseInt(other.queryID);
      return n - nOther;
    } catch (NumberFormatException e) {
      // fall back to string comparison
      return queryID.compareTo(other.queryID);
    }
  }
  
}
