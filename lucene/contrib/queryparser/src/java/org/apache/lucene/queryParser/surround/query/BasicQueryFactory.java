package org.apache.lucene.queryParser.surround.query;
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

/* Create basic queries to be used during rewrite.
 * The basic queries are TermQuery and SpanTermQuery.
 * An exception can be thrown when too many of these are used.
 * SpanTermQuery and TermQuery use IndexReader.termEnum(Term), which causes the buffer usage.
 *
 * Use this class to limit the buffer usage for reading terms from an index.
 * Default is 1024, the same as the max. number of subqueries for a BooleanQuery.
 */
 
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanTermQuery;

public class BasicQueryFactory {
  public BasicQueryFactory(int maxBasicQueries) {
    this.maxBasicQueries = maxBasicQueries;
    this.queriesMade = 0;
  }
  
  public BasicQueryFactory() {
    this(1024);
  }
  
  private int maxBasicQueries;
  private int queriesMade;
  
  public int getNrQueriesMade() {return queriesMade;}
  public int getMaxBasicQueries() {return maxBasicQueries;}
  
  private synchronized void checkMax() throws TooManyBasicQueries {
    if (queriesMade >= maxBasicQueries)
      throw new TooManyBasicQueries(getMaxBasicQueries());
    queriesMade++;
  }
  
  public TermQuery newTermQuery(Term term) throws TooManyBasicQueries {
    checkMax();
    return new TermQuery(term);
  }
  
  public SpanTermQuery newSpanTermQuery(Term term) throws TooManyBasicQueries {
    checkMax();
    return new SpanTermQuery(term);
  }
}


