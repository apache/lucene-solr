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

import java.io.PrintWriter;

/**
 * Judge if a document is relevant for a quality query.
 */
public interface Judge {

  /**
   * Judge if document <code>docName</code> is relevant for the given quality query.
   * @param docName name of doc tested for relevancy.
   * @param query tested quality query. 
   * @return true if relevant, false if not.
   */
  public boolean isRelevant(String docName, QualityQuery query);

  /**
   * Validate that queries and this Judge match each other.
   * To be perfectly valid, this Judge must have some data for each and every 
   * input quality query, and must not have any data on any other quality query.  
   * <b>Note</b>: the quality benchmark run would not fail in case of imperfect
   * validity, just a warning message would be logged.  
   * @param qq quality queries to be validated.
   * @param logger if not null, validation issues are logged.
   * @return true if perfectly valid, false if not.
   */
  public boolean validateData (QualityQuery qq[], PrintWriter logger);
  
  /**
   * Return the maximal recall for the input quality query. 
   * It is the number of relevant docs this Judge "knows" for the query. 
   * @param query the query whose maximal recall is needed.
   */
  public int maxRecall (QualityQuery query);

}
