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

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;

/** Parse a QualityQuery into a Lucene query. */
public interface QualityQueryParser {

  /**
   * Parse a given QualityQuery into a Lucene query.
   *
   * @param qq the quality query to be parsed.
   * @throws ParseException if parsing failed.
   */
  public Query parse(QualityQuery qq) throws ParseException;
}
