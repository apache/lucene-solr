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

package org.apache.lucene.luke.models.search;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/**
 * A dedicated interface for Luke's Search tab.
 */
public interface Search {

  /**
   * Returns all field names in this index.
   */
  Collection<String> getFieldNames();

  /**
   * Returns field names those are sortable.
   */
  Collection<String> getSortableFieldNames();

  /**
   * Returns field names those are searchable.
   */
  Collection<String> getSearchableFieldNames();

  /**
   * Returns field names those are searchable by range query.
   */
  Collection<String> getRangeSearchableFieldNames();

  /**
   * Returns the current query.
   */
  Query getCurrentQuery();

  /**
   * Parses the specified query expression with given configurations.
   *
   * @param expression - query expression
   * @param defField - default field for the query
   * @param analyzer - analyzer for parsing query expression
   * @param config - query parser configuration
   * @param rewrite - if true, re-written query is returned
   * @return parsed query
   * @throws LukeException - if an internal error occurs when accessing index
   */
  Query parseQuery(String expression, String defField, Analyzer analyzer, QueryParserConfig config, boolean rewrite);

  /**
   * Creates the MoreLikeThis query for the specified document with given configurations.
   *
   * @param docid - document id
   * @param mltConfig - MoreLikeThis configuration
   * @param analyzer - analyzer for analyzing the document
   * @return MoreLikeThis query
   * @throws LukeException - if an internal error occurs when accessing index
   */
  Query mltQuery(int docid, MLTConfig mltConfig, Analyzer analyzer);

  /**
   * Searches this index by the query with given configurations.
   *
   * @param query - search query
   * @param simConfig - similarity configuration
   * @param fieldsToLoad - field names to load
   * @param pageSize - page size
   * @param exactHitsCount - if set to true, the exact total hits count is returned.
   * @return search results
   * @throws LukeException - if an internal error occurs when accessing index
   */
  SearchResults search(Query query, SimilarityConfig simConfig, Set<String> fieldsToLoad, int pageSize, boolean exactHitsCount);

  /**
   * Searches this index by the query with given sort criteria and configurations.
   *
   * @param query - search query
   * @param simConfig - similarity configuration
   * @param sort - sort criteria
   * @param fieldsToLoad - fields to load
   * @param pageSize - page size
   * @param exactHitsCount - if set to true, the exact total hits count is returned.
   * @return search results
   * @throws LukeException - if an internal error occurs when accessing index
   */
  SearchResults search(Query query, SimilarityConfig simConfig, Sort sort, Set<String> fieldsToLoad, int pageSize, boolean exactHitsCount);

  /**
   * Returns the next page for the current query.
   *
   * @return search results, or empty if there are no more results
   * @throws LukeException - if an internal error occurs when accessing index
   */
  Optional<SearchResults> nextPage();

  /**
   * Returns the previous page for the current query.
   *
   * @return search results, or empty if there are no more results.
   * @throws LukeException - if an internal error occurs when accessing index
   */
  Optional<SearchResults> prevPage();

  /**
   * Explains the document for the specified query.
   *
   * @param query - query
   * @param docid - document id to be explained
   * @return explanations
   * @throws LukeException - if an internal error occurs when accessing index
   */
  Explanation explain(Query query, int docid);

  /**
   * Returns possible {@link SortField}s for the specified field.
   *
   * @param name - field name
   * @return list of possible sort types
   * @throws LukeException - if an internal error occurs when accessing index
   */
  List<SortField> guessSortTypes(String name);

  /**
   * Returns the {@link SortField} for the specified field with the sort type and order.
   *
   * @param name - field name
   * @param type - string representation for a type
   * @param reverse - if true, descending order is used
   * @return sort type, or empty if the type is incompatible with the field
   * @throws LukeException - if an internal error occurs when accessing index
   */
  Optional<SortField> getSortType(String name, String type, boolean reverse);
}
