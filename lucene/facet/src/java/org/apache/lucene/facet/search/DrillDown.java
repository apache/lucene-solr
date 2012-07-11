package org.apache.lucene.facet.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;

import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;

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

/**
 * Creation of drill down term or query.
 * 
 * @lucene.experimental
 */
public final class DrillDown {

  /**
   * @see #term(FacetIndexingParams, CategoryPath)
   */
  public static final Term term(FacetSearchParams sParams, CategoryPath path) {
    return term(sParams.getFacetIndexingParams(), path);
  }

  /**
   * Return a term for drilling down into a category.
   */
  public static final Term term(FacetIndexingParams iParams, CategoryPath path) {
    CategoryListParams clp = iParams.getCategoryListParams(path);
    char[] buffer = new char[path.charsNeededForFullPath()];
    iParams.drillDownTermText(path, buffer);
    return new Term(clp.getTerm().field(), String.valueOf(buffer));
  }
  
  /**
   * Return a query for drilling down into all given categories (AND).
   * @see #term(FacetSearchParams, CategoryPath)
   * @see #query(FacetSearchParams, Query, CategoryPath...)
   */
  public static final Query query(FacetIndexingParams iParams, CategoryPath... paths) {
    if (paths==null || paths.length==0) {
      throw new IllegalArgumentException("Empty category path not allowed for drill down query!");
    }
    if (paths.length==1) {
      return new TermQuery(term(iParams, paths[0]));
    }
    BooleanQuery res = new BooleanQuery();
    for (CategoryPath cp : paths) {
      res.add(new TermQuery(term(iParams, cp)), Occur.MUST);
    }
    return res;
  }
  
  /**
   * Return a query for drilling down into all given categories (AND).
   * @see #term(FacetSearchParams, CategoryPath)
   * @see #query(FacetSearchParams, Query, CategoryPath...)
   */
  public static final Query query(FacetSearchParams sParams, CategoryPath... paths) {
    return query(sParams.getFacetIndexingParams(), paths);
  }

  /**
   * Turn a base query into a drilling-down query for all given category paths (AND).
   * @see #query(FacetIndexingParams, CategoryPath...)
   */
  public static final Query query(FacetIndexingParams iParams, Query baseQuery, CategoryPath... paths) {
    BooleanQuery res = new BooleanQuery();
    res.add(baseQuery, Occur.MUST);
    res.add(query(iParams, paths), Occur.MUST);
    return res;
  }

  /**
   * Turn a base query into a drilling-down query for all given category paths (AND).
   * @see #query(FacetSearchParams, CategoryPath...)
   */
  public static final Query query(FacetSearchParams sParams, Query baseQuery, CategoryPath... paths) {
    return query(sParams.getFacetIndexingParams(), baseQuery, paths);
  }

  /**
   * Turn a base query into a drilling-down query using the default {@link FacetSearchParams}  
   * @see #query(FacetSearchParams, Query, CategoryPath...)
   */
  public static final Query query(Query baseQuery, CategoryPath... paths) {
    return query(new FacetSearchParams(), baseQuery, paths);
  }

}
