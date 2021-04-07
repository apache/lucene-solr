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
package org.apache.solr.client.solrj;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.MoreLikeThisParams;
import org.apache.solr.common.params.StatsParams;
import org.apache.solr.common.params.TermsParams;


/**
 * This is an augmented SolrParams with get/set/add fields for common fields used
 * in the Standard and Dismax request handlers
 * 
 *
 * @since solr 1.3
 */
public class SolrQuery extends ModifiableSolrParams 
{  
  public static final String DOCID = "_docid_"; // duplicate of org.apache.solr.search.SortSpecParsing.DOCID which is not accessible from here
  
  public enum ORDER { desc, asc;
    public ORDER reverse() {
      return (this == asc) ? desc : asc;
    }
  }

  /** Maintains a map of current sorts */
  private List<SortClause> sortClauses;
  
  public SolrQuery() {
    super();
  }

  /** Create a new SolrQuery
   * 
   * @param q query string
   */
  public SolrQuery(String q) {
    this();
    this.set(CommonParams.Q, q);
  }

  public SolrQuery(String k, String v, String... params) {
    assert params.length % 2 == 0;
    this.set(k, v);
    for (int i = 0; i < params.length; i += 2) {
      this.set(params[i], params[i + 1]);
    }
  }

  /** enable/disable terms.  
   * 
   * @param b flag to indicate terms should be enabled. <br> if b==false, removes all other terms parameters
   * @return Current reference (<i>this</i>)
   */
  public SolrQuery setTerms(boolean b) {
    if (b) {
      this.set(TermsParams.TERMS, true);
    } else {
      this.remove(TermsParams.TERMS);
      this.remove(TermsParams.TERMS_FIELD);
      this.remove(TermsParams.TERMS_LOWER);
      this.remove(TermsParams.TERMS_UPPER);
      this.remove(TermsParams.TERMS_UPPER_INCLUSIVE);
      this.remove(TermsParams.TERMS_LOWER_INCLUSIVE);
      this.remove(TermsParams.TERMS_LIMIT);
      this.remove(TermsParams.TERMS_PREFIX_STR);
      this.remove(TermsParams.TERMS_MINCOUNT);
      this.remove(TermsParams.TERMS_MAXCOUNT);
      this.remove(TermsParams.TERMS_RAW);
      this.remove(TermsParams.TERMS_SORT);
      this.remove(TermsParams.TERMS_REGEXP_STR);
      this.remove(TermsParams.TERMS_REGEXP_FLAG);
    }
    return this;
  }
  
  public boolean getTerms() {
    return this.getBool(TermsParams.TERMS, false);
  }
  
  public SolrQuery addTermsField(String field) {
    this.add(TermsParams.TERMS_FIELD, field);
    return this;
  }
  
  public String[] getTermsFields() {
    return this.getParams(TermsParams.TERMS_FIELD);
  }
  
  public SolrQuery setTermsLower(String lower) {
    this.set(TermsParams.TERMS_LOWER, lower);
    return this;
  }
  
  public String getTermsLower() {
    return this.get(TermsParams.TERMS_LOWER, "");
  }
  
  public SolrQuery setTermsUpper(String upper) {
    this.set(TermsParams.TERMS_UPPER, upper);
    return this;
  }
  
  public String getTermsUpper() {
    return this.get(TermsParams.TERMS_UPPER, "");
  }
  
  public SolrQuery setTermsUpperInclusive(boolean b) {
    this.set(TermsParams.TERMS_UPPER_INCLUSIVE, b);
    return this;
  }
  
  public boolean getTermsUpperInclusive() {
    return this.getBool(TermsParams.TERMS_UPPER_INCLUSIVE, false);
  }
  
  public SolrQuery setTermsLowerInclusive(boolean b) {
    this.set(TermsParams.TERMS_LOWER_INCLUSIVE, b);
    return this;
  }
  
  public boolean getTermsLowerInclusive() {
    return this.getBool(TermsParams.TERMS_LOWER_INCLUSIVE, true);
  }
 
  public SolrQuery setTermsLimit(int limit) {
    this.set(TermsParams.TERMS_LIMIT, limit);
    return this;
  }
  
  public int getTermsLimit() {
    return this.getInt(TermsParams.TERMS_LIMIT, 10);
  }
 
  public SolrQuery setTermsMinCount(int cnt) {
    this.set(TermsParams.TERMS_MINCOUNT, cnt);
    return this;
  }
  
  public int getTermsMinCount() {
    return this.getInt(TermsParams.TERMS_MINCOUNT, 1);
  }

  public SolrQuery setTermsMaxCount(int cnt) {
    this.set(TermsParams.TERMS_MAXCOUNT, cnt);
    return this;
  }
  
  public int getTermsMaxCount() {
    return this.getInt(TermsParams.TERMS_MAXCOUNT, -1);
  }
  
  public SolrQuery setTermsPrefix(String prefix) {
    this.set(TermsParams.TERMS_PREFIX_STR, prefix);
    return this;
  }
  
  public String getTermsPrefix() {
    return this.get(TermsParams.TERMS_PREFIX_STR, "");
  }
  
  public SolrQuery setTermsRaw(boolean b) {
    this.set(TermsParams.TERMS_RAW, b);
    return this;
  }
  
  public boolean getTermsRaw() {
    return this.getBool(TermsParams.TERMS_RAW, false);
  }
 
  public SolrQuery setTermsSortString(String type) {
    this.set(TermsParams.TERMS_SORT, type);
    return this;
  }
  
  public String getTermsSortString() {
    return this.get(TermsParams.TERMS_SORT, TermsParams.TERMS_SORT_COUNT);
  }

  public SolrQuery setTermsRegex(String regex)  {
    this.set(TermsParams.TERMS_REGEXP_STR, regex);
    return this;
  }

  public String getTermsRegex() {
    return this.get(TermsParams.TERMS_REGEXP_STR);
  }

  public SolrQuery setTermsRegexFlag(String flag) {
    this.add(TermsParams.TERMS_REGEXP_FLAG, flag);
    return this;
  }

  public String[] getTermsRegexFlags()  {
    return this.getParams(TermsParams.TERMS_REGEXP_FLAG);
  }
     
  /** Add field(s) for facet computation.
   * 
   * @param fields Array of field names from the IndexSchema
   * @return this
   */
  public SolrQuery addFacetField(String ... fields) {
    add(FacetParams.FACET_FIELD, fields);
    this.set(FacetParams.FACET, true);
    return this;
  }

  /** Add field(s) for pivot computation.
   * 
   * pivot fields are comma separated
   * 
   * @param fields Array of field names from the IndexSchema
   * @return this
   */
  public SolrQuery addFacetPivotField(String ... fields) {
    add(FacetParams.FACET_PIVOT, fields);
    this.set(FacetParams.FACET, true);
    return this;
  }

  /**
   * Add a numeric range facet.
   *
   * @param field The field
   * @param start The start of range
   * @param end The end of the range
   * @param gap The gap between each count
   * @return this
   */
  public SolrQuery addNumericRangeFacet(String field, Number start, Number end, Number gap) {
    add(FacetParams.FACET_RANGE, field);
    add(String.format(Locale.ROOT, "f.%s.%s", field, FacetParams.FACET_RANGE_START), start.toString());
    add(String.format(Locale.ROOT, "f.%s.%s", field, FacetParams.FACET_RANGE_END), end.toString());
    add(String.format(Locale.ROOT, "f.%s.%s", field, FacetParams.FACET_RANGE_GAP), gap.toString());
    this.set(FacetParams.FACET, true);
    return this;
  }

  /**
   * Add a numeric range facet.
   *
   * @param field The field
   * @param start The start of range
   * @param end The end of the range
   * @param gap The gap between each count
   * @return this
   */
  public SolrQuery addDateRangeFacet(String field, Date start, Date end, String gap) {
    add(FacetParams.FACET_RANGE, field);
    add(String.format(Locale.ROOT, "f.%s.%s", field, FacetParams.FACET_RANGE_START), start.toInstant().toString());
    add(String.format(Locale.ROOT, "f.%s.%s", field, FacetParams.FACET_RANGE_END),   end.toInstant().toString());
    add(String.format(Locale.ROOT, "f.%s.%s", field, FacetParams.FACET_RANGE_GAP),   gap);
    this.set(FacetParams.FACET, true);
    return this;
  }
  
  /**
   * Add Interval Faceting on a field. All intervals for the same field should be included
   * in the same call to this method.
   * For syntax documentation see <a href="https://wiki.apache.org/solr/SimpleFacetParameters#Interval_Faceting">Solr wiki</a>.
   * <br>
   * Key substitution, filter exclusions or other local params on the field are not supported when using this method, 
   * if this is needed, use the lower level {@link #add} method.<br> 
   * Key substitution IS supported on intervals when using this method.
   * 
   * 
   * @param field the field to add facet intervals. Must be an existing field and can't be null
   * @param intervals Intervals to be used for faceting. It can be an empty array, but it can't 
   * be <code>null</code>
   * @return this
   */
  public SolrQuery addIntervalFacets(String field, String[] intervals) {
    if (intervals == null) {
      throw new IllegalArgumentException("Can't add null intervals");
    }
    if (field == null) {
      throw new IllegalArgumentException("Field can't be null");
    }
    set(FacetParams.FACET, true);
    add(FacetParams.FACET_INTERVAL, field);
    for (String interval:intervals) {
      add(String.format(Locale.ROOT, "f.%s.facet.interval.set", field), interval);
    }
    return this;
  }
  
  /**
   * Remove all Interval Facets on a field
   * 
   * @param field the field to remove from facet intervals
   * @return Array of current intervals for <code>field</code>
   */
  public String[] removeIntervalFacets(String field) {
    while(remove(FacetParams.FACET_INTERVAL, field)){};
    return remove(String.format(Locale.ROOT, "f.%s.facet.interval.set", field));
  }
  
  /** get the facet fields
   * 
   * @return string array of facet fields or null if not set/empty
   */
  public String[] getFacetFields() {
    return this.getParams(FacetParams.FACET_FIELD);
  }

  /** remove a facet field
   * 
   * @param name Name of the facet field to be removed.
   * 
   * @return true, if the item was removed. <br>
   *           false, if the facet field was null or did not exist.
   */
  public boolean removeFacetField(String name) {
    boolean b = this.remove(FacetParams.FACET_FIELD, name);
    if (this.get(FacetParams.FACET_FIELD) == null && this.get(FacetParams.FACET_QUERY) == null) {
      this.setFacet(false);
    }
    return b;
  }
  
  /** enable/disable faceting.  
   * 
   * @param b flag to indicate faceting should be enabled. <br> if b==false, removes all other faceting parameters
   * @return Current reference (<i>this</i>)
   */
  public SolrQuery setFacet(boolean b) {
    if (b) {
      this.set(FacetParams.FACET, true);
    } else {
      this.remove(FacetParams.FACET);
      this.remove(FacetParams.FACET_MINCOUNT);
      this.remove(FacetParams.FACET_FIELD);
      this.remove(FacetParams.FACET_LIMIT);
      this.remove(FacetParams.FACET_MISSING);
      this.remove(FacetParams.FACET_OFFSET);
      this.remove(FacetParams.FACET_PREFIX);
      this.remove(FacetParams.FACET_QUERY);
      this.remove(FacetParams.FACET_SORT);
      this.remove(FacetParams.FACET_ZEROS);
      this.remove(FacetParams.FACET_PREFIX); // does not include the individual fields...
      this.remove(FacetParams.FACET_INTERVAL); // does not remove interval parameters
    }
    return this;
  }
  
  public SolrQuery setFacetPrefix( String prefix )
  {
    this.set( FacetParams.FACET_PREFIX, prefix );
    return this;
  }

  public SolrQuery setFacetPrefix( String field, String prefix )
  {
    this.set( "f."+field+"."+FacetParams.FACET_PREFIX, prefix );
    return this;
  }

  /** add a faceting query
   * 
   * @param f facet query
   */
  public SolrQuery addFacetQuery(String f) {
    this.add(FacetParams.FACET_QUERY, f);
    this.set(FacetParams.FACET, true);
    return this;
  }

  /** get facet queries
   * 
   * @return all facet queries or null if not set/empty
   */
  public String[] getFacetQuery() {
    return this.getParams(FacetParams.FACET_QUERY);
  }

  /** remove a facet query
   * 
   * @param q the facet query to remove
   * @return true if the facet query was removed false otherwise
   */
  public boolean removeFacetQuery(String q) {
    boolean b = this.remove(FacetParams.FACET_QUERY, q);
    if (this.get(FacetParams.FACET_FIELD) == null && this.get(FacetParams.FACET_QUERY) == null) {
      this.setFacet(false);
    }
    return b;
  }

  /** set the facet limit
   * 
   * @param lim number facet items to return
   */
  public SolrQuery setFacetLimit(int lim) {
    this.set(FacetParams.FACET_LIMIT, lim);
    return this;
  }

  /** get current facet limit
   * 
   * @return facet limit or default of 25
   */
  public int getFacetLimit() {
    return this.getInt(FacetParams.FACET_LIMIT, 25);
  }

  /** set facet minimum count
   * 
   * @param cnt facets having less that cnt hits will be excluded from teh facet list
   */
  public SolrQuery setFacetMinCount(int cnt) {
    this.set(FacetParams.FACET_MINCOUNT, cnt);
    return this;
  }

  /** get facet minimum count
   * 
   * @return facet minimum count or default of 1
   */
  public int getFacetMinCount() {
    return this.getInt(FacetParams.FACET_MINCOUNT, 1);
  }

  /**
   * Sets facet missing boolean flag 
   * 
   * @param v flag to indicate the field of  {@link FacetParams#FACET_MISSING} .
   * @return this
   */
  public SolrQuery setFacetMissing(Boolean v) {
    this.set(FacetParams.FACET_MISSING, v);
    return this;
  }

  /** get facet sort
   * 
   * @return facet sort or default of {@link FacetParams#FACET_SORT_COUNT}
   */
  public String getFacetSortString() {
    return this.get(FacetParams.FACET_SORT, FacetParams.FACET_SORT_COUNT);
  }


  /** set facet sort
   * 
   * @param sort sort facets
   * @return this
   */
  public SolrQuery setFacetSort(String sort) {
    this.set(FacetParams.FACET_SORT, sort);
    return this;
  }

  /** add highlight field
   * 
   * @param f field to enable for highlighting
   */
  public SolrQuery addHighlightField(String f) {
    this.add(HighlightParams.FIELDS, f);
    this.set(HighlightParams.HIGHLIGHT, true);
    return this;
  }

  /** remove a field for highlighting
   * 
   * @param f field name to not highlight
   * @return <i>true</i>, if removed, <br> <i>false</i>, otherwise
   */
  public boolean removeHighlightField(String f) {
    boolean b = this.remove(HighlightParams.FIELDS, f);
    if (this.get(HighlightParams.FIELDS) == null) {
      this.setHighlight(false);
    }
    return b;
  }

  /** get list of highlighted fields
   * 
   * @return Array of highlight fields or null if not set/empty
   */
  public String[] getHighlightFields() {
    return this.getParams(HighlightParams.FIELDS);
  }

  public SolrQuery setHighlightSnippets(int num) {
    this.set(HighlightParams.SNIPPETS, num);
    return this;
  }

  public int getHighlightSnippets() {
    return this.getInt(HighlightParams.SNIPPETS, 1);
  }

  public SolrQuery setHighlightFragsize(int num) {
    this.set(HighlightParams.FRAGSIZE, num);
    return this;
  }

  public int getHighlightFragsize() {
    return this.getInt(HighlightParams.FRAGSIZE, 100);
  }

  public SolrQuery setHighlightRequireFieldMatch(boolean flag) {
    this.set(HighlightParams.FIELD_MATCH, flag);
    return this;
  }

  public boolean getHighlightRequireFieldMatch() {
    return this.getBool(HighlightParams.FIELD_MATCH, false);
  }

  public SolrQuery setHighlightSimplePre(String f) {
    this.set(HighlightParams.SIMPLE_PRE, f);
    return this;
  }

  public String getHighlightSimplePre() {
    return this.get(HighlightParams.SIMPLE_PRE, "");
  }

  public SolrQuery setHighlightSimplePost(String f) {
    this.set(HighlightParams.SIMPLE_POST, f);
    return this;
  }

  public String getHighlightSimplePost() {
    return this.get(HighlightParams.SIMPLE_POST, "");
  }

  /**
   * Gets the raw sort field, as it will be sent to Solr.
   * <p>
   * The returned sort field will always contain a serialized version
   * of the sort string built using {@link #setSort(SortClause)},
   * {@link #addSort(SortClause)}, {@link #addOrUpdateSort(SortClause)},
   * {@link #removeSort(SortClause)}, {@link #clearSorts()} and 
   * {@link #setSorts(List)}.
   */
  public String getSortField() {
    return this.get(CommonParams.SORT);
  }
  
  /**
   * Clears current sort information.
   *
   * @return the modified SolrQuery object, for easy chaining
   * @since 4.2
   */
  public SolrQuery clearSorts() {
    sortClauses = null;
    serializeSorts();
    return this;
  }

  /**
   * Replaces the current sort information.
   *
   * @return the modified SolrQuery object, for easy chaining
   * @since 4.2
   */
  public SolrQuery setSorts(List<SortClause> value) {
    sortClauses = new ArrayList<>(value);
    serializeSorts();
    return this;
  }

  /**
   * Gets an a list of current sort clauses.
   *
   * @return an immutable list of current sort clauses
   * @since 4.2
   */
  public List<SortClause> getSorts() {
    if (sortClauses == null) return Collections.emptyList();
    else return Collections.unmodifiableList(sortClauses);
  }

  /**
   * Replaces the current sort information with a single sort clause
   *
   * @return the modified SolrQuery object, for easy chaining
   * @since 4.2
   */
  public SolrQuery setSort(String field, ORDER order) {
    return setSort(new SortClause(field, order));
  }

  /**
   * Replaces the current sort information with a single sort clause
   *
   * @return the modified SolrQuery object, for easy chaining
   * @since 4.2
   */
  public SolrQuery setSort(SortClause sortClause) {
    clearSorts();
    return addSort(sortClause);
  }

  /**
   * Adds a single sort clause to the end of the current sort information.
   *
   * @return the modified SolrQuery object, for easy chaining
   * @since 4.2
   */
  public SolrQuery addSort(String field, ORDER order) {
    return addSort(new SortClause(field, order));
  }

  /**
   * Adds a single sort clause to the end of the query.
   *
   * @return the modified SolrQuery object, for easy chaining
   * @since 4.2
   */
  public SolrQuery addSort(SortClause sortClause) {
    if (sortClauses == null) sortClauses = new ArrayList<>();
    sortClauses.add(sortClause);
    serializeSorts();
    return this;
  }

  /**
   * Updates or adds a single sort clause to the query.
   * If the field is already used for sorting, the order
   * of the existing field is modified; otherwise, it is
   * added to the end.
   * @return the modified SolrQuery object, for easy chaining
   * @since 4.2
   */
  public SolrQuery addOrUpdateSort(String field, ORDER order) {
    return addOrUpdateSort(new SortClause(field, order));
  }

  /**
   * Updates or adds a single sort field specification to the current sort
   * information. If the sort field already exist in the sort information map,
   * its position is unchanged and the sort order is set; if it does not exist,
   * it is appended at the end with the specified order..
   *
   * @return the modified SolrQuery object, for easy chaining
   * @since 4.2
   */
  public SolrQuery addOrUpdateSort(SortClause sortClause) {
    if (sortClauses != null) {
      for (int index=0 ; index<sortClauses.size() ; index++) {
        SortClause existing = sortClauses.get(index);
        if (existing.getItem().equals(sortClause.getItem())) {
          sortClauses.set(index, sortClause);
          serializeSorts();
          return this;
        }
      }
    }
    return addSort(sortClause);
  }

  /**
   * Removes a single sort field from the current sort information.
   *
   * @return the modified SolrQuery object, for easy chaining
   * @since 4.2
   */
  public SolrQuery removeSort(SortClause sortClause) {
    return removeSort(sortClause.getItem());
  }

  /**
   * Removes a single sort field from the current sort information.
   *
   * @return the modified SolrQuery object, for easy chaining
   * @since 4.2
   */
  public SolrQuery removeSort(String itemName) {
    if (sortClauses != null) {
      for (SortClause existing : sortClauses) {
        if (existing.getItem().equals(itemName)) {
          sortClauses.remove(existing);
          if (sortClauses.isEmpty()) sortClauses = null;
          serializeSorts();
          break;
        }
      }
    }
    return this;
  }

  private void serializeSorts() {
    if (sortClauses == null || sortClauses.isEmpty()) {
      remove(CommonParams.SORT);
    } else {
      StringBuilder sb = new StringBuilder();
      for (SortClause sortClause : sortClauses) {
        if (sb.length() > 0) sb.append(",");
        sb.append(sortClause.getItem());
        sb.append(" ");
        sb.append(sortClause.getOrder());
      }
      set(CommonParams.SORT, sb.toString());
    }
  }

  public void setGetFieldStatistics( boolean v )
  {
    this.set( StatsParams.STATS, v );
  }
  
  public void setGetFieldStatistics( String field )
  {
    this.set( StatsParams.STATS, true );
    this.add( StatsParams.STATS_FIELD, field );
  }
  

  public void addGetFieldStatistics( String ... field )
    {
      this.set( StatsParams.STATS, true );
      this.add( StatsParams.STATS_FIELD, field );
    }
  
  public void addStatsFieldFacets( String field, String ... facets )
  {
    if( field == null ) {
      this.add( StatsParams.STATS_FACET, facets );
    }
    else {
      for( String f : facets ) {
        this.add( "f."+field+"."+StatsParams.STATS_FACET, f );
      }
    }
  }

  public void addStatsFieldCalcDistinct(String field, boolean calcDistinct) {
    if (field == null) {
      this.add(StatsParams.STATS_CALC_DISTINCT, Boolean.toString(calcDistinct));
    } else {
      this.add("f." + field + "." + StatsParams.STATS_CALC_DISTINCT, Boolean.toString(calcDistinct));
    }
  }

  public SolrQuery setFilterQueries(String ... fq) {
    this.set(CommonParams.FQ, fq);
    return this;
  }

  public SolrQuery addFilterQuery(String ... fq) {
    this.add(CommonParams.FQ, fq);
    return this;
  }

  public boolean removeFilterQuery(String fq) {
    return this.remove(CommonParams.FQ, fq);
  }

  public String[] getFilterQueries() {
    return this.getParams(CommonParams.FQ);
  }
  
  public boolean getHighlight() {
    return this.getBool(HighlightParams.HIGHLIGHT, false);
  }
  
  public SolrQuery setHighlight(boolean b) {
    if (b) {
      this.set(HighlightParams.HIGHLIGHT, true);
    } else {
      this.remove(HighlightParams.HIGHLIGHT);
      this.remove(HighlightParams.FIELD_MATCH);
      this.remove(HighlightParams.FIELDS);
      this.remove(HighlightParams.FORMATTER);
      this.remove(HighlightParams.FRAGSIZE);
      this.remove(HighlightParams.SIMPLE_POST);
      this.remove(HighlightParams.SIMPLE_PRE);
      this.remove(HighlightParams.SNIPPETS);
    }
    return this;
  }


  /**
   * Add field for MoreLikeThis. Automatically
   * enables MoreLikeThis.
   *
   * @param field the names of the field to be added
   * @return this
   */
  public SolrQuery addMoreLikeThisField(String field) {
    this.setMoreLikeThis(true);
    return addValueToParam(MoreLikeThisParams.SIMILARITY_FIELDS, field);
  }

  public SolrQuery setMoreLikeThisFields(String... fields) {
    if( fields == null || fields.length == 0 ) {
      this.remove( MoreLikeThisParams.SIMILARITY_FIELDS );
      this.setMoreLikeThis(false);
      return this;
    }

    StringBuilder sb = new StringBuilder();
    sb.append(fields[0]);
    for (int i = 1; i < fields.length; i++) {
      sb.append(',');
      sb.append(fields[i]);
    }
    this.set(MoreLikeThisParams.SIMILARITY_FIELDS, sb.toString());
    this.setMoreLikeThis(true);
    return this;
  }

  /**
   * @return an array with the fields used to compute similarity.
   */
  public String[] getMoreLikeThisFields() {
    String fl = this.get(MoreLikeThisParams.SIMILARITY_FIELDS);
    if(fl==null || fl.length()==0) {
      return null;
    }
    return fl.split(",");
  }

  /**
   * Sets the frequency below which terms will be ignored in the source doc
   *
   * @param mintf the minimum term frequency
   * @return this
   */
  public SolrQuery setMoreLikeThisMinTermFreq(int mintf) {
    this.set(MoreLikeThisParams.MIN_TERM_FREQ, mintf);
    return this;
  }

  /**
   * Gets the frequency below which terms will be ignored in the source doc
   */
  public int getMoreLikeThisMinTermFreq() {
    return this.getInt(MoreLikeThisParams.MIN_TERM_FREQ, 2);
  }

  /**
   * Sets the frequency at which words will be ignored which do not occur in
   * at least this many docs.
   *
   * @param mindf the minimum document frequency
   * @return this
   */
  public SolrQuery setMoreLikeThisMinDocFreq(int mindf) {
    this.set(MoreLikeThisParams.MIN_DOC_FREQ, mindf);
    return this;
  }

  /**
   * Gets the frequency at which words will be ignored which do not occur in
   * at least this many docs.
   */
  public int getMoreLikeThisMinDocFreq() {
    return this.getInt(MoreLikeThisParams.MIN_DOC_FREQ, 5);
  }

  /**
   * Sets the minimum word length below which words will be ignored.
   *
   * @param minwl the minimum word length
   * @return this
   */
  public SolrQuery setMoreLikeThisMinWordLen(int minwl) {
    this.set(MoreLikeThisParams.MIN_WORD_LEN, minwl);
    return this;
  }

  /**
   * Gets the minimum word length below which words will be ignored.
   */
  public int getMoreLikeThisMinWordLen() {
    return this.getInt(MoreLikeThisParams.MIN_WORD_LEN, 0);
  }

  /**
   * Sets the maximum word length above which words will be ignored.
   *
   * @param maxwl the maximum word length
   * @return this
   */
  public SolrQuery setMoreLikeThisMaxWordLen(int maxwl) {
    this.set(MoreLikeThisParams.MAX_WORD_LEN, maxwl);
    return this;
  }

  /**
   * Gets the maximum word length above which words will be ignored.
   */
  public int getMoreLikeThisMaxWordLen() {
    return this.getInt(MoreLikeThisParams.MAX_WORD_LEN, 0);
  }

  /**
   * Sets the maximum number of query terms that will be included in any
   * generated query.
   *
   * @param maxqt the maximum number of query terms
   * @return this
   */
  public SolrQuery setMoreLikeThisMaxQueryTerms(int maxqt) {
    this.set(MoreLikeThisParams.MAX_QUERY_TERMS, maxqt);
    return this;
  }

  /**
   * Gets the maximum number of query terms that will be included in any
   * generated query.
   */
  public int getMoreLikeThisMaxQueryTerms() {
    return this.getInt(MoreLikeThisParams.MAX_QUERY_TERMS, 25);
  }

  /**
   * Sets the maximum number of tokens to parse in each example doc field
   * that is not stored with TermVector support.
   *
   * @param maxntp the maximum number of tokens to parse
   * @return this
   */
  public SolrQuery setMoreLikeThisMaxTokensParsed(int maxntp) {
    this.set(MoreLikeThisParams.MAX_NUM_TOKENS_PARSED, maxntp);
    return this;
  }

  /**
   * Gets the maximum number of tokens to parse in each example doc field
   * that is not stored with TermVector support.
   */
  public int getMoreLikeThisMaxTokensParsed() {
    return this.getInt(MoreLikeThisParams.MAX_NUM_TOKENS_PARSED, 5000);
  }

  /**
   * Sets if the query will be boosted by the interesting term relevance.
   *
   * @param b set to true to boost the query with the interesting term relevance
   * @return this
   */
  public SolrQuery setMoreLikeThisBoost(boolean b) {
    this.set(MoreLikeThisParams.BOOST, b);
    return this;
  }

  /**
   * Gets if the query will be boosted by the interesting term relevance.
   */
  public boolean getMoreLikeThisBoost() {
    return this.getBool(MoreLikeThisParams.BOOST, false);
  }

  /**
   * Sets the query fields and their boosts using the same format as that
   * used in DisMaxQParserPlugin. These fields must also be added
   * using {@link #addMoreLikeThisField(String)}.
   *
   * @param qf the query fields
   * @return this
   */
  public SolrQuery setMoreLikeThisQF(String qf) {
    this.set(MoreLikeThisParams.QF, qf);
    return this;
  }

  /**
   * Gets the query fields and their boosts.
   */
  public String getMoreLikeThisQF() {
    return this.get(MoreLikeThisParams.QF);
  }

  /**
   * Sets the number of similar documents to return for each result.
   *
   * @param count the number of similar documents to return for each result
   * @return this
   */
  public SolrQuery setMoreLikeThisCount(int count) {
    this.set(MoreLikeThisParams.DOC_COUNT, count);
    return this;
  }

  /**
   * Gets the number of similar documents to return for each result.
   */
  public int getMoreLikeThisCount() {
    return this.getInt(MoreLikeThisParams.DOC_COUNT, MoreLikeThisParams.DEFAULT_DOC_COUNT);
  }

  /**
   * Enable/Disable MoreLikeThis. After enabling MoreLikeThis, the fields
   * used for computing similarity must be specified calling
   * {@link #addMoreLikeThisField(String)}.
   *
   * @param b flag to indicate if MoreLikeThis should be enabled. if b==false
   * removes all mlt.* parameters
   * @return this
   */
  public SolrQuery setMoreLikeThis(boolean b) {
    if(b) {
      this.set(MoreLikeThisParams.MLT, true);
    } else {
      this.remove(MoreLikeThisParams.MLT);
      this.remove(MoreLikeThisParams.SIMILARITY_FIELDS);
      this.remove(MoreLikeThisParams.MIN_TERM_FREQ);
      this.remove(MoreLikeThisParams.MIN_DOC_FREQ);
      this.remove(MoreLikeThisParams.MIN_WORD_LEN);
      this.remove(MoreLikeThisParams.MAX_WORD_LEN);
      this.remove(MoreLikeThisParams.MAX_QUERY_TERMS);
      this.remove(MoreLikeThisParams.MAX_NUM_TOKENS_PARSED);
      this.remove(MoreLikeThisParams.BOOST);
      this.remove(MoreLikeThisParams.QF);
      this.remove(MoreLikeThisParams.DOC_COUNT);
    }
    return this;
  }

  /**
   * @return true if MoreLikeThis is enabled, false otherwise
   */
  public boolean getMoreLikeThis() {
    return this.getBool(MoreLikeThisParams.MLT, false);
  }

  public SolrQuery setFields(String ... fields) {
    if( fields == null || fields.length == 0 ) {
      this.remove( CommonParams.FL );
      return this;
    }
    StringBuilder sb = new StringBuilder();
    sb.append( fields[0] );
    for( int i=1; i<fields.length; i++ ) {
      sb.append( ',' );
      sb.append( fields[i] );
    }
    this.set(CommonParams.FL, sb.toString() );
    return this;
  }
    
  public SolrQuery addField(String field) {
    return addValueToParam(CommonParams.FL, field);
  }

  public String getFields() {
    String fields = this.get(CommonParams.FL);
    if (fields!=null && fields.equals("score")) {
      fields = "*, score";
    }
    return fields;
  }

  private static Pattern scorePattern = Pattern.compile("(^|[, ])score");

  public SolrQuery setIncludeScore(boolean includeScore) {
    String fields = get(CommonParams.FL,"*");
    if (includeScore) {
      if (!scorePattern.matcher(fields).find()) {   
        this.set(CommonParams.FL, fields+",score");
      }
    } else {
      this.set(CommonParams.FL, scorePattern.matcher(fields).replaceAll(""));
    }
    return this;
  }

  public SolrQuery setQuery(String query) {
    this.set(CommonParams.Q, query);
    return this;
  }

  public String getQuery() {
    return this.get(CommonParams.Q);
  }

  public SolrQuery setRows(Integer rows) {
    if( rows == null ) {
      this.remove( CommonParams.ROWS );
    }
    else {
      this.set(CommonParams.ROWS, rows);
    }
    return this;
  }

  public Integer getRows()
  {
    return this.getInt(CommonParams.ROWS);
  }

  public SolrQuery setShowDebugInfo(boolean showDebugInfo) {
    this.set(CommonParams.DEBUG_QUERY, String.valueOf(showDebugInfo));
    return this;
  }

  public void setDistrib(boolean val) {
    this.set(CommonParams.DISTRIB, String.valueOf(val));
  }


  public SolrQuery setStart(Integer start) {
    if( start == null ) {
      this.remove( CommonParams.START );
    }
    else {
      this.set(CommonParams.START, start);
    }
    return this;
  }
  
  public Integer getStart()
  {
    return this.getInt(CommonParams.START);
  }

  /**
   * The Request Handler to use (see the solrconfig.xml), which is stored in the "qt" parameter.
   * Normally it starts with a '/' and if so it will be used by
   * {@link org.apache.solr.client.solrj.request.QueryRequest#getPath()} in the URL instead of the "qt" parameter.
   * If this is left blank, then the default of "/select" is assumed.
   *
   * @param qt The Request Handler name corresponding to one in solrconfig.xml on the server.
   * @return this
   */
  public SolrQuery setRequestHandler(String qt) {
    this.set(CommonParams.QT, qt);
    return this;
  }

  public String getRequestHandler() {
    return this.get(CommonParams.QT);
  }

  /**
   * @return this
   * @see ModifiableSolrParams#set(String,String[])
   */
  public SolrQuery setParam(String name, String ... values) {
    this.set(name, values);
    return this;
  }

  /**
   * @return this
   * @see org.apache.solr.common.params.ModifiableSolrParams#set(String, boolean)
   */
  public SolrQuery setParam(String name, boolean value) {
    this.set(name, value);
    return this;
  }

  /** get a deep copy of this object **/
  public SolrQuery getCopy() {
    SolrQuery q = new SolrQuery();
    for (String name : this.getParameterNames()) {
      q.setParam(name, this.getParams(name));
    }
    return q;
  }
  
  /**
  * Set the maximum time allowed for this query. If the query takes more time
  * than the specified milliseconds, a timeout occurs and partial (or no)
  * results may be returned.
  * 
  * If given Integer is null, then this parameter is removed from the request
  * 
  *@param milliseconds the time in milliseconds allowed for this query
  */
  public SolrQuery setTimeAllowed(Integer milliseconds) {
    if (milliseconds == null) {
      this.remove(CommonParams.TIME_ALLOWED);
    } else {
      this.set(CommonParams.TIME_ALLOWED, milliseconds);
    }
    return this;
  }
  
  /**
  * Get the maximum time allowed for this query.
  */
  public Integer getTimeAllowed() {
    return this.getInt(CommonParams.TIME_ALLOWED);
  }

  ///////////////////////
  //  Utility functions
  ///////////////////////
  
  private String toSortString(String field, ORDER order) {
    return field.trim() + ' ' + String.valueOf(order).trim();
  }
  
  private String join(String a, String b, String sep) {
    StringBuilder sb = new StringBuilder();
    if (a!=null && a.length()>0) {
      sb.append(a);
      sb.append(sep);
    } 
    if (b!=null && b.length()>0) {
      sb.append(b);
    }
    return sb.toString().trim();
  }
  
  private SolrQuery addValueToParam(String name, String value) {
    String tmp = this.get(name);
    tmp = join(tmp, value, ",");
    this.set(name, tmp);
    return this;
  }
   
  private String join(String[] vals, String sep, String removeVal) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<vals.length; i++) {
      if (!vals[i].equals(removeVal)) {
        if (sb.length() > 0) {
          sb.append(sep);
        }
        sb.append(vals[i]);
      }
    }
    return sb.toString().trim();
  }

  /**
   * A single sort clause, encapsulating what to sort and the sort order.
   * <p>
   * The item specified can be "anything sortable" by solr; some examples
   * include a simple field name, the constant string {@code score}, and functions
   * such as {@code sum(x_f, y_f)}.
   * <p>
   * A SortClause can be created through different mechanisms:
   * <PRE><code>
   * new SortClause("product", SolrQuery.ORDER.asc);
   * new SortClause("product", "asc");
   * SortClause.asc("product");
   * SortClause.desc("product");
   * </code></PRE>
   */
  public static class SortClause implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private final String item;
    private final ORDER order;

    /**
     * Creates a SortClause based on item and order
     * @param item item to sort on
     * @param order direction to sort
     */
    public SortClause(String item, ORDER order) {
      this.item = item;
      this.order = order;
    }

    /**
     * Creates a SortClause based on item and order
     * @param item item to sort on
     * @param order string value for direction to sort
     */
    public SortClause(String item, String order) {
      this(item, ORDER.valueOf(order));
    }

    /**
     * Creates an ascending SortClause for an item
     * @param item item to sort on
     */
    public static SortClause create (String item, ORDER order) {
      return new SortClause(item, order);
    }

    /**
     * Creates a SortClause based on item and order
     * @param item item to sort on
     * @param order string value for direction to sort
     */
    public static SortClause create(String item, String order) {
      return new SortClause(item, ORDER.valueOf(order));
    }

    /**
     * Creates an ascending SortClause for an item
     * @param item item to sort on
     */
    public static SortClause asc (String item) {
      return new SortClause(item, ORDER.asc);
    }

    /**
     * Creates a decending SortClause for an item
     * @param item item to sort on
     */
    public static SortClause desc (String item) {
      return new SortClause(item, ORDER.desc);
    }

    /**
     * Gets the item to sort, typically a function or a fieldname
     * @return item to sort
     */
    public String getItem() {
      return item;
    }

    /**
     * Gets the order to sort
     * @return order to sort
     */
    public ORDER getOrder() {
      return order;
    }

    public boolean equals(Object other){
      if (this == other) return true;
      if (!(other instanceof SortClause)) return false;
      final SortClause that = (SortClause) other;
      return this.getItem().equals(that.getItem()) && this.getOrder().equals(that.getOrder());
    }

    public int hashCode(){
      return this.getItem().hashCode();
    }

    /**
     * Gets a human readable description of the sort clause.
     * <p>
     * The returned string is not suitable for passing to Solr,
     * but may be useful in debug output and the like.
     * @return a description of the current sort clause
     */
    public String toString() {
      return "[" + getClass().getSimpleName() + ": item=" + getItem() + "; order=" + getOrder() + "]";
    }
  }
}
