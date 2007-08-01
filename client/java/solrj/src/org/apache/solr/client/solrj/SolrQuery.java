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

package org.apache.solr.client.solrj;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.ModifiableSolrParams;


/**
 * This is an augmented SolrParams with get/set/add fields for common fields used
 * in the Standard and Dismax request handlers
 * 
 * @version $Id$
 * @since solr 1.3
 */
public class SolrQuery extends ModifiableSolrParams 
{
  public enum ORDER { desc, asc;
    public ORDER reverse() {
      return (this == asc) ? desc : asc;
    }
  }
  
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


  /** add a field for facet computation
   * 
   * @param f the field name from the IndexSchema
   */
  public void addFacetField(String f) {
    this.add(FacetParams.FACET_FIELD, f);
    this.set(FacetParams.FACET, true);
    this.setFacetMinCount(1);
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
   * @param b flag to indicate faceting should be enabled.  if b==false removes all other faceting parameters
   */
  public void setFacet(boolean b) {
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
    }
  }
  
  public void setFacetPrefix( String prefix )
  {
    this.set( FacetParams.FACET_PREFIX, prefix );
  }

  public void setFacetPrefix( String field, String prefix )
  {
    this.set( "f."+field+"."+FacetParams.FACET_PREFIX, prefix );
  }

  /** add a faceting query
   * 
   * @param f facet query
   */
  public void addFacetQuery(String f) {
    this.add(FacetParams.FACET_QUERY, f);
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

  /** se the facet limit
   * 
   * @param lim number facet items to return
   */
  public void setFacetLimit(int lim) {
    this.set(FacetParams.FACET_LIMIT, lim);
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
  public void setFacetMinCount(int cnt) {
    this.set(FacetParams.FACET_MINCOUNT, cnt);
  }

  /** get facet minimum count
   * 
   * @return facet minimum count or default of 1
   */
  public int getFacetMinCount() {
    return this.getInt(FacetParams.FACET_LIMIT, 1);
  }

  public void setMissing(String fld) {
    this.set(FacetParams.FACET_MISSING, fld);
  }

  /** get facet sort
   * 
   * @return facet sort or default of true
   */
  public boolean getFacetSort() {
    return this.getBool(FacetParams.FACET_SORT, false);
  }

  /** set facet sort
   * 
   * @param sort sort facets
   */
  public void setFacetSort(Boolean sort) {
    this.set(FacetParams.FACET_SORT, sort);
  }

  /** add highlight field
   * 
   * @param f field to enable for highlighting
   */
  public void addHighlightField(String f) {
    this.add(HighlightParams.FIELDS, f);
    this.set(HighlightParams.HIGHLIGHT, true);
  }

  /** remove a field for highlighting
   * 
   * @param f field name to not highlight
   * @return true if removed, false otherwise
   */
  public boolean removeHighlightField(String f) {
    boolean b = this.remove(HighlightParams.FIELDS, f);
    if (this.get(HighlightParams.FIELDS) == null) {
      this.setHighlight(false);
    }
    return b;
  }

  /** get list of hl fields
   * 
   * @return highlight fields or null if not set/empty
   */
  public String[] getHighlightFields() {
    return this.getParams(HighlightParams.FIELDS);
  }

  public void setHighlightSnippets(int num) {
    this.set(HighlightParams.SNIPPETS, num);
  }

  public int getHighlightSnippets() {
    return this.getInt(HighlightParams.SNIPPETS, 1);
  }

  public void setHighlightFragsize(int num) {
    this.set(HighlightParams.FRAGSIZE, num);
  }

  public int getHighlightFragsize() {
    return this.getInt(HighlightParams.FRAGSIZE, 100);
  }

  public void setHighlightRequireFieldMatch(boolean flag) {
    this.set(HighlightParams.FIELD_MATCH, flag);
  }

  public boolean setHighlightRequireFieldMatch() {
    return this.getBool(HighlightParams.FIELD_MATCH, false);
  }

  public void setHighlightSimplePre(String f) {
    this.set(HighlightParams.SIMPLE_PRE, f);
  }

  public String getHighlightSimplePre() {
    return this.get(HighlightParams.SIMPLE_PRE, "");
  }

  public void setHighlightSimplePost(String f) {
    this.set(HighlightParams.SIMPLE_POST, f);
  }

  public String getHighlightSimplePost() {
    return this.get(HighlightParams.SIMPLE_POST, "");
  }

  public void addSortField(String field, ORDER order) {
    addValueToParam(CommonParams.SORT, toSortString(field, order));
  }

  public void removeSortField(String field, ORDER order) {
    String s = this.get(CommonParams.SORT);
    String removeSort = toSortString(field, order);
    if (s != null) {
      String[] sorts = s.split(",");
      s = join(sorts, ", ", removeSort);
      if (s.length()==0) s=null;
      this.set(CommonParams.SORT, s);
    }
  }
  
  public String[] getSortFields() {
    String s = getSortField();
    if (s==null) return null;
    return s.split(",");
  }

  public String getSortField() {
    return this.get(CommonParams.SORT);
  }

  public void setFilterQueries(String ... fq) {
    this.set(CommonParams.FQ, fq);
  }

  public void addFilterQuery(String ... fq) {
    this.add(CommonParams.FQ, fq);
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
  
  public void setHighlight(boolean b) {
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
  }

  public void setFields(String ... fields) {
    this.set(CommonParams.FL, fields);
  }
    
  public void addField(String field) {
    addValueToParam(CommonParams.FL, field);
  }

  public String getFields() {
    String fields = this.get(CommonParams.FL);
    if (fields!=null && fields.equals("score")) {
      fields = "*, score";
    }
    return fields;
  }

  public void setIncludeScore(boolean includeScore) {
    if (includeScore) {
      this.add(CommonParams.FL, "score");
    } else {
      this.remove(CommonParams.FL, "score");
    }
  }

  public void setQuery(String query) {
    this.set(CommonParams.Q, query);
  }

  public void setRows(Integer rows) {
    this.set(CommonParams.ROWS, rows);
  }

  public void setShowDebugInfo(boolean showDebugInfo) {
    this.set(CommonParams.DEBUG_QUERY, String.valueOf(showDebugInfo));
  }

// use addSortField( sort, order 
//  public void setSort(String ... sort) {
//    this.set(CommonParams.SORT, sort);
//  }

  public void setStart(Integer start) {
    this.set(CommonParams.START, start);
  }

  public void setQueryType(String qt) {
    this.set(CommonParams.QT, qt);
  }

  public String getQueryType() {
    return this.get(CommonParams.QT);
  }

  public void setParam(String name, String ... values) {
    this.set(name, values);
  }

  public void setParam(String name, boolean value) {
    this.set(name, value);
  }

  /** get a deep copy of this object * */
  public SolrQuery getCopy() {
    SolrQuery q = new SolrQuery();
    for (String name : this.getParameterNames()) {
      q.setParam(name, this.getParams(name));
    }
    return q;
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
  
  private void addValueToParam(String name, String value) {
    String tmp = this.get(name);
    tmp = join(tmp, value, ",");
    this.set(name, tmp);
  }
   
  private String join(String[] vals, String sep, String removeVal) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<vals.length; i++) {
      if (removeVal==null || !vals[i].equals(removeVal)) {
        sb.append(vals[i]);
        if (i<vals.length-1) {
          sb.append(sep);
        }
      }
    }
    return sb.toString().trim();
  }
}
