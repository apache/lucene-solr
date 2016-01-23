package org.apache.lucene.queryParser.core.nodes;

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

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.lucene.messages.NLS;
import org.apache.lucene.queryParser.core.messages.QueryParserMessages;

/**
 * A {@link QueryNodeImpl} is the default implementation of the interface
 * {@link QueryNode}
 */
public abstract class QueryNodeImpl implements QueryNode, Cloneable {

  private static final long serialVersionUID = 5569870883474845989L;

  /* index default field */
  // TODO remove PLAINTEXT_FIELD_NAME replacing it with configuration APIs
  public static final String PLAINTEXT_FIELD_NAME = "_plain";

  private boolean isLeaf = true;

  private Hashtable<CharSequence, Object> tags = new Hashtable<CharSequence, Object>();

  private List<QueryNode> clauses = null;

  protected void allocate() {

    if (this.clauses == null) {
      this.clauses = new ArrayList<QueryNode>();

    } else {
      this.clauses.clear();
    }

  }

  public final void add(QueryNode child) {

    if (isLeaf() || this.clauses == null || child == null) {
      throw new IllegalArgumentException(NLS
          .getLocalizedMessage(QueryParserMessages.NODE_ACTION_NOT_SUPPORTED));
    }

    this.clauses.add(child);
    ((QueryNodeImpl) child).setParent(this);

  }

  public final void add(List<QueryNode> children) {

    if (isLeaf() || this.clauses == null) {
      throw new IllegalArgumentException(NLS
          .getLocalizedMessage(QueryParserMessages.NODE_ACTION_NOT_SUPPORTED));
    }

    for (QueryNode child : children) {
      add(child);
    }

  }

  public boolean isLeaf() {
    return this.isLeaf;
  }

  public final void set(List<QueryNode> children) {

    if (isLeaf() || this.clauses == null) {
      ResourceBundle bundle = ResourceBundle
          .getBundle("org.apache.lucene.queryParser.messages.QueryParserMessages");
      String message = bundle.getObject("Q0008E.NODE_ACTION_NOT_SUPPORTED")
          .toString();

      throw new IllegalArgumentException(message);

    }

    // reset parent value
    for (QueryNode child : children) {

      ((QueryNodeImpl) child).setParent(null);

    }

    // allocate new children list
    allocate();

    // add new children and set parent
    for (QueryNode child : children) {
      add(child);
    }
  }

  public QueryNode cloneTree() throws CloneNotSupportedException {
    QueryNodeImpl clone = (QueryNodeImpl) super.clone();
    clone.isLeaf = this.isLeaf;

    // Reset all tags
    clone.tags = new Hashtable<CharSequence, Object>();

    // copy children
    if (this.clauses != null) {
      List<QueryNode> localClauses = new ArrayList<QueryNode>();
      for (QueryNode clause : this.clauses) {
        localClauses.add(clause.cloneTree());
      }
      clone.clauses = localClauses;
    }

    return clone;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    return cloneTree();
  }

  protected void setLeaf(boolean isLeaf) {
    this.isLeaf = isLeaf;
  }

  /**
   * @return a List for QueryNode object. Returns null, for nodes that do not
   *         contain children. All leaf Nodes return null.
   */
  public final List<QueryNode> getChildren() {
    if (isLeaf() || this.clauses == null) {
      return null;
    }
    return this.clauses;
  }

  public void setTag(CharSequence tagName, Object value) {
    this.tags.put(tagName.toString().toLowerCase(), value);
  }

  public void unsetTag(CharSequence tagName) {
    this.tags.remove(tagName.toString().toLowerCase());
  }

  public boolean containsTag(CharSequence tagName) {
    return this.tags.containsKey(tagName.toString().toLowerCase());
  }

  public Object getTag(CharSequence tagName) {
    return this.tags.get(tagName.toString().toLowerCase());
  }

  private QueryNode parent = null;

  private void setParent(QueryNode parent) {
    this.parent = parent;
  }

  public QueryNode getParent() {
    return this.parent;
  }

  protected boolean isRoot() {
    return getParent() == null;
  }

  /**
   * If set to true the the method toQueryString will not write field names
   */
  protected boolean toQueryStringIgnoreFields = false;

  /**
   * This method is use toQueryString to detect if fld is the default field
   * 
   * @param fld
   *          - field name
   * @return true if fld is the default field
   */
  protected boolean isDefaultField(CharSequence fld) {
    if (this.toQueryStringIgnoreFields)
      return true;
    if (fld == null)
      return true;
    if (QueryNodeImpl.PLAINTEXT_FIELD_NAME.equals(fld.toString()))
      return true;
    return false;
  }

  /**
   * Every implementation of this class should return pseudo xml like this:
   * 
   * For FieldQueryNode: <field start='1' end='2' field='subject' text='foo'/>
   * 
   * @see org.apache.lucene.queryParser.core.nodes.QueryNode#toString()
   */
  @Override
  public String toString() {
    return super.toString();
  }

  /**
   * @see org.apache.lucene.queryParser.core.nodes.QueryNode#getTag(CharSequence)
   * @return a Map with all tags for this QueryNode
   */
  @SuppressWarnings( { "unchecked" })
  public Map<CharSequence, Object> getTags() {
    return (Map<CharSequence, Object>) this.tags.clone();
  }

} // end class QueryNodeImpl
