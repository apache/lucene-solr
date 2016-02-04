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
package org.apache.lucene.queryparser.flexible.core.nodes;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.lucene.queryparser.flexible.messages.NLS;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.util.StringUtils;

/**
 * A {@link QueryNodeImpl} is the default implementation of the interface
 * {@link QueryNode}
 */
public abstract class QueryNodeImpl implements QueryNode, Cloneable {

  /* index default field */
  // TODO remove PLAINTEXT_FIELD_NAME replacing it with configuration APIs
  public static final String PLAINTEXT_FIELD_NAME = "_plain";

  private boolean isLeaf = true;

  private Hashtable<String, Object> tags = new Hashtable<>();

  private List<QueryNode> clauses = null;

  protected void allocate() {

    if (this.clauses == null) {
      this.clauses = new ArrayList<>();

    } else {
      this.clauses.clear();
    }

  }

  @Override
  public final void add(QueryNode child) {

    if (isLeaf() || this.clauses == null || child == null) {
      throw new IllegalArgumentException(NLS
          .getLocalizedMessage(QueryParserMessages.NODE_ACTION_NOT_SUPPORTED));
    }

    this.clauses.add(child);
    ((QueryNodeImpl) child).setParent(this);

  }

  @Override
  public final void add(List<QueryNode> children) {

    if (isLeaf() || this.clauses == null) {
      throw new IllegalArgumentException(NLS
          .getLocalizedMessage(QueryParserMessages.NODE_ACTION_NOT_SUPPORTED));
    }

    for (QueryNode child : children) {
      add(child);
    }

  }

  @Override
  public boolean isLeaf() {
    return this.isLeaf;
  }

  @Override
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
      child.removeFromParent();
    }
    
    ArrayList<QueryNode> existingChildren = new ArrayList<>(getChildren());
    for (QueryNode existingChild : existingChildren) {
      existingChild.removeFromParent();
    }
    
    // allocate new children list
    allocate();
    
    // add new children and set parent
    add(children);
  }

  @Override
  public QueryNode cloneTree() throws CloneNotSupportedException {
    QueryNodeImpl clone = (QueryNodeImpl) super.clone();
    clone.isLeaf = this.isLeaf;

    // Reset all tags
    clone.tags = new Hashtable<>();

    // copy children
    if (this.clauses != null) {
      List<QueryNode> localClauses = new ArrayList<>();
      for (QueryNode clause : this.clauses) {
        localClauses.add(clause.cloneTree());
      }
      clone.clauses = localClauses;
    }

    return clone;
  }

  @Override
  public QueryNode clone() throws CloneNotSupportedException {
    return cloneTree();
  }

  protected void setLeaf(boolean isLeaf) {
    this.isLeaf = isLeaf;
  }

  /**
   * @return a List for QueryNode object. Returns null, for nodes that do not
   *         contain children. All leaf Nodes return null.
   */
  @Override
  public final List<QueryNode> getChildren() {
    if (isLeaf() || this.clauses == null) {
      return null;
    }
    return new ArrayList<>(this.clauses);
  }

  @Override
  public void setTag(String tagName, Object value) {
    this.tags.put(tagName.toLowerCase(Locale.ROOT), value);
  }

  @Override
  public void unsetTag(String tagName) {
    this.tags.remove(tagName.toLowerCase(Locale.ROOT));
  }

  /** verify if a node contains a tag */
  @Override
  public boolean containsTag(String tagName) {
    return this.tags.containsKey(tagName.toLowerCase(Locale.ROOT));
  }

  @Override
  public Object getTag(String tagName) {
    return this.tags.get(tagName.toLowerCase(Locale.ROOT));
  }

  private QueryNode parent = null;

  private void setParent(QueryNode parent) {
    if (this.parent != parent) {
      this.removeFromParent();
      this.parent = parent;
    }
  }

  @Override
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
   * @param fld - field name
   * @return true if fld is the default field
   */
  // TODO: remove this method, it's commonly used by {@link
  // #toQueryString(org.apache.lucene.queryParser.core.parser.EscapeQuerySyntax)}
  // to figure out what is the default field, however, {@link
  // #toQueryString(org.apache.lucene.queryParser.core.parser.EscapeQuerySyntax)}
  // should receive the default field value directly by parameter
  protected boolean isDefaultField(CharSequence fld) {
    if (this.toQueryStringIgnoreFields)
      return true;
    if (fld == null)
      return true;
    if (QueryNodeImpl.PLAINTEXT_FIELD_NAME.equals(StringUtils.toString(fld)))
      return true;
    return false;
  }

  /**
   * Every implementation of this class should return pseudo xml like this:
   * 
   * For FieldQueryNode: &lt;field start='1' end='2' field='subject' text='foo'/&gt;
   * 
   * @see org.apache.lucene.queryparser.flexible.core.nodes.QueryNode#toString()
   */
  @Override
  public String toString() {
    return super.toString();
  }

  /**
   * Returns a map containing all tags attached to this query node.
   * 
   * @return a map containing all tags attached to this query node
   */
  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Object> getTagMap() {
    return (Map<String, Object>) this.tags.clone();
  }

  @Override
  public void removeChildren(QueryNode childNode){
    Iterator<QueryNode> it = this.clauses.iterator();
    while(it.hasNext()){
      if(it.next() == childNode){
        it.remove();
      }
    }
    childNode.removeFromParent();
  }

  @Override
  public void removeFromParent() {
    if (this.parent != null) {
      QueryNode parent = this.parent;
      this.parent = null;
      parent.removeChildren(this);
    }
  }

} // end class QueryNodeImpl
