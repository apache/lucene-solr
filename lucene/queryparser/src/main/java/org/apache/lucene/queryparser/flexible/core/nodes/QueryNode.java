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

import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

import java.util.List;
import java.util.Map;


/**
 * A {@link QueryNode} is a interface implemented by all nodes on a QueryNode
 * tree.
 */
public interface QueryNode {

  /** convert to a query string understood by the query parser */
  // TODO: this interface might be changed in the future
  public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser);

  /** for printing */
  @Override
  public String toString();

  /** get Children nodes */
  public List<QueryNode> getChildren();

  /** verify if a node is a Leaf node */
  public boolean isLeaf();

  /** verify if a node contains a tag */
  public boolean containsTag(String tagName);
  
  /**
   * Returns object stored under that tag name
   */
  public Object getTag(String tagName);
  
  public QueryNode getParent();

  /**
   * Recursive clone the QueryNode tree The tags are not copied to the new tree
   * when you call the cloneTree() method
   * 
   * @return the cloned tree
   */
  public QueryNode cloneTree() throws CloneNotSupportedException;

  // Below are the methods that can change state of a QueryNode
  // Write Operations (not Thread Safe)

  // add a new child to a non Leaf node
  public void add(QueryNode child);

  public void add(List<QueryNode> children);

  // reset the children of a node
  public void set(List<QueryNode> children);

  /**
   * Associate the specified value with the specified tagName. If the tagName
   * already exists, the old value is replaced. The tagName and value cannot be
   * null. tagName will be converted to lowercase.
   */
  public void setTag(String tagName, Object value);
  
  /**
   * Unset a tag. tagName will be converted to lowercase.
   */
  public void unsetTag(String tagName);
  
  /**
   * Returns a map containing all tags attached to this query node. 
   * 
   * @return a map containing all tags attached to this query node
   */
  public Map<String, Object> getTagMap();

  /**
   * Removes this query node from its parent.
   */
  public void removeFromParent();


  /**
   * Remove a child node
   * @param childNode Which child to remove
   */
  public void removeChildren(QueryNode childNode);
}
