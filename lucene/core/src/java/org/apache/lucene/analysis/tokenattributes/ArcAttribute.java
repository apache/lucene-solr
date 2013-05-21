package org.apache.lucene.analysis.tokenattributes;

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

import org.apache.lucene.util.Attribute;

/**
 * The from/to nodes for a token: every token is one edge in
 * the growing graph.  All leaving tokens for a given node
 * must be enumerated at once; once the tokenizer moves
 * beyond that node then it's done.  Nodes are numbered with
 * integers, but these are not positions!  Positions are
 * assigned at the end of tokenization.
 */
public interface ArcAttribute extends Attribute {
  /** 
   * Returns this Token's from node.
   */
  public int from();

  /** 
   * Returns this Token's to node.
   */
  public int to();

  /** 
   * Set the dest.
   */
  public void set(int from, int to);
}
