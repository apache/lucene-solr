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
package org.apache.lucene.util.bkd;

import java.io.IOException;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.util.BytesRef;

/**
 * Abstraction of a block KD-tree that contains multi-dimensional points in byte[] space.
 *
 * @lucene.internal
 */
public interface BKDIndexInput {

  /** BKD tree parameters */
  BKDConfig getConfig();

  /** min packed value */
  byte[] getMinPackedValue();

  /** max packed value */
  byte[] getMaxPackedValue();

  /** Total number of points */
  long getPointCount();

  /** Total number of documents */
  int getDocCount();

  /** Create a new {@link IndexTree} to navigate the index */
  IndexTree getIndexTree();

  /** Create a new {@link LeafIterator} to read all leaf nodes */
  LeafIterator getLeafTreeIterator() throws IOException;

  /** Basic operations to read the BKD tree. */
  interface IndexTree extends Cloneable {

    /** Clone, but you are not allowed to pop up past the point where the clone happened. */
    IndexTree clone();

    /** Return current node id. */
    int getNodeID();

    /**
     * Navigate to left child. Should not be call if the current node has already called this method
     * or pushRight.
     */
    void pushLeft();

    /**
     * Navigate to right child. Should not be call if the current node has already called this
     * method.
     */
    void pushRight();

    /** Navigate to parent node. */
    void pop();

    /** Check if the current node is a leaf. */
    boolean isLeafNode();

    /** Check if the current node exists. */
    boolean nodeExists();

    /** Get split dimension for this node. Only valid after pushLeft or pushRight, not pop! */
    int getSplitDim();

    /** Get split dimension value for this node. Only valid after pushLeft or pushRight, not pop! */
    BytesRef getSplitDimValue();

    /** Get split value for this node. Only valid after pushLeft or pushRight, not pop! */
    byte[] getSplitPackedValue();

    /** Return the number of leaves below the current node. */
    int getNumLeaves();

    /** Visit the docs of the current node. Only valid if isLeafNode() is true. */
    void visitDocIDs(IntersectVisitor visitor) throws IOException;

    /** Visit the values of the current node. Only valid if isLeafNode() is true. */
    void visitDocValues(IntersectVisitor visitor) throws IOException;
  }

  /** Navigate the leaf nodes of the tree. */
  interface LeafIterator {

    /** Visit the next leaf node. If the tree has no more leaves, it returns false. */
    boolean visitNextLeaf(IntersectVisitor visitor) throws IOException;
  }
}
