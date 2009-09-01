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

package org.apache.lucene.analysis.cn.smart.hhmm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.cn.smart.Utility;

/**
 * Graph representing possible token pairs (bigrams) at each start offset in the sentence.
 * <p>
 * For each start offset, a list of possible token pairs is stored.
 * </p>
 * <p><font color="#FF0000">
 * WARNING: The status of the analyzers/smartcn <b>analysis.cn.smart</b> package is experimental. 
 * The APIs and file formats introduced here might change in the future and will not be 
 * supported anymore in such a case.</font>
 * </p>
 */
class BiSegGraph {

  private Map tokenPairListTable = new HashMap();

  private List segTokenList;

  private static BigramDictionary bigramDict = BigramDictionary.getInstance();

  public BiSegGraph(SegGraph segGraph) {
    segTokenList = segGraph.makeIndex();
    generateBiSegGraph(segGraph);
  }

  /*
   * Generate a BiSegGraph based upon a SegGraph
   */
  private void generateBiSegGraph(SegGraph segGraph) {
    double smooth = 0.1;
    int wordPairFreq = 0;
    int maxStart = segGraph.getMaxStart();
    double oneWordFreq, weight, tinyDouble = 1.0 / Utility.MAX_FREQUENCE;

    int next;
    char[] idBuffer;
    // get the list of tokens ordered and indexed
    segTokenList = segGraph.makeIndex();
    // 因为startToken（"始##始"）的起始位置是-1因此key为-1时可以取出startToken
    int key = -1;
    List nextTokens = null;
    while (key < maxStart) {
      if (segGraph.isStartExist(key)) {

        List tokenList = segGraph.getStartList(key);

        // 为某一个key对应的所有Token都计算一次
        for (Iterator iter = tokenList.iterator(); iter.hasNext();) {
          SegToken t1 = (SegToken) iter.next();
          oneWordFreq = t1.weight;
          next = t1.endOffset;
          nextTokens = null;
          // 找到下一个对应的Token，例如“阳光海岸”，当前Token是“阳光”， 下一个Token可以是“海”或者“海岸”
          // 如果找不到下一个Token，则说明到了末尾，重新循环。
          while (next <= maxStart) {
            // 因为endToken的起始位置是sentenceLen，因此等于sentenceLen是可以找到endToken
            if (segGraph.isStartExist(next)) {
              nextTokens = segGraph.getStartList(next);
              break;
            }
            next++;
          }
          if (nextTokens == null) {
            break;
          }
          for (Iterator iter2 = nextTokens.iterator(); iter2.hasNext();) {
            SegToken t2 = (SegToken) iter2.next();
            idBuffer = new char[t1.charArray.length + t2.charArray.length + 1];
            System.arraycopy(t1.charArray, 0, idBuffer, 0, t1.charArray.length);
            idBuffer[t1.charArray.length] = BigramDictionary.WORD_SEGMENT_CHAR;
            System.arraycopy(t2.charArray, 0, idBuffer,
                t1.charArray.length + 1, t2.charArray.length);

            // Two linked Words frequency
            wordPairFreq = bigramDict.getFrequency(idBuffer);

            // Smoothing

            // -log{a*P(Ci-1)+(1-a)P(Ci|Ci-1)} Note 0<a<1
            weight = -Math
                .log(smooth
                    * (1.0 + oneWordFreq)
                    / (Utility.MAX_FREQUENCE + 0.0)
                    + (1.0 - smooth)
                    * ((1.0 - tinyDouble) * wordPairFreq / (1.0 + oneWordFreq) + tinyDouble));

            SegTokenPair tokenPair = new SegTokenPair(idBuffer, t1.index,
                t2.index, weight);
            this.addSegTokenPair(tokenPair);
          }
        }
      }
      key++;
    }

  }

  /**
   * Returns true if their is a list of token pairs at this offset (index of the second token)
   * 
   * @param to index of the second token in the token pair
   * @return true if a token pair exists
   */
  public boolean isToExist(int to) {
    return tokenPairListTable.get(new Integer(to)) != null;
  }

  /**
   * Return a {@link List} of all token pairs at this offset (index of the second token)
   * 
   * @param to index of the second token in the token pair
   * @return {@link List} of token pairs.
   */
  public List getToList(int to) {
    return (List) tokenPairListTable.get(new Integer(to));
  }

  /**
   * Add a {@link SegTokenPair}
   * 
   * @param tokenPair {@link SegTokenPair}
   */
  public void addSegTokenPair(SegTokenPair tokenPair) {
    int to = tokenPair.to;
    if (!isToExist(to)) {
      ArrayList newlist = new ArrayList();
      newlist.add(tokenPair);
      tokenPairListTable.put(new Integer(to), newlist);
    } else {
      List tokenPairList = (List) tokenPairListTable.get(new Integer(to));
      tokenPairList.add(tokenPair);
    }
  }

  /**
   * Get the number of {@link SegTokenPair} entries in the table.
   * @return number of {@link SegTokenPair} entries
   */
  public int getToCount() {
    return tokenPairListTable.size();
  }

  /**
   * Find the shortest path with the Viterbi algorithm.
   * @return {@link List}
   */
  public List getShortPath() {
    int current;
    int nodeCount = getToCount();
    List path = new ArrayList();
    PathNode zeroPath = new PathNode();
    zeroPath.weight = 0;
    zeroPath.preNode = 0;
    path.add(zeroPath);
    for (current = 1; current <= nodeCount; current++) {
      double weight;
      List edges = getToList(current);

      double minWeight = Double.MAX_VALUE;
      SegTokenPair minEdge = null;
      for (Iterator iter1 = edges.iterator(); iter1.hasNext();) {
        SegTokenPair edge = (SegTokenPair) iter1.next();
        weight = edge.weight;
        PathNode preNode = (PathNode) path.get(edge.from);
        if (preNode.weight + weight < minWeight) {
          minWeight = preNode.weight + weight;
          minEdge = edge;
        }
      }
      PathNode newNode = new PathNode();
      newNode.weight = minWeight;
      newNode.preNode = minEdge.from;
      path.add(newNode);
    }

    // Calculate PathNodes
    int preNode, lastNode;
    lastNode = path.size() - 1;
    current = lastNode;
    List rpath = new ArrayList();
    List resultPath = new ArrayList();

    rpath.add(new Integer(current));
    while (current != 0) {
      PathNode currentPathNode = (PathNode) path.get(current);
      preNode = currentPathNode.preNode;
      rpath.add(new Integer(preNode));
      current = preNode;
    }
    for (int j = rpath.size() - 1; j >= 0; j--) {
      Integer idInteger = (Integer) rpath.get(j);
      int id = idInteger.intValue();
      SegToken t = (SegToken) segTokenList.get(id);
      resultPath.add(t);
    }
    return resultPath;

  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    Collection values = tokenPairListTable.values();
    for (Iterator iter1 = values.iterator(); iter1.hasNext();) {
      List segList = (List) iter1.next();
      for (Iterator iter2 = segList.iterator(); iter2.hasNext();) {
        SegTokenPair pair = (SegTokenPair) iter2.next();
        sb.append(pair + "\n");
      }
    }
    return sb.toString();
  }

}
