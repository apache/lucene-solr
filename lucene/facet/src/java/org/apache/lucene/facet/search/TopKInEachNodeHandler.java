package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.facet.collections.IntIterator;
import org.apache.lucene.facet.collections.IntToObjectMap;
import org.apache.lucene.facet.partitions.IntermediateFacetResult;
import org.apache.lucene.facet.partitions.PartitionsFacetResultsHandler;
import org.apache.lucene.facet.search.FacetRequest.SortOrder;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.util.PriorityQueue;

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
 * Generates {@link FacetResult} from the {@link FacetArrays} aggregated for a
 * particular {@link FacetRequest}. The generated {@link FacetResult} is a
 * subtree of the taxonomy tree. Its root node,
 * {@link FacetResult#getFacetResultNode()}, is the facet specified by
 * {@link FacetRequest#categoryPath}, and the enumerated children,
 * {@link FacetResultNode#subResults}, of each node in that {@link FacetResult}
 * are the top K ( = {@link FacetRequest#numResults}) among its children in the
 * taxonomy. The depth (number of levels excluding the root) of the
 * {@link FacetResult} tree is specified by {@link FacetRequest#getDepth()}.
 * <p>
 * Because the number of selected children of each node is restricted, and not
 * the overall number of nodes in the {@link FacetResult}, facets not selected
 * into {@link FacetResult} might have better values, or ordinals, (typically,
 * higher counts), than facets that are selected into the {@link FacetResult}.
 * <p>
 * The generated {@link FacetResult} also provides with
 * {@link FacetResult#getNumValidDescendants()}, which returns the total number
 * of facets that are descendants of the root node, no deeper than
 * {@link FacetRequest#getDepth()}, and which have valid value. The rootnode
 * itself is not counted here. Valid value is determined by the
 * {@link FacetResultsHandler}. {@link TopKInEachNodeHandler} defines valid as
 * != 0.
 * <p>
 * <b>NOTE:</b> this code relies on the assumption that
 * {@link TaxonomyReader#INVALID_ORDINAL} == -1, a smaller value than any valid
 * ordinal.
 * 
 * @lucene.experimental
 */
public class TopKInEachNodeHandler extends PartitionsFacetResultsHandler {

  public TopKInEachNodeHandler(TaxonomyReader taxonomyReader, FacetRequest facetRequest, FacetArrays facetArrays) {
    super(taxonomyReader, facetRequest, facetArrays);
  }

  /**
   * Recursively explore all facets that can be potentially included in the
   * {@link FacetResult} to be generated, and that belong to the given
   * partition, so that values can be examined and collected. For each such
   * node, gather its top K ({@link FacetRequest#numResults}) children among its
   * children that are encountered in the given particular partition (aka
   * current counting list).
   * @param offset
   *          to <code>offset</code> + the length of the count arrays within
   *          <code>arrays</code> (exclusive)
   * 
   * @return {@link IntermediateFacetResult} consisting of
   *         {@link IntToObjectMap} that maps potential {@link FacetResult}
   *         nodes to their top K children encountered in the current partition.
   *         Note that the mapped potential tree nodes need not belong to the
   *         given partition, only the top K children mapped to. The aim is to
   *         identify nodes that are certainly excluded from the
   *         {@link FacetResult} to be eventually (after going through all the
   *         partitions) returned by this handler, because they have K better
   *         siblings, already identified in this partition. For the identified
   *         excluded nodes, we only count number of their descendants in the
   *         subtree (to be included in
   *         {@link FacetResult#getNumValidDescendants()}), but not bother with
   *         selecting top K in these generations, which, by definition, are,
   *         too, excluded from the FacetResult tree.
   * @throws IOException
   *           in case
   *           {@link TaxonomyReader#getOrdinal(org.apache.lucene.facet.taxonomy.CategoryPath)}
   *           does.
   * @see #fetchPartitionResult(int)
   */
  @Override
  public IntermediateFacetResult fetchPartitionResult(int offset) throws IOException {

    // get the root of the result tree to be returned, and the depth of that result tree
    // (depth means number of node levels excluding the root). 
    int rootNode = this.taxonomyReader.getOrdinal(facetRequest.categoryPath);
    if (rootNode == TaxonomyReader.INVALID_ORDINAL) {
      return null;
    }

    int K = Math.min(facetRequest.numResults,taxonomyReader.getSize()); // number of best results in each node

    // this will grow into the returned IntermediateFacetResult
    IntToObjectMap<AACO> AACOsOfOnePartition = new IntToObjectMap<AACO>();

    int partitionSize = facetArrays.arrayLength; // all partitions, except, possibly, the last,
    // have the same length. Hence modulo is OK.

    int depth = facetRequest.getDepth();

    if (depth == 0) {
      // Need to only have root node.
      IntermediateFacetResultWithHash tempFRWH = new IntermediateFacetResultWithHash(
          facetRequest, AACOsOfOnePartition);
      if (isSelfPartition(rootNode, facetArrays, offset)) {
        tempFRWH.isRootNodeIncluded = true;
        tempFRWH.rootNodeValue = this.facetRequest.getValueOf(facetArrays, rootNode % partitionSize);
      }
      return tempFRWH;
    }

    if (depth > Short.MAX_VALUE - 3) {
      depth = Short.MAX_VALUE -3;
    }

    int endOffset = offset + partitionSize; // one past the largest ordinal in the partition
    ParallelTaxonomyArrays childrenArray = taxonomyReader.getParallelTaxonomyArrays();
    int[] children = childrenArray.children();
    int[] siblings = childrenArray.siblings();
    int totalNumOfDescendantsConsidered = 0; // total number of facets with value != 0, 
    // in the tree. These include those selected as top K in each node, and all the others that
    // were not. Not including rootNode

    // the following priority queue will be used again and again for each node recursed into
    // to select its best K children among its children encountered in the given partition
    PriorityQueue<AggregatedCategory> pq = 
      new AggregatedCategoryHeap(K, this.getSuitableACComparator());

    // reusables will feed the priority queue in each use 
    AggregatedCategory [] reusables = new AggregatedCategory[2+K];
    for (int i = 0; i < reusables.length; i++) {
      reusables[i] = new AggregatedCategory(1,0);
    }

    /*
     * The returned map is built by a recursive visit of potential tree nodes. Nodes 
     * determined to be excluded from the FacetResult are not recursively explored as others,
     * they are only recursed in order to count the number of their descendants.
     * Also, nodes that they and any of their descendants can not be mapped into facets encountered 
     * in this partition, are, too, explored no further. These are facets whose ordinal 
     * numbers are greater than the ordinals of the given partition. (recall that the Taxonomy
     * maintains that a parent ordinal is smaller than any of its descendants' ordinals).  
     * So, when scanning over all children of a potential tree node n: (1) all children with ordinal number
     * greater than those in the given partition are skipped over, (2) among the children of n residing
     * in this partition, the best K children are selected (using pq) for usual further recursion 
     * and the rest (those rejected out from the pq) are only recursed for counting total number
     * of descendants, and (3) all the children of ordinal numbers smaller than the given partition 
     * are further explored in the usual way, since these may lead to descendants residing in this partition.
     * 
     * ordinalStack drives the recursive descent. 
     * Top of stack holds the current node which we recurse from.
     * ordinalStack[0] holds the root of the facetRequest, and
     * it is always maintained that parent(ordianlStack[i]) = ordinalStack[i-1]. 
     * localDepth points to the current top of ordinalStack.
     * Only top of ordinalStack can be TaxonomyReader.INVALID_ORDINAL, and this if and only if
     * the element below it explored all its relevant children.
     */
    int[] ordinalStack = new int[depth+2]; // for 0 and for invalid on top
    ordinalStack[0] = rootNode;
    int localDepth = 0;

    /* 
     * bestSignlingsStack[i] maintains the best K children of ordinalStack[i-1], namely,
     * the best K siblings of ordinalStack[i], best K among those residing in the given partition.
     * Note that the residents of ordinalStack need not belong
     * to the current partition, only the residents of bestSignlingsStack.
     * When exploring the children of ordianlStack[i-1] that reside in the current partition
     * (after the top K of them have been determined and stored into bestSignlingsStack[i]),
     * siblingExplored[i] points into bestSignlingsStack[i], to the child now explored, hence
     * residing in ordinalStack[i], and firstToTheLeftOfPartition[i] holds the largest ordinal of
     * a sibling smaller than the ordinals in the partition.  
     * When siblingExplored[i] == max int, the top K siblings of ordinalStack[i] among those siblings
     * that reside in this partition have not been determined yet. 
     * if siblingExplored[i] < 0, the node in ordinalStack[i] is to the left of partition 
     * (i.e. of a smaller ordinal than the current partition) 
     * (step (3) above is executed for the children of ordianlStack[i-1])   
     */
    int[][] bestSignlingsStack = new int[depth+2][];
    int[] siblingExplored = new int[depth+2];
    int[] firstToTheLeftOfPartition = new int [depth+2];

    int tosOrdinal; // top of stack element, the ordinal at the top of stack

    /*
     * to start the loop, complete the datastructures for root node: 
     * push its youngest child to ordinalStack; make a note in siblingExplored[] that the children
     * of rootNode, which reside in the current partition have not been read yet to select the top
     * K of them.  Also, make rootNode as if, related to its parent, rootNode belongs to the children
     * of ordinal numbers smaller than those of the current partition (this will ease on end condition -- 
     * we can continue to the older sibling of rootNode once the localDepth goes down, before we verify that 
     * it went that down)
     */
    ordinalStack[++localDepth] = children[rootNode];
    siblingExplored[localDepth] = Integer.MAX_VALUE;  // we have not verified position wrt current partition
    siblingExplored[0] = -1; // as if rootNode resides to the left of current position

    /*
     * now the whole recursion: loop as long as stack is not empty of elements descendants of 
     * facetRequest's root.
     */

    while (localDepth > 0) {
      tosOrdinal = ordinalStack[localDepth];
      if (tosOrdinal == TaxonomyReader.INVALID_ORDINAL) {
        // the brotherhood that has been occupying the top of stack is all exhausted.  
        // Hence, element below tos, namely, father of tos, has all its children, 
        // and itself, all explored. 
        localDepth--;
        // replace this father, now on top of stack, by this father's sibling:
        // this parent's ordinal can not be greater than current partition, as otherwise
        // its child, now just removed, would not have been pushed on it.
        // so the father is either inside the partition, or smaller ordinal
        if (siblingExplored[localDepth] < 0 ) {
          ordinalStack[localDepth] = siblings[ordinalStack[localDepth]];
          continue;
        } 
        // in this point, siblingExplored[localDepth] between 0 and number of bestSiblings
        // it can not be max int
        siblingExplored[localDepth]--;
        if (siblingExplored[localDepth] == -1 ) {
          //siblings residing in the partition have been all processed, we now move
          // to those of ordinal numbers smaller than the partition
          ordinalStack[localDepth] = firstToTheLeftOfPartition[localDepth];
        } else {
          // still explore siblings residing in the partition
          // just move to the next one
          ordinalStack[localDepth] = bestSignlingsStack[localDepth][siblingExplored[localDepth]];
        }
        continue;
      } // endof tosOrdinal is invalid, and hence removed, and its parent was replaced by this 
      // parent's sibling

      // now try to push a kid, but first look at tos whether it 'deserves' its kids explored:
      // it is not to the right of current partition, and we know whether to only count or to 
      // select best K siblings.
      if (siblingExplored[localDepth] == Integer.MAX_VALUE) {
        //tosOrdinal was not examined yet for its position relative to current partition
        // and the best K of current partition, among its siblings, have not been determined yet
        while (tosOrdinal >= endOffset) {
          tosOrdinal = siblings[tosOrdinal];
        }
        // now it is inside. Run it and all its siblings inside the partition through a heap
        // and in doing so, count them, find best K
        pq.clear();

        //reusables are consumed as from a stack. The stack starts full and returns full.
        int tosReuslables = reusables.length -1;  

        while (tosOrdinal >= offset) { // while tosOrdinal belongs to the given partition; here, too, we use the fact
          // that TaxonomyReader.INVALID_ORDINAL == -1 < offset
          double value = facetRequest.getValueOf(facetArrays, tosOrdinal % partitionSize);
          if (value != 0) { // the value of yc is not 0, it is to be considered.  
            totalNumOfDescendantsConsidered++;

            // consume one reusable, and push to the priority queue
            AggregatedCategory ac = reusables[tosReuslables--];  
            ac.ordinal = tosOrdinal;
            ac.value = value; 
            ac = pq.insertWithOverflow(ac);
            if (null != ac) {
              /* when a facet is excluded from top K, because already in this partition it has
               * K better siblings, it is only recursed for count only.
               */ 
              // update totalNumOfDescendants by the now excluded node and all its descendants
              totalNumOfDescendantsConsidered--; // reduce the 1 earned when the excluded node entered the heap
              // and now return it and all its descendants. These will never make it to FacetResult
              totalNumOfDescendantsConsidered += countOnly (ac.ordinal, children, 
                  siblings, partitionSize, offset, endOffset, localDepth, depth);
              reusables[++tosReuslables] = ac;
            }
          }
          tosOrdinal = siblings[tosOrdinal];  
        }
        // now pq has best K children of ordinals that belong to the given partition.   
        // Populate a new AACO with them.
        // tosOrdinal is now first sibling smaller than partition, make a note of that
        firstToTheLeftOfPartition[localDepth] = tosOrdinal;
        int aaci = pq.size();
        int[] ords = new int[aaci];
        double [] vals = new double [aaci];
        while (aaci > 0) {
          AggregatedCategory ac = pq.pop();
          ords[--aaci] = ac.ordinal;
          vals[aaci] = ac.value;
          reusables[++tosReuslables] = ac;
        }
        // if more than 0 ordinals, add this AACO to the map to be returned, 
        // and add ords to sibling stack, and make a note in siblingExplored that these are to 
        // be visited now
        if (ords.length > 0) {
          AACOsOfOnePartition.put(ordinalStack[localDepth-1], new AACO(ords,vals));
          bestSignlingsStack[localDepth] = ords;
          siblingExplored[localDepth] = ords.length-1;
          ordinalStack[localDepth] = ords[ords.length-1];
        } else {
          // no ordinals siblings of tosOrdinal in current partition, move to the left of it
          // tosOrdinal is already there (to the left of partition).
          // make a note of it in siblingExplored
          ordinalStack[localDepth] = tosOrdinal;
          siblingExplored[localDepth] = -1;
        }
        continue;
      } // endof we did not check the position of a valid ordinal wrt partition

      // now tosOrdinal is a valid ordinal, inside partition or to the left of it, we need 
      // to push its kids on top of it, if not too deep. 
      // Make a note that we did not check them yet
      if (localDepth >= depth) { 
        // localDepth == depth; current tos exhausted its possible children, mark this by pushing INVALID_ORDINAL
        ordinalStack[++localDepth] = TaxonomyReader.INVALID_ORDINAL;
        continue;
      }
      ordinalStack[++localDepth] = children[tosOrdinal];
      siblingExplored[localDepth] = Integer.MAX_VALUE;
    } // endof loop while stack is not empty

    // now generate a TempFacetResult from AACOsOfOnePartition, and consider self.
    IntermediateFacetResultWithHash tempFRWH = new IntermediateFacetResultWithHash(
        facetRequest, AACOsOfOnePartition);
    if (isSelfPartition(rootNode, facetArrays, offset)) {
      tempFRWH.isRootNodeIncluded = true;
      tempFRWH.rootNodeValue = this.facetRequest.getValueOf(facetArrays, rootNode % partitionSize);
    }
    tempFRWH.totalNumOfFacetsConsidered = totalNumOfDescendantsConsidered;
    return tempFRWH;

  }

  /**
   * Recursively count <code>ordinal</code>, whose depth is <code>currentDepth</code>, 
   * and all its descendants down to <code>maxDepth</code> (including), 
   * descendants whose value in the count arrays, <code>arrays</code>, is != 0. 
   * The count arrays only includes the current partition, from <code>offset</code>, to (exclusive) 
   * <code>endOffset</code>.
   * It is assumed that <code>ordinal</code> < <code>endOffset</code>, 
   * otherwise, not <code>ordinal</code>, and none of its descendants, reside in
   * the current partition. <code>ordinal</code> < <code>offset</code> is allowed, 
   * as ordinal's descendants might be >= <code>offeset</code>.
   * 
   * @param ordinal a facet ordinal. 
   * @param youngestChild mapping a given ordinal to its youngest child in the taxonomy (of largest ordinal number),
   * or to -1 if has no children.  
   * @param olderSibling  mapping a given ordinal to its older sibling, or to -1
   * @param partitionSize  number of ordinals in the given partition
   * @param offset  the first (smallest) ordinal in the given partition
   * @param endOffset one larger than the largest ordinal that belong to this partition
   * @param currentDepth the depth or ordinal in the TaxonomyTree (relative to rootnode of the facetRequest)
   * @param maxDepth maximal depth of descendants to be considered here (measured relative to rootnode of the 
   * facetRequest).
   * @return the number of nodes, from ordinal down its descendants, of depth <= maxDepth,
   * which reside in the current partition, and whose value != 0
   */
  private int countOnly(int ordinal, int[] youngestChild, int[] olderSibling, int partitionSize, int offset, 
      int endOffset, int currentDepth, int maxDepth) {
    int ret = 0;
    if (offset <= ordinal) {
      // ordinal belongs to the current partition
      if (0 != facetRequest.getValueOf(facetArrays, ordinal % partitionSize)) {
        ret++;
      }
    }
    // now consider children of ordinal, if not too deep
    if (currentDepth >= maxDepth) {
      return ret;
    }

    int yc = youngestChild[ordinal];
    while (yc >= endOffset) {
      yc = olderSibling[yc];
    }
    while (yc > TaxonomyReader.INVALID_ORDINAL) { // assuming this is -1, smaller than any legal ordinal
      ret += countOnly (yc, youngestChild, olderSibling, partitionSize, 
          offset, endOffset, currentDepth+1, maxDepth);
      yc = olderSibling[yc];
    }
    return ret;
  }

  /**
   * Merge several partitions' {@link IntermediateFacetResult}-s into one of the
   * same format
   * 
   * @see #mergeResults(IntermediateFacetResult...)
   */
  @Override
  public IntermediateFacetResult mergeResults(IntermediateFacetResult... tmpResults) {

    if (tmpResults.length == 0) {
      return null;
    }

    int i=0;
    // skip over null tmpResults
    for (; (i < tmpResults.length)&&(tmpResults[i] == null); i++) {}
    if (i == tmpResults.length) {
      // all inputs are null
      return null;
    }

    // i points to the first non-null input 
    int K = this.facetRequest.numResults; // number of best result in each node
    IntermediateFacetResultWithHash tmpToReturn = (IntermediateFacetResultWithHash)tmpResults[i++];

    // now loop over the rest of tmpResults and merge each into tmpToReturn
    for ( ; i < tmpResults.length; i++) {
      IntermediateFacetResultWithHash tfr = (IntermediateFacetResultWithHash)tmpResults[i];
      tmpToReturn.totalNumOfFacetsConsidered += tfr.totalNumOfFacetsConsidered;
      if (tfr.isRootNodeIncluded) {
        tmpToReturn.isRootNodeIncluded = true;
        tmpToReturn.rootNodeValue = tfr.rootNodeValue;
      }
      // now merge the HashMap of tfr into this of tmpToReturn
      IntToObjectMap<AACO> tmpToReturnMapToACCOs = tmpToReturn.mapToAACOs;
      IntToObjectMap<AACO> tfrMapToACCOs = tfr.mapToAACOs;
      IntIterator tfrIntIterator = tfrMapToACCOs.keyIterator();
      //iterate over all ordinals in tfr that are maps to their children
      while (tfrIntIterator.hasNext()) {
        int tfrkey = tfrIntIterator.next();
        AACO tmpToReturnAACO = null;
        if (null == (tmpToReturnAACO = tmpToReturnMapToACCOs.get(tfrkey))) {
          // if tmpToReturn does not have any kids of tfrkey, map all the kids
          // from tfr to it as one package, along with their redisude
          tmpToReturnMapToACCOs.put(tfrkey, tfrMapToACCOs.get(tfrkey));
        } else {
          // merge the best K children of tfrkey as appear in tmpToReturn and in tfr
          AACO tfrAACO = tfrMapToACCOs.get(tfrkey);
          int resLength = tfrAACO.ordinals.length + tmpToReturnAACO.ordinals.length;
          if (K < resLength) {
            resLength = K;
          }
          int[] resOrds = new int [resLength];
          double[] resVals = new double [resLength];
          int indexIntoTmpToReturn = 0;
          int indexIntoTFR = 0;
          ACComparator merger = getSuitableACComparator(); // by facet Request
          for (int indexIntoRes = 0; indexIntoRes < resLength; indexIntoRes++) {
            if (indexIntoTmpToReturn >= tmpToReturnAACO.ordinals.length) {
              //tmpToReturnAACO (former result to return) ran out of indices
              // it is all merged into resOrds and resVal
              resOrds[indexIntoRes] = tfrAACO.ordinals[indexIntoTFR];
              resVals[indexIntoRes] = tfrAACO.values[indexIntoTFR];
              indexIntoTFR++;
              continue;
            }
            if (indexIntoTFR >= tfrAACO.ordinals.length) {
              // tfr ran out of indices
              resOrds[indexIntoRes] = tmpToReturnAACO.ordinals[indexIntoTmpToReturn];
              resVals[indexIntoRes] = tmpToReturnAACO.values[indexIntoTmpToReturn];
              indexIntoTmpToReturn++;
              continue;
            }
            // select which goes now to res: next (ord, value) from tmpToReturn or from tfr:
            if (merger.leftGoesNow(  tmpToReturnAACO.ordinals[indexIntoTmpToReturn], 
                tmpToReturnAACO.values[indexIntoTmpToReturn], 
                tfrAACO.ordinals[indexIntoTFR], 
                tfrAACO.values[indexIntoTFR])) {
              resOrds[indexIntoRes] = tmpToReturnAACO.ordinals[indexIntoTmpToReturn];
              resVals[indexIntoRes] = tmpToReturnAACO.values[indexIntoTmpToReturn];
              indexIntoTmpToReturn++;
            } else {
              resOrds[indexIntoRes] = tfrAACO.ordinals[indexIntoTFR];
              resVals[indexIntoRes] = tfrAACO.values[indexIntoTFR];
              indexIntoTFR++;
            }
          } // end of merge of best kids of tfrkey that appear in tmpToReturn and its kids that appear in tfr
          // altogether yielding no more that best K kids for tfrkey, not to appear in the new shape of 
          // tmpToReturn

          //update the list of best kids of tfrkey as appear in tmpToReturn
          tmpToReturnMapToACCOs.put(tfrkey, new AACO(resOrds, resVals));
        } // endof need to merge both AACO -- children for same ordinal

      } // endof loop over all ordinals in tfr 
    } // endof loop over all temporary facet results to merge

    return tmpToReturn;
  }

  private static class AggregatedCategoryHeap extends PriorityQueue<AggregatedCategory> {
    
    private ACComparator merger;
    public AggregatedCategoryHeap(int size, ACComparator merger) {
      super(size);
      this.merger = merger;
    }

    @Override
    protected boolean lessThan(AggregatedCategory arg1, AggregatedCategory arg2) {
      return merger.leftGoesNow(arg2.ordinal, arg2.value, arg1.ordinal, arg1.value);
    }

  }

  private static class ResultNodeHeap extends PriorityQueue<FacetResultNode> {
    private ACComparator merger;
    public ResultNodeHeap(int size, ACComparator merger) {
      super(size);
      this.merger = merger;
    }

    @Override
    protected boolean lessThan(FacetResultNode arg1, FacetResultNode arg2) {
      return merger.leftGoesNow(arg2.ordinal, arg2.value, arg1.ordinal, arg1.value);
    }

  }

  /**
   * @return the {@link ACComparator} that reflects the order,
   * expressed in the {@link FacetRequest}, of 
   * facets in the {@link FacetResult}. 
   */

  private ACComparator getSuitableACComparator() {
    if (facetRequest.getSortOrder() == SortOrder.ASCENDING) {
      return new AscValueACComparator();
    } else {
      return new DescValueACComparator();
    }
  }

  /**
   * A comparator of two Aggregated Categories according to the order
   * (ascending / descending) and item (ordinal or value) specified in the 
   * FacetRequest for the FacetResult to be generated
   */

  private static abstract class ACComparator {
    ACComparator() { }
    protected abstract boolean leftGoesNow (int ord1, double val1, int ord2, double val2); 
  }

  private static final class AscValueACComparator extends ACComparator {
    
    AscValueACComparator() { }
    
    @Override
    protected boolean leftGoesNow (int ord1, double val1, int ord2, double val2) {
      return (val1 == val2) ? (ord1 < ord2) : (val1 < val2);
    }
  }

  private static final class DescValueACComparator extends ACComparator {
    
    DescValueACComparator() { }
    
    @Override
    protected boolean leftGoesNow (int ord1, double val1, int ord2, double val2) {
      return (val1 == val2) ? (ord1 > ord2) : (val1 > val2);
    }
  }

  /**
   * Intermediate result to hold counts from one or more partitions processed
   * thus far. Its main field, constructor parameter <i>mapToAACOs</i>, is a map
   * from ordinals to AACOs. The AACOs mapped to contain ordinals and values
   * encountered in the count arrays of the partitions processed thus far. The
   * ordinals mapped from are their parents, and they may be not contained in
   * the partitions processed thus far. All nodes belong to the taxonomy subtree
   * defined at the facet request, constructor parameter <i>facetReq</i>, by its
   * root and depth.
   */
  public static class IntermediateFacetResultWithHash implements IntermediateFacetResult {
    protected IntToObjectMap<AACO> mapToAACOs;
    FacetRequest facetRequest;
    boolean isRootNodeIncluded; // among the ordinals in the partitions 
    // processed thus far
    double rootNodeValue; // the value of it, in case encountered.
    int totalNumOfFacetsConsidered; // total number of facets 
    // which belong to facetRequest subtree and have value != 0,
    // and have been encountered thus far in the partitions processed. 
    // root node of result tree is not included in this count.

    public IntermediateFacetResultWithHash(FacetRequest facetReq,
                                    IntToObjectMap<AACO> mapToAACOs) {
      this.mapToAACOs = mapToAACOs;
      this.facetRequest = facetReq;
      this.isRootNodeIncluded = false;
      this.rootNodeValue = 0.0;
      this.totalNumOfFacetsConsidered = 0;
    }

    @Override
    public FacetRequest getFacetRequest() {
      return this.facetRequest;
    }
  } // endof FacetResultWithHash

  /**
   * Maintains info of one entry in the filled up count array:
   * an ordinal number of a category and the value aggregated for it 
   * (typically, that value is the count for that ordinal).
   */
  private static final class AggregatedCategory {
    int ordinal;
    double value;
    AggregatedCategory(int ord, double val) {
      this.ordinal = ord;
      this.value = val;
    }
  }

  /**
   * Maintains an array of <code>AggregatedCategory</code>. For space consideration, this is implemented as 
   * a pair of arrays, <i>ordinals</i> and <i>values</i>, rather than one array of pairs.
   * Enumerated in <i>ordinals</i> are siblings,  
   * potential nodes of the {@link FacetResult} tree  
   * (i.e., the descendants of the root node, no deeper than the specified depth).
   * No more than K ( = {@link FacetRequest#numResults}) 
   * siblings are enumerated.
   * @lucene.internal
   */
  protected static final class AACO {
    int [] ordinals; // ordinals of the best K children, sorted from best to least
    double [] values; // the respective values for these children
    AACO (int[] ords, double[] vals) {
      this.ordinals = ords;
      this.values = vals;
    }
  }

  @Override
  public void labelResult(FacetResult facetResult) throws IOException {
    if (facetResult == null) {
      return; // any result to label?
    }
    FacetResultNode rootNode = facetResult.getFacetResultNode();
    recursivelyLabel(rootNode, facetRequest.getNumLabel());
  }

  private void recursivelyLabel(FacetResultNode node, int numToLabel) throws IOException {
    if (node == null) {
      return;
    }
    node.label = taxonomyReader.getPath(node.ordinal);

    // recursively label the first numToLabel children of every node
    int numLabeled = 0;
    for (FacetResultNode frn : node.subResults) { 
      recursivelyLabel(frn, numToLabel);
      if (++numLabeled >= numToLabel) {
        return;
      }
    }
  }

  @Override
  // verifies that the children of each node are sorted by the order
  // specified by the facetRequest.
  // the values in these nodes may have changed due to a re-count, for example
  // following the accumulation by Sampling.
  // so now we test and re-order if necessary.
  public FacetResult rearrangeFacetResult(FacetResult facetResult) {
    PriorityQueue<FacetResultNode> nodesHeap = 
      new ResultNodeHeap(this.facetRequest.numResults, this.getSuitableACComparator());
    FacetResultNode topFrn = facetResult.getFacetResultNode();
    rearrangeChilrenOfNode(topFrn, nodesHeap);
    return facetResult;
  }

  private void rearrangeChilrenOfNode(FacetResultNode node, PriorityQueue<FacetResultNode> nodesHeap) {
    nodesHeap.clear(); // just to be safe
    for (FacetResultNode frn : node.subResults) {
      nodesHeap.add(frn);
    }
    int size = nodesHeap.size();
    ArrayList<FacetResultNode> subResults = new ArrayList<FacetResultNode>(size);
    while (nodesHeap.size() > 0) {
      subResults.add(0, nodesHeap.pop());
    }
    node.subResults = subResults;
    for (FacetResultNode frn : node.subResults) {
      rearrangeChilrenOfNode(frn, nodesHeap);
    }

  }

  @Override
  public FacetResult renderFacetResult(IntermediateFacetResult tmpResult) throws IOException {
    IntermediateFacetResultWithHash tmp = (IntermediateFacetResultWithHash) tmpResult;
    int ordinal = this.taxonomyReader.getOrdinal(this.facetRequest.categoryPath);
    if ((tmp == null) || (ordinal == TaxonomyReader.INVALID_ORDINAL)) {
      return null;
    }
    double value = Double.NaN;
    if (tmp.isRootNodeIncluded) {
      value = tmp.rootNodeValue;
    }
    FacetResultNode root = generateNode(ordinal, value, tmp.mapToAACOs);
    return new FacetResult(tmp.facetRequest, root, tmp.totalNumOfFacetsConsidered);
  }

  private FacetResultNode generateNode(int ordinal, double val,  IntToObjectMap<AACO> mapToAACOs) {
    FacetResultNode node = new FacetResultNode(ordinal, val);
    AACO aaco = mapToAACOs.get(ordinal);
    if (null == aaco) {
      return node;
    }
    List<FacetResultNode> list = new ArrayList<FacetResultNode>();
    for (int i = 0; i < aaco.ordinals.length; i++) {
      list.add(generateNode(aaco.ordinals[i], aaco.values[i], mapToAACOs));
    }
    node.subResults = list;
    return node;  
  }

}
