/*
 * Copyright (c) 2005 Bruno Martins
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions 
 * are met:
 * 1. Redistributions of source code must retain the above copyright 
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the organization nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.apache.lucene.search.suggest.jaspell;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Vector;
import java.util.zip.GZIPInputStream;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Implementation of a Ternary Search Trie, a data structure for storing
 * <code>String</code> objects that combines the compact size of a binary search
 * tree with the speed of a digital search trie, and is therefore ideal for
 * practical use in sorting and searching data.
 * 
 * <p>
 * This data structure is faster than hashing for many typical search problems,
 * and supports a broader range of useful problems and operations. Ternary
 * searches are faster than hashing and more powerful, too.
 * </p>
 * 
 * <p>
 * The theory of ternary search trees was described at a symposium in 1997 (see
 * "Fast Algorithms for Sorting and Searching Strings," by J.L. Bentley and R.
 * Sedgewick, Proceedings of the 8th Annual ACM-SIAM Symposium on Discrete
 * Algorithms, January 1997). Algorithms in C, Third Edition, by Robert
 * Sedgewick (Addison-Wesley, 1998) provides yet another view of ternary search
 * trees.
 * </p>
 *
 * @deprecated Migrate to one of the newer suggesters which are much more RAM efficient.
 */
@Deprecated
public class JaspellTernarySearchTrie implements Accountable {

  /**
   * An inner class of Ternary Search Trie that represents a node in the trie.
   */
  protected static final class TSTNode implements Accountable {

    /** Index values for accessing relatives array. */
    protected final static int PARENT = 0, LOKID = 1, EQKID = 2, HIKID = 3;

    /** The key to the node. */
    protected Object data;

    /** The relative nodes. */
    protected final TSTNode[] relatives = new TSTNode[4];

    /** The char used in the split. */
    protected char splitchar;

    /**
     * Constructor method.
     * 
     *@param splitchar
     *          The char used in the split.
     *@param parent
     *          The parent node.
     */
    protected TSTNode(char splitchar, TSTNode parent) {
      this.splitchar = splitchar;
      relatives[PARENT] = parent;
    }

    @Override
    public long ramBytesUsed() {
      long mem = RamUsageEstimator.shallowSizeOf(this) + RamUsageEstimator.shallowSizeOf(relatives);
      // We don't need to add parent since our parent added itself:
      for (int i=1;i<4;i++) {
        TSTNode node = relatives[i];
        if (node != null) {
          mem += node.ramBytesUsed();
        }
      }
      return mem;
    }
  }

  /**
   * Compares characters by alfabetical order.
   * 
   *@param cCompare2
   *          The first char in the comparison.
   *@param cRef
   *          The second char in the comparison.
   *@return A negative number, 0 or a positive number if the second char is
   *         less, equal or greater.
   */
  private static int compareCharsAlphabetically(char cCompare2, char cRef) {
    return Character.toLowerCase(cCompare2) - Character.toLowerCase(cRef);
  }
  
  /* what follows is the original Jaspell code. 
  private static int compareCharsAlphabetically(int cCompare2, int cRef) {
    int cCompare = 0;
    if (cCompare2 >= 65) {
      if (cCompare2 < 89) {
        cCompare = (2 * cCompare2) - 65;
      } else if (cCompare2 < 97) {
        cCompare = cCompare2 + 24;
      } else if (cCompare2 < 121) {
        cCompare = (2 * cCompare2) - 128;
      } else cCompare = cCompare2;
    } else cCompare = cCompare2;
    if (cRef < 65) {
      return cCompare - cRef;
    }
    if (cRef < 89) {
      return cCompare - ((2 * cRef) - 65);
    }
    if (cRef < 97) {
      return cCompare - (cRef + 24);
    }
    if (cRef < 121) {
      return cCompare - ((2 * cRef) - 128);
    }
    return cCompare - cRef;
  }
  */

  /**
   * The default number of values returned by the <code>matchAlmost</code>
   * method.
   */
  private int defaultNumReturnValues = -1;

  /**
   * the number of differences allowed in a call to the
   * <code>matchAlmostKey</code> method.
   */
  private int matchAlmostDiff;

  /** The base node in the trie. */
  private TSTNode rootNode;
  
  private final Locale locale;

  /**
   * Constructs an empty Ternary Search Trie.
   */
  public JaspellTernarySearchTrie() {
    this(Locale.ROOT);
  }
  
  /**
   * Constructs an empty Ternary Search Trie,
   * specifying the Locale used for lowercasing.
   */
  public JaspellTernarySearchTrie(Locale locale) {
    this.locale = locale;
  }
  
  // for loading
  void setRoot(TSTNode newRoot) {
    rootNode = newRoot;
  }
  
  // for saving
  TSTNode getRoot() {
    return rootNode;
  }

  /**
   * Constructs a Ternary Search Trie and loads data from a <code>Path</code>
   * into the Trie. The file is a normal text document, where each line is of
   * the form word TAB float.
   * 
   *@param file
   *          The <code>Path</code> with the data to load into the Trie.
   *@exception IOException
   *              A problem occured while reading the data.
   */
  public JaspellTernarySearchTrie(Path file) throws IOException {
    this(file, false);
  }

  /**
   * Constructs a Ternary Search Trie and loads data from a <code>File</code>
   * into the Trie. The file is a normal text document, where each line is of
   * the form "word TAB float".
   * 
   *@param file
   *          The <code>File</code> with the data to load into the Trie.
   *@param compression
   *          If true, the file is compressed with the GZIP algorithm, and if
   *          false, the file is a normal text document.
   *@exception IOException
   *              A problem occured while reading the data.
   */
  public JaspellTernarySearchTrie(Path file, boolean compression)
          throws IOException {
    this();
    BufferedReader in;
    if (compression)
      in = new BufferedReader(IOUtils.getDecodingReader(new GZIPInputStream(
              Files.newInputStream(file)), StandardCharsets.UTF_8));
    else in = Files.newBufferedReader(file, StandardCharsets.UTF_8);
    try {
      String word;
      int pos;
      Float occur, one = new Float(1);
      while ((word = in.readLine()) != null) {
        pos = word.indexOf("\t");
        occur = one;
        if (pos != -1) {
          occur = Float.parseFloat(word.substring(pos + 1).trim());
          word = word.substring(0, pos);
        }
        String key = word.toLowerCase(locale);
        if (rootNode == null) {
          rootNode = new TSTNode(key.charAt(0), null);
        }
        TSTNode node = null;
        if (key.length() > 0 && rootNode != null) {
          TSTNode currentNode = rootNode;
          int charIndex = 0;
          while (true) {
            if (currentNode == null) break;
            int charComp = compareCharsAlphabetically(key.charAt(charIndex),
                    currentNode.splitchar);
            if (charComp == 0) {
              charIndex++;
              if (charIndex == key.length()) {
                node = currentNode;
                break;
              }
              currentNode = currentNode.relatives[TSTNode.EQKID];
            } else if (charComp < 0) {
              currentNode = currentNode.relatives[TSTNode.LOKID];
            } else {
              currentNode = currentNode.relatives[TSTNode.HIKID];
            }
          }
          Float occur2 = null;
          if (node != null) occur2 = ((Float) (node.data));
          if (occur2 != null) {
            occur += occur2.floatValue();
          }
          currentNode = getOrCreateNode(word.trim().toLowerCase(locale));
          currentNode.data = occur;
        }
      }
    } finally {
      IOUtils.close(in);
    }
  }

  /**
   * Deletes the node passed in as an argument. If this node has non-null data,
   * then both the node and the data will be deleted. It also deletes any other
   * nodes in the trie that are no longer needed after the deletion of the node.
   * 
   *@param nodeToDelete
   *          The node to delete.
   */
  private void deleteNode(TSTNode nodeToDelete) {
    if (nodeToDelete == null) {
      return;
    }
    nodeToDelete.data = null;
    while (nodeToDelete != null) {
      nodeToDelete = deleteNodeRecursion(nodeToDelete);
      // deleteNodeRecursion(nodeToDelete);
    }
  }

  /**
   * Recursively visits each node to be deleted.
   * 
   * To delete a node, first set its data to null, then pass it into this
   * method, then pass the node returned by this method into this method (make
   * sure you don't delete the data of any of the nodes returned from this
   * method!) and continue in this fashion until the node returned by this
   * method is <code>null</code>.
   * 
   * The TSTNode instance returned by this method will be next node to be
   * operated on by <code>deleteNodeRecursion</code> (This emulates recursive
   * method call while avoiding the JVM overhead normally associated with a
   * recursive method.)
   * 
   *@param currentNode
   *          The node to delete.
   *@return The next node to be called in deleteNodeRecursion.
   */
  private TSTNode deleteNodeRecursion(TSTNode currentNode) {
    if (currentNode == null) {
      return null;
    }
    if (currentNode.relatives[TSTNode.EQKID] != null
            || currentNode.data != null) {
      return null;
    }
    // can't delete this node if it has a non-null eq kid or data
    TSTNode currentParent = currentNode.relatives[TSTNode.PARENT];
    boolean lokidNull = currentNode.relatives[TSTNode.LOKID] == null;
    boolean hikidNull = currentNode.relatives[TSTNode.HIKID] == null;
    int childType;
    if (currentParent.relatives[TSTNode.LOKID] == currentNode) {
      childType = TSTNode.LOKID;
    } else if (currentParent.relatives[TSTNode.EQKID] == currentNode) {
      childType = TSTNode.EQKID;
    } else if (currentParent.relatives[TSTNode.HIKID] == currentNode) {
      childType = TSTNode.HIKID;
    } else {
      rootNode = null;
      return null;
    }
    if (lokidNull && hikidNull) {
      currentParent.relatives[childType] = null;
      return currentParent;
    }
    if (lokidNull) {
      currentParent.relatives[childType] = currentNode.relatives[TSTNode.HIKID];
      currentNode.relatives[TSTNode.HIKID].relatives[TSTNode.PARENT] = currentParent;
      return currentParent;
    }
    if (hikidNull) {
      currentParent.relatives[childType] = currentNode.relatives[TSTNode.LOKID];
      currentNode.relatives[TSTNode.LOKID].relatives[TSTNode.PARENT] = currentParent;
      return currentParent;
    }
    int deltaHi = currentNode.relatives[TSTNode.HIKID].splitchar
            - currentNode.splitchar;
    int deltaLo = currentNode.splitchar
            - currentNode.relatives[TSTNode.LOKID].splitchar;
    int movingKid;
    TSTNode targetNode;
    if (deltaHi == deltaLo) {
      if (Math.random() < 0.5) {
        deltaHi++;
      } else {
        deltaLo++;
      }
    }
    if (deltaHi > deltaLo) {
      movingKid = TSTNode.HIKID;
      targetNode = currentNode.relatives[TSTNode.LOKID];
    } else {
      movingKid = TSTNode.LOKID;
      targetNode = currentNode.relatives[TSTNode.HIKID];
    }
    while (targetNode.relatives[movingKid] != null) {
      targetNode = targetNode.relatives[movingKid];
    }
    targetNode.relatives[movingKid] = currentNode.relatives[movingKid];
    currentParent.relatives[childType] = targetNode;
    targetNode.relatives[TSTNode.PARENT] = currentParent;
    if (!lokidNull) {
      currentNode.relatives[TSTNode.LOKID] = null;
    }
    if (!hikidNull) {
      currentNode.relatives[TSTNode.HIKID] = null;
    }
    return currentParent;
  }

  /**
   * Retrieve the object indexed by a key.
   * 
   *@param key
   *          A <code>String</code> index.
   *@return The object retrieved from the Ternary Search Trie.
   */
  public Object get(CharSequence key) {
    TSTNode node = getNode(key);
    if (node == null) {
      return null;
    }
    return node.data;
  }

  /**
   * Retrieve the <code>Float</code> indexed by key, increment it by one unit
   * and store the new <code>Float</code>.
   * 
   *@param key
   *          A <code>String</code> index.
   *@return The <code>Float</code> retrieved from the Ternary Search Trie.
   */
  public Float getAndIncrement(String key) {
    String key2 = key.trim().toLowerCase(locale);
    TSTNode node = getNode(key2);
    if (node == null) {
      return null;
    }
    Float aux = (Float) (node.data);
    if (aux == null) {
      aux = new Float(1);
    } else {
      aux = new Float(aux.intValue() + 1);
    }
    put(key2, aux);
    return aux;
  }

  /**
   * Returns the key that indexes the node argument.
   * 
   *@param node
   *          The node whose index is to be calculated.
   *@return The <code>String</code> that indexes the node argument.
   */
  protected String getKey(TSTNode node) {
    StringBuilder getKeyBuffer = new StringBuilder();
    getKeyBuffer.setLength(0);
    getKeyBuffer.append("" + node.splitchar);
    TSTNode currentNode;
    TSTNode lastNode;
    currentNode = node.relatives[TSTNode.PARENT];
    lastNode = node;
    while (currentNode != null) {
      if (currentNode.relatives[TSTNode.EQKID] == lastNode) {
        getKeyBuffer.append("" + currentNode.splitchar);
      }
      lastNode = currentNode;
      currentNode = currentNode.relatives[TSTNode.PARENT];
    }
    getKeyBuffer.reverse();
    return getKeyBuffer.toString();
  }

  /**
   * Returns the node indexed by key, or <code>null</code> if that node doesn't
   * exist. Search begins at root node.
   * 
   *@param key
   *          A <code>String</code> that indexes the node that is returned.
   *@return The node object indexed by key. This object is an instance of an
   *         inner class named <code>TernarySearchTrie.TSTNode</code>.
   */
  public TSTNode getNode(CharSequence key) {
    return getNode(key, rootNode);
  }

  /**
   * Returns the node indexed by key, or <code>null</code> if that node doesn't
   * exist. The search begins at root node.
   * 
   *@param key
   *          A <code>String</code> that indexes the node that is returned.
   *@param startNode
   *          The top node defining the subtrie to be searched.
   *@return The node object indexed by key. This object is an instance of an
   *         inner class named <code>TernarySearchTrie.TSTNode</code>.
   */
  protected TSTNode getNode(CharSequence key, TSTNode startNode) {
    if (key == null || startNode == null || key.length() == 0) {
      return null;
    }
    TSTNode currentNode = startNode;
    int charIndex = 0;
    while (true) {
      if (currentNode == null) {
        return null;
      }
      int charComp = compareCharsAlphabetically(key.charAt(charIndex),
              currentNode.splitchar);
      if (charComp == 0) {
        charIndex++;
        if (charIndex == key.length()) {
          return currentNode;
        }
        currentNode = currentNode.relatives[TSTNode.EQKID];
      } else if (charComp < 0) {
        currentNode = currentNode.relatives[TSTNode.LOKID];
      } else {
        currentNode = currentNode.relatives[TSTNode.HIKID];
      }
    }
  }

  /**
   * Returns the node indexed by key, creating that node if it doesn't exist,
   * and creating any required intermediate nodes if they don't exist.
   * 
   *@param key
   *          A <code>String</code> that indexes the node that is returned.
   *@return The node object indexed by key. This object is an instance of an
   *         inner class named <code>TernarySearchTrie.TSTNode</code>.
   *@exception NullPointerException
   *              If the key is <code>null</code>.
   *@exception IllegalArgumentException
   *              If the key is an empty <code>String</code>.
   */
  protected TSTNode getOrCreateNode(CharSequence key) throws NullPointerException,
          IllegalArgumentException {
    if (key == null) {
      throw new NullPointerException(
              "attempt to get or create node with null key");
    }
    if (key.length() == 0) {
      throw new IllegalArgumentException(
              "attempt to get or create node with key of zero length");
    }
    if (rootNode == null) {
      rootNode = new TSTNode(key.charAt(0), null);
    }
    TSTNode currentNode = rootNode;
    int charIndex = 0;
    while (true) {
      int charComp = compareCharsAlphabetically(key.charAt(charIndex),
              currentNode.splitchar);
      if (charComp == 0) {
        charIndex++;
        if (charIndex == key.length()) {
          return currentNode;
        }
        if (currentNode.relatives[TSTNode.EQKID] == null) {
          currentNode.relatives[TSTNode.EQKID] = new TSTNode(key
                  .charAt(charIndex), currentNode);
        }
        currentNode = currentNode.relatives[TSTNode.EQKID];
      } else if (charComp < 0) {
        if (currentNode.relatives[TSTNode.LOKID] == null) {
          currentNode.relatives[TSTNode.LOKID] = new TSTNode(key
                  .charAt(charIndex), currentNode);
        }
        currentNode = currentNode.relatives[TSTNode.LOKID];
      } else {
        if (currentNode.relatives[TSTNode.HIKID] == null) {
          currentNode.relatives[TSTNode.HIKID] = new TSTNode(key
                  .charAt(charIndex), currentNode);
        }
        currentNode = currentNode.relatives[TSTNode.HIKID];
      }
    }
  }

  /**
   * Returns a <code>List</code> of keys that almost match the argument key.
   * Keys returned will have exactly diff characters that do not match the
   * target key, where diff is equal to the last value passed in as an argument
   * to the <code>setMatchAlmostDiff</code> method.
   * <p>
   * If the <code>matchAlmost</code> method is called before the
   * <code>setMatchAlmostDiff</code> method has been called for the first time,
   * then diff = 0.
   * 
   *@param key
   *          The target key.
   *@return A <code>List</code> with the results.
   */
  public List<String> matchAlmost(String key) {
    return matchAlmost(key, defaultNumReturnValues);
  }

  /**
   * Returns a <code>List</code> of keys that almost match the argument key.
   * Keys returned will have exactly diff characters that do not match the
   * target key, where diff is equal to the last value passed in as an argument
   * to the <code>setMatchAlmostDiff</code> method.
   * <p>
   * If the <code>matchAlmost</code> method is called before the
   * <code>setMatchAlmostDiff</code> method has been called for the first time,
   * then diff = 0.
   * 
   *@param key
   *          The target key.
   *@param numReturnValues
   *          The maximum number of values returned by this method.
   *@return A <code>List</code> with the results
   */
  public List<String> matchAlmost(CharSequence key, int numReturnValues) {
    return matchAlmostRecursion(rootNode, 0, matchAlmostDiff, key,
            ((numReturnValues < 0) ? -1 : numReturnValues), new Vector<String>(), false);
  }

  /**
   * Recursivelly vists the nodes in order to find the ones that almost match a
   * given key.
   * 
   *@param currentNode
   *          The current node.
   *@param charIndex
   *          The current char.
   *@param d
   *          The number of differences so far.
   *@param matchAlmostNumReturnValues
   *          The maximum number of values in the result <code>List</code>.
   *@param matchAlmostResult2
   *          The results so far.
   *@param upTo
   *          If true all keys having up to and including matchAlmostDiff
   *          mismatched letters will be included in the result (including a key
   *          that is exactly the same as the target string) otherwise keys will
   *          be included in the result only if they have exactly
   *          matchAlmostDiff number of mismatched letters.
   *@param matchAlmostKey
   *          The key being searched.
   *@return A <code>List</code> with the results.
   */
  private List<String> matchAlmostRecursion(TSTNode currentNode, int charIndex,
          int d, CharSequence matchAlmostKey, int matchAlmostNumReturnValues,
          List<String> matchAlmostResult2, boolean upTo) {
    if ((currentNode == null)
            || (matchAlmostNumReturnValues != -1 && matchAlmostResult2.size() >= matchAlmostNumReturnValues)
            || (d < 0) || (charIndex >= matchAlmostKey.length())) {
      return matchAlmostResult2;
    }
    int charComp = compareCharsAlphabetically(matchAlmostKey.charAt(charIndex),
            currentNode.splitchar);
    List<String> matchAlmostResult = matchAlmostResult2;
    if ((d > 0) || (charComp < 0)) {
      matchAlmostResult = matchAlmostRecursion(
              currentNode.relatives[TSTNode.LOKID], charIndex, d,
              matchAlmostKey, matchAlmostNumReturnValues, matchAlmostResult,
              upTo);
    }
    int nextD = (charComp == 0) ? d : d - 1;
    boolean cond = (upTo) ? (nextD >= 0) : (nextD == 0);
    if ((matchAlmostKey.length() == charIndex + 1) && cond
            && (currentNode.data != null)) {
      matchAlmostResult.add(getKey(currentNode));
    }
    matchAlmostResult = matchAlmostRecursion(
            currentNode.relatives[TSTNode.EQKID], charIndex + 1, nextD,
            matchAlmostKey, matchAlmostNumReturnValues, matchAlmostResult, upTo);
    if ((d > 0) || (charComp > 0)) {
      matchAlmostResult = matchAlmostRecursion(
              currentNode.relatives[TSTNode.HIKID], charIndex, d,
              matchAlmostKey, matchAlmostNumReturnValues, matchAlmostResult,
              upTo);
    }
    return matchAlmostResult;
  }

  /**
   * Returns an alphabetical <code>List</code> of all keys in the trie that
   * begin with a given prefix. Only keys for nodes having non-null data are
   * included in the <code>List</code>.
   * 
   *@param prefix
   *          Each key returned from this method will begin with the characters
   *          in prefix.
   *@return A <code>List</code> with the results.
   */
  public List<String> matchPrefix(String prefix) {
    return matchPrefix(prefix, defaultNumReturnValues);
  }

  /**
   * Returns an alphabetical <code>List</code> of all keys in the trie that
   * begin with a given prefix. Only keys for nodes having non-null data are
   * included in the <code>List</code>.
   * 
   *@param prefix
   *          Each key returned from this method will begin with the characters
   *          in prefix.
   *@param numReturnValues
   *          The maximum number of values returned from this method.
   *@return A <code>List</code> with the results
   */
  public List<String> matchPrefix(CharSequence prefix, int numReturnValues) {
    Vector<String> sortKeysResult = new Vector<>();
    TSTNode startNode = getNode(prefix);
    if (startNode == null) {
      return sortKeysResult;
    }
    if (startNode.data != null) {
      sortKeysResult.addElement(getKey(startNode));
    }
    return sortKeysRecursion(startNode.relatives[TSTNode.EQKID],
            ((numReturnValues < 0) ? -1 : numReturnValues), sortKeysResult);
  }

  /**
   * Returns the number of nodes in the trie that have non-null data.
   * 
   *@return The number of nodes in the trie that have non-null data.
   */
  public int numDataNodes() {
    return numDataNodes(rootNode);
  }

  /**
   * Returns the number of nodes in the subtrie below and including the starting
   * node. The method counts only nodes that have non-null data.
   * 
   *@param startingNode
   *          The top node of the subtrie. the node that defines the subtrie.
   *@return The total number of nodes in the subtrie.
   */
  protected int numDataNodes(TSTNode startingNode) {
    return recursiveNodeCalculator(startingNode, true, 0);
  }

  /**
   * Returns the total number of nodes in the trie. The method counts nodes
   * whether or not they have data.
   * 
   *@return The total number of nodes in the trie.
   */
  public int numNodes() {
    return numNodes(rootNode);
  }

  /**
   * Returns the total number of nodes in the subtrie below and including the
   * starting Node. The method counts nodes whether or not they have data.
   * 
   *@param startingNode
   *          The top node of the subtrie. The node that defines the subtrie.
   *@return The total number of nodes in the subtrie.
   */
  protected int numNodes(TSTNode startingNode) {
    return recursiveNodeCalculator(startingNode, false, 0);
  }

  /**
   * Stores a value in the trie. The value may be retrieved using the key.
   * 
   *@param key
   *          A <code>String</code> that indexes the object to be stored.
   *@param value
   *          The object to be stored in the Trie.
   */
  public void put(CharSequence key, Object value) {
    getOrCreateNode(key).data = value;
  }

  /**
   * Recursivelly visists each node to calculate the number of nodes.
   * 
   *@param currentNode
   *          The current node.
   *@param checkData
   *          If true we check the data to be different of <code>null</code>.
   *@param numNodes2
   *          The number of nodes so far.
   *@return The number of nodes accounted.
   */
  private int recursiveNodeCalculator(TSTNode currentNode, boolean checkData,
          int numNodes2) {
    if (currentNode == null) {
      return numNodes2;
    }
    int numNodes = recursiveNodeCalculator(
            currentNode.relatives[TSTNode.LOKID], checkData, numNodes2);
    numNodes = recursiveNodeCalculator(currentNode.relatives[TSTNode.EQKID],
            checkData, numNodes);
    numNodes = recursiveNodeCalculator(currentNode.relatives[TSTNode.HIKID],
            checkData, numNodes);
    if (checkData) {
      if (currentNode.data != null) {
        numNodes++;
      }
    } else {
      numNodes++;
    }
    return numNodes;
  }

  /**
   * Removes the value indexed by key. Also removes all nodes that are rendered
   * unnecessary by the removal of this data.
   * 
   *@param key
   *          A <code>string</code> that indexes the object to be removed from
   *          the Trie.
   */
  public void remove(String key) {
    deleteNode(getNode(key.trim().toLowerCase(locale)));
  }

  /**
   * Sets the number of characters by which words can differ from target word
   * when calling the <code>matchAlmost</code> method.
   * <p>
   * Arguments less than 0 will set the char difference to 0, and arguments
   * greater than 3 will set the char difference to 3.
   * 
   *@param diff
   *          The number of characters by which words can differ from target
   *          word.
   */
  public void setMatchAlmostDiff(int diff) {
    if (diff < 0) {
      matchAlmostDiff = 0;
    } else if (diff > 3) {
      matchAlmostDiff = 3;
    } else {
      matchAlmostDiff = diff;
    }
  }

  /**
   * Sets the default maximum number of values returned from the
   * <code>matchPrefix</code> and <code>matchAlmost</code> methods.
   * <p>
   * The value should be set this to -1 to get an unlimited number of return
   * values. note that the methods mentioned above provide overloaded versions
   * that allow you to specify the maximum number of return values, in which
   * case this value is temporarily overridden.
   * 
   **@param num
   *          The number of values that will be returned when calling the
   *          methods above.
   */
  public void setNumReturnValues(int num) {
    defaultNumReturnValues = (num < 0) ? -1 : num;
  }

  /**
   * Returns keys sorted in alphabetical order. This includes the start Node and
   * all nodes connected to the start Node.
   * <p>
   * The number of keys returned is limited to numReturnValues. To get a list
   * that isn't limited in size, set numReturnValues to -1.
   * 
   *@param startNode
   *          The top node defining the subtrie to be searched.
   *@param numReturnValues
   *          The maximum number of values returned from this method.
   *@return A <code>List</code> with the results.
   */
  protected List<String> sortKeys(TSTNode startNode, int numReturnValues) {
    return sortKeysRecursion(startNode, ((numReturnValues < 0) ? -1
            : numReturnValues), new Vector<String>());
  }

  /**
   * Returns keys sorted in alphabetical order. This includes the current Node
   * and all nodes connected to the current Node.
   * <p>
   * Sorted keys will be appended to the end of the resulting <code>List</code>.
   * The result may be empty when this method is invoked, but may not be
   * <code>null</code>.
   * 
   *@param currentNode
   *          The current node.
   *@param sortKeysNumReturnValues
   *          The maximum number of values in the result.
   *@param sortKeysResult2
   *          The results so far.
   *@return A <code>List</code> with the results.
   */
  private List<String> sortKeysRecursion(TSTNode currentNode,
          int sortKeysNumReturnValues, List<String> sortKeysResult2) {
    if (currentNode == null) {
      return sortKeysResult2;
    }
    List<String> sortKeysResult = sortKeysRecursion(
            currentNode.relatives[TSTNode.LOKID], sortKeysNumReturnValues,
            sortKeysResult2);
    if (sortKeysNumReturnValues != -1
            && sortKeysResult.size() >= sortKeysNumReturnValues) {
      return sortKeysResult;
    }
    if (currentNode.data != null) {
      sortKeysResult.add(getKey(currentNode));
    }
    sortKeysResult = sortKeysRecursion(currentNode.relatives[TSTNode.EQKID],
            sortKeysNumReturnValues, sortKeysResult);
    return sortKeysRecursion(currentNode.relatives[TSTNode.HIKID],
            sortKeysNumReturnValues, sortKeysResult);
  }

  @Override
  public long ramBytesUsed() {
    long mem = RamUsageEstimator.shallowSizeOf(this);
    final TSTNode root = getRoot();
    if (root != null) {
      mem += root.ramBytesUsed();
    }
    return mem;
  }
}
