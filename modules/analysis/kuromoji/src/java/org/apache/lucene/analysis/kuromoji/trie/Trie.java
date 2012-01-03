package org.apache.lucene.analysis.kuromoji.trie;

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

public class Trie {
	
	private Node root;	// Root node of Trie

	/**
	 * Constructor
	 * Initialize Trie with empty root node
	 */
	public Trie() {
		root = new Node();
	}

	/**
	 * Add input value into Trie
	 * Before adding, it adds terminating character(\u0001) to input string
	 * @param value String to add to Trie
	 */
	public void add(String value) {
		root.add(value + DoubleArrayTrie.TERMINATING_CHARACTER);
	}

	/**
	 * Return root node which contains other nodes
	 * @return	Node
	 */
	public Node getRoot() {
		return root;
	}

	/**
	 * Trie Node
	 */
	public class Node {
		char key;						// key(char) of this node
		
		Node[] children = new Node[0];	// Array to hold children nodes

		/**
		 * Constructor
		 */
		public Node() {
		}

		/**
		 * Constructor
		 * @param key key for this node
		 */
		public Node(char key) {
			this.key = key;
		}

		/**
		 * Add string to Trie
		 * @param value String to add
		 */
		public void add(String value) {
			if (value.length() == 0) {
				return;
			}
			
			Node node = new Node(value.charAt(0));
			addChild(node).add(value.substring(1));
		}

		/**
		 * Add Node to this node as child
		 * @param newNode node to add
		 * @return added node. If a node with same key already exists, return that node.
		 */
		public Node addChild(Node newNode) {
			Node child = getChild(newNode.getKey());
			if (child == null) {
				Node[] newChildren = new Node[children.length + 1];
				System.arraycopy(children, 0, newChildren, 0, children.length);
				newChildren[newChildren.length -1] = newNode;
				children = newChildren;
				child = newNode;
			}
			return child;
		}

		/**
		 * Return the key of the node
		 * @return key
		 */
		public char getKey() {
			return key;
		}
		
		/**
		 * Check if children following this node has only single path.
		 * For example, if you have "abcde" and "abfgh" in Trie, calling this method on node "a" and "b" returns false.
		 * Calling this method on "c", "d", "e", "f", "g" and "h" returns true.
		 * @return true if it has only single path. false if it has multiple path.
		 */
		public boolean hasSinglePath() {
			switch(children.length){
			case 0:
				return true;
			case 1:
				return children[0].hasSinglePath();
			default:
				return false;
			}
		}

		/**
		 * Return children node
		 * @return Array of children nodes
		 */
		public Node[] getChildren() {
			return children;
		}

		/**
		 * Return node which has input key
		 * @param key key to look for
		 * @return node which has input key. null if it doesn't exist.
		 */
		private Node getChild(char key) {
			for (Node child : children) {
				if (child.getKey() == key) {
					return child;
				}
			}
			return null;
		}
	}
}
