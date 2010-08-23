package org.apache.solr.spelling.suggest.tst;

/**
 * The class creates a TST node.
 * @variable splitchar the character stored by a node.
 * @variable loKid a reference object to the node containing character smaller than
 * this node's character.
 * @variable eqKid a reference object to the node containg character next to this
 * node's character as occuring in the inserted token.
 * @variable hiKid a reference object to the node containing character higher than
 * this node's character.
 * @variable token used by leaf nodes to store the complete tokens to be added to 
 * suggest list while auto-completing the prefix.
 */

public class TernaryTreeNode {
	char splitchar;
	TernaryTreeNode loKid, eqKid, hiKid;
	String token;
	Object val;
}
