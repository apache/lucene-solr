/*
 * $Id$
 */

package hplb.org.w3c.dom;

/**
 * 
 */
public interface TreeIterator extends NodeIterator {
    public int  numChildren();
    public int  numPreviousSiblings();
    public int  numNextSiblings();
    public Node toParent();
    public Node toPreviousSibling();
    public Node toNextSibling();
    public Node toFirstChild();
    public Node toLastChild();
    public Node toNthChild();
}
