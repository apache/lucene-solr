/*
 * $Id$
 */

package hplb.org.w3c.dom;

/**
 * 
 */
public interface NodeIterator {
    public int  getLength();
    public Node getCurrent();
    public Node toNext();
    public Node toPrevious();
    public Node toFirst();
    public Node toLast();
    public Node toNth(int Nth);
    public Node toNode(Node destNode);
}
