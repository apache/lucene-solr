/*
 * $Id$
 */

package hplb.org.w3c.dom;

/**
 * 
 */
public interface Node {
    // NodeType
    public static final int DOCUMENT             = 1;
    public static final int ELEMENT              = 2;
    public static final int ATTRIBUTE            = 3;
    public static final int PI                   = 4;
    public static final int COMMENT              = 5;
    public static final int TEXT                 = 6;
    
    public int              getNodeType();
    public Node             getParentNode();
    public NodeIterator     getChildNodes();
    public boolean          hasChildNodes();
    public Node             getFirstChild();
    public Node             getPreviousSibling();
    public Node             getNextSibling();
    public Node             insertBefore(Node newChild, Node refChild);
    public Node             replaceChild(Node newChild, Node oldChild);
    public Node             removeChild(Node oldChild);
}
