/*
 * $Id$
 */

package hplb.org.w3c.dom;

/**
 * 
 */
public interface Element extends Node {
    public String           getTagName();
    public AttributeList    attributes();
    public void             setAttribute(Attribute newAttr);
    public void             normalize();
    public NodeIterator     getElementsByTagName();
}
