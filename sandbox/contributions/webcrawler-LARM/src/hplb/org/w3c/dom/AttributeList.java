/*
 * $Id$
 */

package hplb.org.w3c.dom;

/**
 * 
 */
public interface AttributeList {
    public Attribute    getAttribute(String attrName);
    public Attribute    setAttribute(Attribute attr);
    public Attribute    remove(String attrName);
    public Attribute    item(int index);
    public int          getLength();
}
