/*
 * $Id$
 */

package hplb.org.w3c.dom;

/**
 * 
 */
public interface Attribute {
    
    public String   getName();
    public Node     getValue();
    public void     setValue(Node arg);
    
    public boolean  getSpecified();
    public void     setSpecified(boolean arg);
    
    public String   toString();
}
