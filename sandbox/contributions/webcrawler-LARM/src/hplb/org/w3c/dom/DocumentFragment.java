/*
 * $Id$
 */

package hplb.org.w3c.dom;

/**
 * 
 */
public interface DocumentFragment extends Node {
    public Document getMasterDoc();
    public void setMasterDoc(Document arg);
}
