/*
 * $Id$
 */

package hplb.org.w3c.dom;

/**
 * Processing Instruction
 */
public interface PI extends Node {
    public String   getName();
    public void     setName(String arg);
    
    public String   getData();
    public void     setData(String arg);
}
