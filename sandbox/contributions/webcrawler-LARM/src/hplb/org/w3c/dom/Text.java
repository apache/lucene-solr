/*
 * $Id$
 */

package hplb.org.w3c.dom;

/**
 * 
 */
public interface Text extends Node {
    public String getData();
    public void setData(String arg);

    public void append(String data);
    public void insert(int offset, String data);
    public void delete(int offset, int count);
    public void replace(int offset, int count, String data);
    public void splice(Element element, int offset, int count);
}
