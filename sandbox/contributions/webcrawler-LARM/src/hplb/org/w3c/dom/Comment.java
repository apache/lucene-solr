/*
 * $Id$
 */

package hplb.org.w3c.dom;

/**
 * Represents the content of comments: &lt;!-- ... --&gt;
 */
public interface Comment extends Node {
    public String getData();
    public void setData(String arg);
}
