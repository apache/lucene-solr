/*
 * $Id$
 * 
 * Copyright 1997 Hewlett-Packard Company
 * 
 * This file may be copied, modified and distributed only in
 * accordance with the terms of the limited licence contained
 * in the accompanying file LICENSE.TXT.
 */

// FIXME: check parameters reasonable [within bounds]

package hplb.xml;

import hplb.org.w3c.dom.*;

/**
 * Class whose instances represent PCDATA, comments, and PIs (processing
 * instructions.
 * @author  Anders Kristensen
 */
public class TextImpl extends NodeImpl implements Text, Comment, PI {
    protected String data;
    protected String name; // only valid for PIs
    
    /**
     * Construct new leaf node whose value is textual.
     * @param type  one of Node.PI, Node.COMMENT, and Node.TEXT.
     * @param data  the PCDATA, CDATA, comment, whatever
     */
    public TextImpl(int type, String data) {
        super(type);
        this.data = data;
    }
    
    // getData/setData common for the three interfaces
    public String getData() {
        return data;
    }
    public void setData(String arg) {
        data = arg;
    }
    
    // Text specific methods:
    
    public void append(String data) {
        this.data = this.data + data;
    }
    
    public void insert(int offset, String data) {
        this.data = this.data.substring(0, offset)
                    + data
                    + this.data.substring(offset);
    }
    
    public void delete(int offset, int count) {
        this.data = this.data.substring(0, offset)
                    + this.data.substring(offset + count);
    }
    
    public void replace(int offset, int count, String data) {
        this.data = this.data.substring(0, offset)
                    + data
                    + this.data.substring(offset + count);
    }
    
    public void splice(Element element, int offset, int count) {
        if (offset <= 0) {
            parent.insertBefore(element, this);
        } else if (offset+count > data.length()) {
            parent.insertAfter(element, this);
        } else {
            Node n;
            n = new TextImpl(Node.TEXT, data.substring(offset, offset+count));
            element.insertBefore(n , null);
            n = new TextImpl(Node.TEXT, data.substring(offset+count));
            parent.insertAfter(n, this);
            data = data.substring(0, offset);
        }
    }
    
    // PI specific methods:
    public String getName() {
        return name;
    }
    public void setName(String arg) {
        name = arg;
    }
    
    protected String typeAsString() {
        switch (type) {
            case Node.PI:        return "PI";
            case Node.COMMENT:   return "COMMENT";
            case Node.TEXT:      return "TEXT";
            default:        return "UNKNOWN";
        }
    }
    
    public String toDebugString() {
        return typeAsString() +
               (data == null ? "" : Utils.compact(data));
    }
    
    public String toString() {
        switch (type) {
            case Node.PI:        return "<?" + name + " " + data + "?>";
            case Node.COMMENT:   return "<!--" + data + "-->";
            case Node.TEXT:      return data;
            default:        return "UNKNOWN";
        }
    }
}
