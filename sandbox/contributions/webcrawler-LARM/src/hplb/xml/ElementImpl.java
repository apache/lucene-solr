/*
 * $Id$
 * 
 * Copyright 1997 Hewlett-Packard Company
 * 
 * This file may be copied, modified and distributed only in
 * accordance with the terms of the limited licence contained
 * in the accompanying file LICENSE.TXT.
 */

package hplb.xml;

import hplb.org.w3c.dom.*;

/**
 * 
 * @author  Anders Kristensen
 */
public class ElementImpl extends NodeImpl implements Element {
    protected String tagName;
    protected AttributeList attrs;  // Note: Parser ensures this wont be null
    
    public ElementImpl(String tagName, AttributeList attributes) {
        super(Node.ELEMENT);
        this.tagName = tagName;
        attrs = attributes;
    }
    
    public String getTagName() {
        return tagName;
    }
    
    public AttributeList attributes() {
        return attrs;
    }
    
    public void setAttribute(Attribute newAttr) {
        if (attrs == null) attrs = new AttrListImpl();
        attrs.setAttribute(newAttr);
    }
    
    public void normalize() {}
    
    public NodeIterator getElementsByTagName() {
        throw new IllegalArgumentException(
            "Why wasn't this method defined by the DOM WG to take an arg???");
    }
    
    public String toString() {
        boolean empty = (children == null || children.getLength() == 0);
        return "<" + tagName + " "
               + (attrs != null ? attrs.toString() : "{}")
               + (empty ? " />" : ">");
    }
}
