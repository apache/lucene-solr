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
public final class AttrImpl implements Attribute {
    protected String name;
    protected Node value;
    protected boolean specified;
    
    public AttrImpl(String name, String value) {
        this(name, new TextImpl(Node.TEXT, value), true);
    }
    
    public AttrImpl(String name, Node value, boolean specified) {
        this.name = name;
        this.value = value;
        this.specified = specified;
    }
    
    public String getName() {
        return name;
    }
    
    public Node getValue() {
        return value;
    }
    
    public void setValue(Node arg) {
        value = arg;
    }
    
    public boolean getSpecified() {
        return specified;
    }
    
    public void setSpecified(boolean arg) {
        specified = arg;
    }
    
    public String toString() {
        return value.toString();
    }
}
