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
 * Implements management of list of children.
 * @author  Anders Kristensen
 */
public abstract class NodeImpl implements Node {
    protected int type;
    protected NodeImpl parent;
    protected NodeListImpl children = new NodeListImpl();
    
    public NodeImpl(int type) {
        this.type = type;
    }
    
    public NodeListImpl getChildren() {
        return children;
    }
    
    public int getNodeType() {
        return type;
    }
    
    public Node getParentNode() {
        return parent;
    }
    
    public NodeIterator getChildNodes() {
        return children.getIterator();
    }
    
    public boolean hasChildNodes() {
        return (children.getLength() > 0);
    }
    
    public Node getFirstChild() {
        return children.item(0);
    }
    
    public Node getPreviousSibling() {
        if (parent == null) return null;
        else return parent.children.getPreviousNode(this);
    }
    
    public Node getNextSibling() {
        if (parent == null) return null;
        else return parent.children.getNextNode(this);
    }
    
    public Node insertBefore(Node newChild, Node refChild) {
        NodeImpl node = (NodeImpl) children.insertBefore(newChild, refChild);
        if (node != null) ((NodeImpl) newChild).parent = this;
        return node;
    }
    
    public Node insertAfter(Node newChild, Node refChild) {
        NodeImpl node = (NodeImpl) children.insertAfter(newChild, refChild);
        if (node != null) ((NodeImpl) newChild).parent = this;
        return node;
    }
    
    public Node replaceChild(Node newChild, Node oldChild) {
        NodeImpl node = (NodeImpl) children.replace(newChild, oldChild);
        if (node != null) {
            node.parent = null;
            ((NodeImpl) newChild).parent = this;
        }
        return node;
    }
    
    public Node removeChild(Node oldChild) {
        NodeImpl node = (NodeImpl) children.remove(oldChild);
        if (node != null) node.parent = null;
        return node;
    }
}
