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
public class NodeListImpl {
    protected Node[] elms;
    protected int count = 0;
    
    public NodeListImpl() {
        this(5);
    }
    
    public NodeListImpl(int size) {
        if (size <= 0) throw new IllegalArgumentException(
                "Initial size of must be at least 1");
        elms = new Node[size];
    }
    
    public synchronized void add(Node node) {
        int len = elms.length;
        if (len == count) {
            Node[] e = new Node[len * 2];
            System.arraycopy(elms, 0, e, 0, len);
            elms = e;
        }
        elms[count++] = node;
    }
    
    public synchronized Node replace(int index, Node replaceNode) {
        if (index < 0 || index >= count) {
            throw new IndexOutOfBoundsException(""+index);
        } else if (index == count) {
            add(replaceNode);
            return null;
        } else {
            Node node = elms[index];
            elms[index] = replaceNode;
            return node;
        }
    }
    
    // XXX: TEST THIS METHOD!!!
    public synchronized Node insert(int index, Node newNode) {
        Node res = null;
        if (index < 0 || index > count) {
            throw new IndexOutOfBoundsException(""+index);
        } else if (index == count) {
            add(newNode);
        } else {
            int len = elms.length;
            if (len == count) {
                Node[] e = new Node[len * 2];
                System.arraycopy(elms, 0, e, 0, index);
                System.arraycopy(elms, index, e, index+1, count-index);
                elms = e;
            } else {
                System.arraycopy(elms, index, elms, index+1, count-index);
            }
            res = elms[index];
            elms[index] = newNode;
            count++;
        }
        return res;
    }
    
    public NodeIterator getIterator() {
        return new NodeIteratorImpl(this);
    }
    
    public synchronized Node remove(int index) {
        if (index < 0 || index >= count) {
            throw new IndexOutOfBoundsException(""+index);
        }
        Node node = elms[index];
        System.arraycopy(elms, index+1, elms, index, count-index-1);
        count--;
        return node;
    }
    
    public synchronized Node item(int index) {
        if (index < 0 || index >= count) {
            return null;
        }
        return elms[index];
    }
    
    /** Returns the number of keys in this dictionary. */
    public synchronized int getLength() {
        return count;
    }
    
    public Node getPreviousNode(Node node) {
        for (int i = 1; i < count; i++) {
            if (elms[i] == node) return elms[i-1];
        }
        return null;
    }
    
    public Node getNextNode(Node node) {
        for (int i = 0; i < count-1; i++) {
            if (elms[i] == node) return elms[i+1];
        }
        return null;
    }
    
    public Node insertBefore(Node node, Node ref) {
        int idx = index(ref);
        if (idx > -1) insert(idx, node);
        else add(node);
        return node;
    }
    
    public Node insertAfter(Node node, Node ref) {
        int idx = index(ref);
        if (idx > -1) insert(idx+1, node);
        else add(node);
        return node;
    }
    
    public Node replace(Node node, Node ref) {
        return replace(index(ref), node);
    }
    
    public Node remove(Node node) {
        int idx = index(node);
        return (idx > -1 ? remove(idx) : null);
    }
    
    public int index(Node node) {
        for (int i = 0; i < count; i++) {
            if (elms[i] == node) return i;
        }
        return -1;
    }
    
    public synchronized String toString() {
        StringBuffer sb = new StringBuffer();
        boolean f = true;
        int count = getLength();
        
        sb.append("{ ");
        for (int i = 0; i < count; i++) {
            if (f) { f = false; }
            else { sb.append(", "); }
            sb.append(item(i).toString());
        }
        sb.append(" }");
        return sb.toString();
    }
}


// FIXME: doesn't work properly when list changed underneath iterator
// proper thing would be to use observer pattern on current element--if
// this is removed we get callback and reposition the cursor... THISISAHACK!
// FIXME synchronize on the list itself.
class NodeIteratorImpl implements NodeIterator {
    NodeListImpl nlist;
    int index;
    
    /**
     * Create iterator over the specified NodeList. The initial position
     * will be one *before* the first element. Calling toNext() will
     * position the iterator at the first element.
     */
    public NodeIteratorImpl(NodeListImpl list) {
        nlist = list;
        index = -1;
    }
    
    public int getLength() {
        return nlist.getLength();
    }
    
    public Node getCurrent() {
        return (index >= 0 && index < nlist.count) ? nlist.item(index) : null;
    }
    
    public Node toNext() {
        if (index < nlist.count) index++;
        return getCurrent();
    }
    
    public Node toPrevious() {
        if (index >= 0) index--;
        return getCurrent();
    }
    
    public Node toFirst() {
        index = 0;
        return getCurrent();
    }
    
    public Node toLast() {
        index = nlist.count;
        return getCurrent();
    }
    
    public Node toNth(int Nth) {
        index = Nth;
        return getCurrent();
    }
    
    // FIXME: multi-threading problems here... (race condition)
    public Node toNode(Node destNode) {
        int idx = nlist.index(destNode);
        return (idx >= 0 ? toNth(idx) : null);
    }
}
