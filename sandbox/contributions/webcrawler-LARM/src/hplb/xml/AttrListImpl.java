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
 * An ordered Dictionary. keys() and elements() returns Enumerations
 * which enumerate over elements in the order they were inserted.
 * Elements are stored linearly. Operations put(), get(), and remove()
 * are linear in the number of elements in the Dictionary.
 * 
 * <p>Allows direct access to elements (as an alternative to using
 * Enumerators) for speed.
 * 
 * <p>Can function as a <em>bag</em>, i.e. it can be created with a mode
 * which allows the same key to map to multiple entries. In this case 
 * operations get() and remove() operate on the <em>first</em> pair in
 * the map. Hence to get hold of all values associated with a key it is
 * necessary to use the direct access to underlying arrays.
 * 
 * @author  Anders Kristensen
 */
public class AttrListImpl implements AttributeList {
    protected Attribute[] elms;
    
    /**
     * Number of elements. The elements are held at indices 0 to n in elms.
     */
    protected int n = 0;
    
    public AttrListImpl() {
        this(2);
    }
    
    /**
     * Create an AttrListImpl with the specififed initial capacity.
     */
    public AttrListImpl(int size) {
        if (size <= 0) throw new IllegalArgumentException(
                "Initial size must be at least 1");
        elms = new Attribute[size];
    }
    
    /**
     * Returns the value to which the key is mapped in this dictionary. 
     */
    public synchronized Attribute getAttribute(String attrName) {
        int i = getIndex(attrName);
        return (i < 0 ? null : elms[i]);
    }
    
    protected int getIndex(String name) {
        for (int i = 0; i < n; i++) {
            if (elms[i].getName().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    // XXX: what if attrName != attr.getName()???
    public synchronized Attribute setAttribute(Attribute attr) {
        int i = getIndex(attr.getName());
        if (i >= 0) {
            Attribute old = elms[i];
            elms[i] = attr;
            return old;
        }
        
        int len = elms.length;
        if (len == n) {
            // double size of key,elms arrays
            AttrImpl[] e;
            e = new AttrImpl[len * 2];
            System.arraycopy(elms, 0, e, 0, len);
            elms = e;
        }
        elms[n] = attr;
        n++;
        return null;
    }
    
    public synchronized Attribute remove(String attrName) {
        int i = getIndex(attrName);
        if (i < 0) return null;
        Attribute val = elms[i];
        System.arraycopy(elms, i+1, elms, i, n-i-1);
        n--;
        return val;
    }
    
    public synchronized Attribute item(int index) {
        if (index < 0 || index >= n) {
            throw new IndexOutOfBoundsException(""+index);
        }
        return elms[index];
    }
    
    /** Returns the number of keys in this dictionary. */
    public synchronized int getLength() {
        return n;
    }
    
    public synchronized String toString() {
        StringBuffer sb = new StringBuffer();
        boolean f = true;
        int n = getLength();
        
        sb.append("{ ");
        for (int i = 0; i < n; i++) {
            if (f) { f = false; }
            else { sb.append(", "); }
            Attribute attr = item(i);
            sb.append(attr.getName() + '=' + attr);
        }
        sb.append(" }");
        return sb.toString();
    }
    
    /**/
    // for testing
    public static void main(String[] args) throws Exception {
        AttrListImpl alist;
        Attribute attr;
        java.io.BufferedReader r;
        java.util.StringTokenizer tok;
        String op;
        
        if (args.length > 1) {
            alist = new AttrListImpl(Integer.parseInt(args[0]));
        } else {
            alist = new AttrListImpl();
        }
        
        System.out.println(
            "Enter operations... op's are one of\n"+
            "put <key> <val>\n"+
            "get <key>\n"+
            "rem <key>\n"+
            "size\n"+
            "quit\n");
        
        r = new java.io.BufferedReader(
                new java.io.InputStreamReader(System.in)); 
        while (true) {
            System.out.print("doyourworst> ");
            tok = new java.util.StringTokenizer(r.readLine());
            op = tok.nextToken();
            if ("put".equals(op)) {
                attr = new AttrImpl(tok.nextToken(), tok.nextToken());
                System.out.println("Value: " +
                        alist.setAttribute(attr));
            } else if ("get".equals(op)) {
                attr = alist.getAttribute(tok.nextToken());
                System.out.println("Value: " +
                        (attr == null ? "No such element" : attr.toString()));
            } else if ("rem".equals(op)) {
                attr = alist.remove(tok.nextToken());
                System.out.println("Value: " + attr);
            } else if (op.startsWith("s")) {
                System.out.println("Size: " + alist.getLength());
            } else if (op.startsWith("q")) {
                break;
            } else {
                System.out.println("Unrecognized op: " + op);
            }
            
            System.out.println("AttributeList: " + alist);
            System.out.println("Size: " + alist.getLength());
            System.out.println();
        }
    }
    //*/
}
