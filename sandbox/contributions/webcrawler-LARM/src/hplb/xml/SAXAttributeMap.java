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

import hplb.org.xml.sax.AttributeMap;
import java.util.Enumeration;

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
public class SAXAttributeMap implements AttributeMap {
    
    /** The list of keys. */
    public String[] keys;
    
    /** List of values associated with keys. */
    public String[] elms;
    
    /**
     * Number of elements in the Dictionary.
     * The elements are held at indices 0 to n in the keys and elms arrays.
     */
    public int n = 0;
    
    public SAXAttributeMap() {
        this(5);
    }
    
    /**
     * Create a SAXAttributeMap with the specififed initial cpacity.
     */
    public SAXAttributeMap(int size) {
        if (size <= 0) throw new IllegalArgumentException(
                "Initial size must be at least 1");
        keys = new String[size];
        elms = new String[size];
    }
    
    /** Returns the number of keys in this dictionary. */
    public synchronized int size() {
        return n;
    }
    
    /** Returns true if this dictionary maps no keys to value. */
    public synchronized boolean isEmpty() {
        return size() == 0;
    }
    
    /**
     * Returns an enumeration of the keys in this dictionary. 
     */
    public Enumeration getAttributeNames() {
        return new SAXAttributeEnum(keys, n);
    }

    /**
     * Returns the value to which the key is mapped in this dictionary. 
     */
    public synchronized String getValue(String key) {
        int i = getIndex(key);
        return (i < 0 ? null : elms[i]);
    }
    
    protected int getIndex(String key) {
        for (int i = 0; i < n; i++) {
            if (keys[i].equals(key))
                return i;
        }
        return -1;
    }

    /**
     * Maps the specified key to the specified value in this dictionary.
     * Neither the key nor the value can be null. 
     * 
     * <p>The value can be retrieved by calling the get method with a key
     * that is equal to the original key. 
     * @return  the previous value to which the key was mapped in
     *          this dictionary, or null if the key did not have a
     *          previous mapping.
     * @throws NullPointerException  if the key or value is null
     */
    public synchronized String put(String key, String value) {
        if (value == null) throw new NullPointerException("value is null");
        int i = getIndex(key);
        if (i >= 0) {
            String old = elms[i];
            elms[i] = value;
            return old;
        }
        int len = keys.length;
        if (len == n) {
            // double size of key,elms arrays
            String[] k, e;
            k = new String[len * 2];
            e = new String[len * 2];
            System.arraycopy(keys, 0, k, 0, len);
            System.arraycopy(elms, 0, e, 0, len);
            keys = k;
            elms = e;
        }
        keys[n] = key;
        elms[n] = value;
        n++;
        return null;
    }
  
  public void clear() {
    n = 0;
  }
    
  public boolean isEntity (String aname) { return false; }
  public boolean isNotation (String aname) { return false; }
  public boolean isId (String aname) { return false; }
  public boolean isIdref (String aname) { return false; }
  public String getEntityPublicID (String aname) { return null; }
  public String getEntitySystemID (String aname) { return null; }
  public String getNotationName (String aname) { return null; }
  public String getNotationPublicID (String aname) { return null; }
  public String getNotationSystemID (String aname) { return null; }

    public synchronized String toString() {
        StringBuffer sb = new StringBuffer();
        boolean f = true;
        
        sb.append("{ ");
        for (Enumeration e = getAttributeNames(); e.hasMoreElements(); ) {
            if (f) { f = false; }
            else { sb.append(", "); }
            String key = (String) e.nextElement();
            sb.append("" + key + '=' + getValue(key));
        }
        sb.append(" }");
        return sb.toString();
    }
    
    /*
    // for testing
    public static void main(String[] args) throws Exception {
        SAXAttributeMap d;
        java.io.BufferedReader r;
        java.util.StringTokenizer tok;
        String op;
        
        if (args.length > 1) {
            d = new SAXAttributeMap(Integer.parseInt(args[0]));
        } else {
            d = new SAXAttributeMap();
        }
        
        System.out.println(
            "Enter operations... op's are one of\n"+
            "put <key> <val>\n"+
            "get <key>\n"+
            "enum\n"+
            "size\n"+
            "quit\n");
        
        r = new java.io.BufferedReader(
                new java.io.InputStreamReader(System.in)); 
        while (true) {
            System.out.print("doyourworst> ");
            tok = new java.util.StringTokenizer(r.readLine());
            op = tok.nextToken();
            if ("put".equals(op)) {
                System.out.println("Value: " +
                                   d.put(tok.nextToken(), tok.nextToken()));
            } else if ("get".equals(op)) {
                System.out.println("Value: " + d.getValue(tok.nextToken()));
            } else if ("enum".equals(op)) {
                for (Enumeration e = d.getAttributeNames();
                     e.hasMoreElements(); ) {
                    System.out.println("" + e.nextElement() + " ");
                }
            } else if (op.startsWith("s")) {
                System.out.println("Size: " + d.size());
            } else if (op.startsWith("q")) {
                break;
            } else {
                System.out.println("Unrecognized op: " + op);
            }
            
            System.out.println("Dictionary: " + d);
            System.out.println("Size: " + d.size());
            System.out.println();
        }
    }
    */
}

class SAXAttributeEnum implements Enumeration {
    String[] objs;
    int i = 0, n;
    
    public SAXAttributeEnum(String[] objs, int n) {
        this.objs = objs;
        this.n = n;
    }
    
    public boolean hasMoreElements() {
        return i < n;
    }
    
    public Object nextElement() {
        return objs[i++];
    }
}
