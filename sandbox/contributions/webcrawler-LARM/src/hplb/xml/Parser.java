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

import hplb.org.xml.sax.*;
import hplb.org.w3c.dom.*;
import java.util.*;
import java.io.*;

/**
 * Parses a stream of MarkupTokens into a tree structure.
 * Uses Tokenizer.
 * 
 * <p>This class has very shallow (no) understanding of HTML. Correct
 * handling of &lt;p&gt; tags requires some special code as does correct
 * handling of &lt;li&gt;. This parser doesn't know that an "li" tag can
 * be terminated by another "li" tag or a "ul" end tag. Hence "li" is
 * treated as an empty tag here which means that in the generated parse
 * tree the children of the "li" element are represented as siblings of it.
 * 
 * @see Tokenizer
 * @author  Anders Kristensen
 */
public class Parser implements DocumentHandler {
    // FIXME: add support for discriminate per-element whitespace handling
    
    /**
     * Set of elements which the parser will expect to be empty, i.e. it
     * will not expect an end tag (e.g. IMG, META HTML elements).
     * End tags for any of these are ignored...
     */
    protected Hashtable emptyElms = new Hashtable();
    
    /**
     * Maps element names to a list of names of other elements which
     * terminate that element. So for example "dt" might be mapped to
     * ("dt", "dd") and "p" might be mapped to all blocklevel HTML
     * elements.
     */
    protected Hashtable terminators = new Hashtable();
    protected Tokenizer tok;
    protected DOM dom;
    protected Document root;
    protected Node current;
    
    /**
     * Non-fatal errors are written to this PrintStream. Fatal errors
     * are reported as Exceptions.
     */
    PrintStream err = System.err;
    
    public Parser() {
        tok = new Tokenizer();
        tok.setDocumentHandler(this);
        dom = new DOMImpl();
    }
    
    public DOM setDOM(DOM dom) {
        DOM old = dom;
        this.dom = dom;
        return old;
    }
    
    public Tokenizer getTokenizer() {
        return tok;
    }
    
    /**
     * Add the set of HTML empty elements to the set of tags recognized
     * as empty tags.
     */
    public void addEmptyElms(String[] elms) {
        for (int i = 0; i < elms.length; i++) {
            emptyElms.put(elms[i], elms[i]);
        }
    }
    
    public void clearEmptyElmSet() {
        emptyElms.clear();
    }
    
    public boolean isEmptyElm(String elmName) {
        return emptyElms.get(elmName) != null;
    }
    
    public void setElmTerminators(String elmName, String[] elmTerms) {
        terminators.put(elmName, putIds(new Hashtable(), elmTerms));
    }
    
    public void addTerminator(String elmName, String elmTerm) {
        Hashtable h = (Hashtable) terminators.get(elmName);
        if (h == null) terminators.put(elmName, h = new Hashtable());
        h.put(elmTerm, elmTerm);
    }
    
    public static final Dictionary putIds(Dictionary dict, String[] sary) {
        for (int i = 0; i < sary.length; i++) {
            dict.put(sary[i], sary[i]);
        }
        return dict;
    }
    
    protected Document root() {
        return root;
    }
    
    public Document parse(InputStream in) throws Exception {
        root = dom.createDocument(null);
        current = root;
        tok.parse(in);
        return root();
    }
   
    public void startDocument() {}
    public void endDocument() {}
    
    // FIXME: record in root DOCUMENT the id's of elements which have one
    
    public void doctype(String name, String publicID, String systemID) {
    }
    
    public void startElement(String name, AttributeMap attributes) {
        //System.out.println("CURRENT: " + current);
        
        // does this new element terminate the current element?
        if (current != root) {
        String tagName = ((Element) current).getTagName();
        if (tagName != null) {
            Hashtable terms = (Hashtable) terminators.get(tagName);
            if (terms != null && terms.get(name) != null) {
                current = current.getParentNode();  // FIXME: could be null
            }
        }
        }
        
        Element elm = root.createElement(name, getDOMAttrs(attributes));
        // FIXME: <hr> gets written as <hr></hr> - the following line changes
        // this tp <hr/> which is even wors - we should distinguish between
        // those two types of empty elements.
        current.insertBefore(elm, null);
        if (!isEmptyElm(name)) current = elm;
    }
    
    public void endElement(String name) {
        // we go up the parse tree till we find the node which matches
        // this end tag. This mechanism elegantly handles "implicitly
        // closed" elements such as <li> being terminated by an
        // enclosing <ul> being ended.
        
        //System.out.println("CURRENT: " + current);
        
        Node node = current;
        for (;;) {
            if (node == root) {
                err.println("Stray end tag ignored: " + name +
                            " line " + tok.line + " column " + tok.column);
                return;
            } else if (name.equals(((Element) node).getTagName())) {
                current = node.getParentNode();
                return;
            } else {
                node = node.getParentNode();
            }
        }
    }
    
    public void characters(char[] ch, int start, int length) {
        current.insertBefore(
            root.createTextNode(new String(ch, start, length)), null);
    }
    
    public void ignorable (char ch[], int start, int length) {
        System.out.println("Ignorable ws: " + new String(ch, start, length));
    }
    
    public void processingInstruction(String target, String remainder) {
        // FIXME: the DOM says 2nd arg should be everything between "<?" and "?>"
        current.insertBefore(root.createPI(target, remainder), null);
    }
    
    public AttributeList getDOMAttrs(AttributeMap attrs) {
        String name;
        Node value;
        Enumeration e;
        AttributeList domAttrs = root.createAttributeList();
        
        for (e = attrs.getAttributeNames(); e.hasMoreElements(); ) {
            name = (String) e.nextElement();
            value = root.createTextNode(attrs.getValue(name));
            domAttrs.setAttribute(root.createAttribute(name, value));
        }
        return domAttrs;
    }
    
    // for debugging
    public static void main(String[] args) throws Exception {
        Parser parser = new Parser();
        Document doc = parser.parse(System.in);
        Utils.pp(doc, System.out);
    }
}
