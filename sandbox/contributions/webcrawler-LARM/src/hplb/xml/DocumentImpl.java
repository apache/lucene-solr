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
 */
public class DocumentImpl extends NodeImpl implements Document {
    DocumentContext context;
    Document        masterDoc = this;
    Node            type;
    Element         rootNode;
    
    public DocumentImpl() {
        super(Node.DOCUMENT);
    }
    
    public Document getMasterDoc() {
        return masterDoc;
    }
    public void setMasterDoc(Document arg) {
        masterDoc = arg;
    }
    
    public Node getDocumentType() {
        return type;
    }
    public void setDocumentType(Node arg) {
        type = arg;
    }
    
    public Element getDocumentElement() {
        return rootNode;
    }
    public void setDocumentElement(Element arg) {
        rootNode = arg;
    }
    
    public DocumentContext getContextInfo() {
        return context;
    }
    public void setContextInfo(DocumentContext arg) {
        context = arg;
    }
    
    public Document createDocument() {
        return new DocumentImpl();
    }
    
    public DocumentContext createDocumentContext() {
        return new DocContextImpl();
    }
    
    public Element createElement(String tagName, AttributeList attributes) {
        return new ElementImpl(tagName, attributes);
    }
    
    public Text createTextNode(String data) {
        return new TextImpl(Node.TEXT, data);
    }
    
    public Comment createComment(String data) {
        return new TextImpl(Node.COMMENT, data);
    }
    
    public PI createPI(String name, String data) {
        PI pi = new TextImpl(Node.PI, data);
        pi.setName(name);
        return pi;
    }
    
    public Attribute createAttribute(String name, Node value) {
        return new AttrImpl(name, value, true);
    }
    
    public AttributeList createAttributeList() {
        return new AttrListImpl();
    }
    
    public NodeIterator getElementsByTagName() {
        throw new NullPointerException("NOT IMPLEMENTED");
    }
    
    public String toString() {
        return "ROOT";
        /*
        if (children == null) return "";
        StringBuffer sb = new StringBuffer();
        int len = children.getLength();
        for (int i = 0; i < len; i++) {
            System.out.println(children.item(i));
        }
        return sb.toString();
        */
    }
}
