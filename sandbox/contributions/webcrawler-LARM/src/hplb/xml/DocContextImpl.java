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

public class DocContextImpl implements DocumentContext {
    Document doc;
    
    public Document getDocument() {
        return doc;
    }
    
    public void setDocument(Document arg) {
        doc = arg;
    }
}
