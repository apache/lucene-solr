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

import hplb.org.w3c.dom.DOM;
import hplb.org.w3c.dom.Document;

public class DOMImpl implements DOM {
    public Document createDocument(String type) {
        return new DocumentImpl();
    }
    public boolean hasFeature(String feature) {
        return false;
    }
}
