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

/** 
 * The HtmlXmlParser is a Parser with some HTML specific <i>hacks</i>
 * applied to it which means it will more or less correctly parse most
 * HTML pages, also when they arbitrary embedded XML markup. It is
 * very forgiving as is commonly the case with HTML parsers.
 * 
 * @author  Anders Kristensen
 */
public class HtmlXmlParser extends Parser {
    public HtmlXmlParser() {
        super();
        HTML.applyHacks(this);
        tok.rcgnzCDATA = false;
    }
    
    // for debugging
    public static void main(String[] args) throws Exception {
        Parser parser = new HtmlXmlParser();
        hplb.org.w3c.dom.Document doc = parser.parse(System.in);
        Utils.pp(doc, System.out);
    }
}
