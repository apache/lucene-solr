/*
 * $Id$
 *
 * Copyright 1997 Hewlett-Packard Company
 *
 * This file may be copied, modified and distributed only in
 * accordance with the terms of the limited licence contained
 * in the accompanying file LICENSE.TXT.
 */

package hplb.xml.util;

import hplb.xml.*;
import hplb.org.w3c.dom.*;
import java.io.*;

/**
 * Reads an HTML document on System.in, "normalizes" it in a couple of ways, and
 * writes it to System.out. In the process HTML4.0 element names are converted to
 * upper case, attribute names are converted to lower case, all attribute values
 * gets enclosed in double quotes, all non-empty elements with an optional and
 * omitted end tag are given an end tag.
 *
 * @author      Anders Kristensen
 */
public class NormalizeHtml {
    static PrintStream out = System.out;

    public static void usage() {
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        /*
        Tokenizer tok = new Tokenizer();
        tok.setDocumentHandler(new NormalizeHtml());
        HTML.applyHacks(tok);
        //tok.rcgnzEntities = false;
        tok.rcgnzCDATA = false;
        tok.atomize = true;
        tok.parse(System.in);
        */
        HtmlXmlParser parser = new HtmlXmlParser();
        Tokenizer tok = parser.getTokenizer();
        tok.rcgnzEntities = false;
        tok.rcgnzCDATA = false;
        tok.rcgnzComments = false;
        tok.atomize = true;
        print(parser.parse(System.in));
    }

    public static void print(Document doc) {
        //print(doc.getDocumentElement());
        NodeIterator iter = doc.getChildNodes();
        while (iter.toNext() != null) {
            printNode(iter.getCurrent());
        }
    }

    public static void printNode(Node node) {
        if (node instanceof Document) print((Document) node);
        else if (node instanceof Element) print((Element) node);
        else if (node instanceof Text) print((Text) node);
        else System.err.println("Error: non-text, non-element node ignored.");
    }

    public static void print(Text text) {
        //out.print(encodeText(text.getData(), false));
        out.print(text.getData());
    }

    public static void print(Element elm) {
        String tagName      = elm.getTagName();
        AttributeList attrs = elm.attributes();
        boolean isHtmlElm = isHtmlElm(tagName);
        boolean isEmpty = (elm.getFirstChild() == null);
        boolean isHtmlEmptyElm =
              (tagName == HTML.AREA
            || tagName == HTML.BASE
            || tagName == HTML.BR
            || tagName == HTML.COL
            || tagName == HTML.FRAME
            || tagName == HTML.HR
            || tagName == HTML.IMG
            || tagName == HTML.LINK
            || tagName == HTML.META
            || tagName == HTML.PARAM);

        if (isHtmlElm) tagName = tagName.toUpperCase();

        // print start tag and attribute name-value pairs
        out.print("<" + tagName);
        int len = attrs.getLength();
        for (int i = 0; i < len; i++) {
            print(attrs.item(i), isHtmlElm);
        }
        if (isEmpty && !isHtmlEmptyElm) out.print("/");
        out.print(">");
        if (isEmpty) return;

        // print content
        NodeIterator iter = elm.getChildNodes();
        while (iter.toNext() != null) {
            printNode(iter.getCurrent());
        }

        // print end tag
        out.print("</" + tagName + ">");
    }

    public static void print(Attribute attr, boolean toLower) {
        String a = attr.getName();
        out.print(" " + (toLower ? a.toLowerCase() : a)
                + "=\"" + encodeText(attr.toString(), true) +'"');
    }

    public static String encodeText(String s, boolean attr) {
        StringBuffer sb = new StringBuffer();
        int ch, len = s.length();

        for (int i = 0; i < len; i++) {
            ch = s.charAt(i);
            if (ch == '"') sb.append("&quot;");
            /* cause we don't recognize markup within PCDATA and attr values
            else if (ch == '&') sb.append("&amp;");
            else if (!attr && ch == '<') sb.append("&lt;");
            else if (!attr && ch == '>') sb.append("&gt;");
            else if ((" \r\n\t".indexOf((char) ch) != -1)
                     && (ch <= 31 || ch >= 127)) sb.append("&#"+ch+";");
            */
            else sb.append((char) ch);
        }
        return sb.toString();
    }

    public static boolean isHtmlElm(String tagName) {
        int len = HTML.elements.length;
        for (int i = 0; i < len; i++) {
            if (tagName == HTML.elements[i]) return true;
        }
        return false;
    }
}
