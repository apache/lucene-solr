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
import java.io.*;
import java.util.*;

public class Utils {
    /** Pretty-print elm. */
    public static void pp(Node node, PrintStream out) {
        pp(node, out, 0);
    }
    
    public static void pp(Node node, PrintStream out, int indent) {
        indent(out, indent);
        out.println("" + node);
        indent += 2;
        
        NodeIterator iter = node.getChildNodes();
        Node child;
        while ((child = iter.toNext()) != null) {
            pp(child, out, indent);
        }
    }
    
    public static String compact(String s) {
        if (s.length() < 18) {
            return "[" + noCRLF(s) + "]";
        } else {
            return "[" + noCRLF(s.substring(0, 7)) + "..." +
                   noCRLF(s.substring(s.length() - 7)) + "]";
        }
    }
    
    public static String noCRLF(String s) {
        return s.replace('\r', ' ').replace('\n', ' ');
    }
    
    public static void indent(PrintStream out, int indent) {
        for (int i = 0; i < indent; i++) out.print(' ');
    }
    
    /**
     * Encode an XML attribute value. Changes &lt;"&gt; to "&amp;quote;".
     */
    public static String encAttrVal(String val) {
        if (val.indexOf('"') > -1) {
            StringBuffer sbuf = new StringBuffer();
            int offset = 0, i;
            while ((i = val.indexOf('"', offset)) > -1) {
                sbuf.append(val.substring(offset, i));
                sbuf.append("&quote;");
                offset = i+1;
            }
            sbuf.append(val.substring(offset));
            return sbuf.toString();
        }
        return val;
    }
    
    /**
     * Encode the specified String as XML PCDATA, i.e. "&lt;" is
     * encoded as "&amp;lt;" and "&amp;" is encoded as "&amp;amp;".
     */
    public static String encPCDATA(String s) {
        if (s.indexOf('<') > -1 || s.indexOf('&') > -1) {
            StringBuffer sbuf = new StringBuffer();
            int offset = 0;
            int i = s.indexOf('<', offset);
            int j = s.indexOf('&', offset);
            while (i > -1 || j > -1) {
                if (i > j) {
                    sbuf.append(s.substring(offset, i));
                    sbuf.append("&quote;");
                    offset = i+1;
                    i = s.indexOf('<', offset);
                } else {
                    sbuf.append(s.substring(offset, j));
                    sbuf.append("&quote;");
                    offset = j+1;
                    j = s.indexOf('&', offset);
                }
            }
            sbuf.append(s.substring(offset));
            return sbuf.toString();
        }
        return s;
    }
}
