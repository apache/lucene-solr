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
import hplb.org.xml.sax.*;
import java.io.*;

/**
 * Test of Tokenizer.
 * Usage: TokTest [-w] < html-file
 * @author  Anders Kristensen
 */
public class TokTest implements DocumentHandler {
    static Tokenizer tok;
    static PrintStream out = System.out;
    int n = 60;
    int n2 = (n-3)/2;
    
    public void startDocument () {
        out.println("START DOC");
    }
    
    public void endDocument () {
        out.println("END DOC");
    }
    
    public void doctype (String name, String publicID, String systemID) {
        out.println("DOC TYPE " + name + ", " + publicID + ", " + systemID);
    }

    public void startElement (String name, AttributeMap attributes) {
        out.println("START " + name + ", " + attributes);
    }

    public void endElement (String name) {
        out.println("END   " + name);
    }

    public void characters (char ch[], int start, int length) {
        //out.println("Chars: " + new String(ch, start, length));
        out.println("Chars: " + compact(new String(ch, start, length)));
    }

    public void ignorable (char ch[], int start, int length) {
        out.println("Ignorable: " + compact(new String(ch, start, length)));
    }

    public void processingInstruction (String name, String remainder) {
        out.println("PI: " + name + ", " + compact(remainder));
    }
    
    // Returns short description of PCDATA argument.
    public String compact(char[] buf) {
        return compact(new String(buf));
    }
    
    public String compact(String s) {
        if (s.length() < n) {
            return "[" + noCRLF(s) + "]";
        } else {
            return "[" + noCRLF(s.substring(0, n2)) + "..." +
                   noCRLF(s.substring(s.length() - n2)) + "]";
        }
    }
    
    private static String noCRLF(String s) {
        return s.replace('\r', ' ').replace('\n', ' ');
    }
    
    /**
     * Process options in 'args' vector and apply to the supplied Tokenizer.
     */
    public static void args(String[] args, Tokenizer tok) {
        // case mappoing: tags/attr names/attr values, upper/lower/depends...
        for (int i = 0; i < args.length; i++) {
            if ("-w".equals(args[i])) {
                tok.rcgnzWS = true;
            } else if ("-d".equals(args[i])) {
                tok.rcgnzComments = false;
            } else if ("-c".equals(args[i])) {
                tok.rcgnzCDATA = false;
            } else if ("-e".equals(args[i])) {
                tok.rcgnzEntities = false;
            } else if ("-h".equals(args[i])) {
                HTML.applyHacks(tok);
            } else {
                System.err.println("Unrecognized option: " + args[i]);
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        tok = new Tokenizer();
        tok.setDocumentHandler(new TokTest());
        args(args, tok);
        tok.parse(System.in);
    }
}
