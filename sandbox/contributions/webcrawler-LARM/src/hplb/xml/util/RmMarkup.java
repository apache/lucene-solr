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

import hplb.xml.Tokenizer;
import hplb.org.xml.sax.*;
import java.io.*;

public class RmMarkup extends HandlerBase {
    static Tokenizer tok;
    static Writer out = new OutputStreamWriter(System.out);

    public void characters (char ch[], int start, int length) throws IOException {
        out.write(ch, start, length);
    }

    public static void main(String[] args) throws Exception {
        tok = new Tokenizer();
        tok.setDocumentHandler(new RmMarkup());
        TokTest.args(args, tok);
        tok.parse(System.in);
        out.flush();
    }
}
