/*
 * $Id$
 * 
 * Copyright 1997 Hewlett-Packard Company
 * 
 * This file may be copied, modified and distributed only in
 * accordance with the terms of the limited licence contained
 * in the accompanying file LICENSE.TXT.
 * 
 * Copyright 1998 Hewlett-Packard Company
 * 
 * This file may be copied, modified and distributed only in
 * accordance with the terms of the limited licence contained
 * in the accompanying file LICENSE.TXT.
 */

package hplb.xml.util;

import java.net.*;
import java.io.*;
import java.util.Date;

/**
 * Scans an HTML Web object for embedded link and prints them on stdout.
 * <b>Usage</b>:
 * <pre>
 *  java hplb.www.client.UrlScan [-t] [-v] [-h proxy-host] [-p proxy-port] URL
 *  where -t means test validity of embedded URLs and
 *        -v means be verbose
 * </pre>
 * 
 * @author      Anders Kristensen
 */
public class UrlScanner implements HtmlObserver {

    // should use getenv and/or getProperty for these:
    static String   proxyHost;
    static String   proxyPort;
    static boolean  test;
    static boolean  verbose;
    
    public static void usage() {
        PrintStream out = System.out;
        out.println("Usage: UrlScan [-v] [-t] <baseurl>");
        out.println("Extracts URLs from System.in and writes them on stdout.");
        out.println("  -v  verbose mode");
        out.println("  -t  test links (using HTTP HEAD requests)");
        
        System.exit(1);
    }
    
    public static void main(String[] args) throws Exception {
        URL url = null;
        //HttpClient cl;
        //HttpResponse res = null;
    
        try {
            url = new URL(args[args.length-1]);
            for (int i = 0; i < args.length - 1; i++) {
                if ("-t".equals(args[i])) {
                    test = true;
                } else if ("-v".equals(args[i])) {
                    verbose = true;
                } else if ("-h".equals(args[i])) {
                    proxyHost = args[++i];
                } else if ("-p".equals(args[i])) {
                    proxyPort = args[++i];
                } else {
                    usage();
                }
            }
        } catch (Exception e) {
            usage();
        }

        //cl = new HttpClient(url);
        if (proxyHost != null) {
            System.getProperties().put("http.proxyHost", proxyHost);
        }
        if (proxyPort != null) {
            System.getProperties().put("http.proxyPort", proxyPort);
        }
        /*
        try {
            res = cl.get();
        } catch (UnknownHostException e) {
            panic("Couldn't connect to host " + e.getMessage());
        } catch (IOException e) {
            panic("I/O exception");
        } catch (Exception e) {
            panic("Error: " + e.getMessage());
        }
        */
        
        new HtmlScanner(url, new UrlScanner());
    }

    public static void panic(String reason) {
        System.out.println(reason);
        System.exit(1);
    }

    public void gotAHref(String urlStr, URL contextUrl, Object data) {
        try {
            URL url = new URL(contextUrl, urlStr);
            System.out.print(url.toExternalForm());
            if (test) testLink(url);
            System.out.println();
        } catch (Exception e) {
            if (verbose) e.printStackTrace();
        }
    }

    /** Invoked when the scanner finds an &lt;img src=""&gt; URL. */
    public void gotImgSrc(String urlStr, URL contextUrl, Object data) {
        try {
            URL url = new URL(contextUrl, urlStr);
            System.out.print(url.toExternalForm());
            if (test) testLink(url);
            System.out.println();
        } catch (Exception e) {
            if (verbose) e.printStackTrace();
        }
    }

    /** Invoked when the scanner finds a &lt;base href=""&gt; URL. */
    public void gotBaseHref(String urlStr, URL contextUrl, Object data ) {
        if (verbose) {
            System.out.println("gotBASEHREF: " + urlStr);
            System.out.println("               " + contextUrl);
        }
    }

    /** Invoked when the scanner finds a &lt;area href=""&gt; URL. */
    public void gotAreaHref(String urlStr, URL contextUrl, Object data ) {
        if (verbose) {
            System.out.println("gotAreaHref:   " + urlStr);
            System.out.println("               " + contextUrl);
        }
    }

    /** Invoked when the scanner finds an &lt;frame src=""&gt; URL. */
    public void gotFrameSrc(String urlStr, URL contextUrl, Object data ) {
        try {
            URL url = new URL(contextUrl, urlStr);
            System.out.print(url.toExternalForm());
            if (test) testLink(url);
            System.out.println();
        } catch (Exception e) {
            if (verbose) e.printStackTrace();
        }
    }
    
    public static void testLink(URL url) throws IOException {
        throw new IOException("Not implemented");
        /*
        HttpClient cl = new HttpClient(url);
        if (proxyHost != null)
            cl.setProxyAddr(proxyHost, proxyPort);
        HttpResponse res = cl.head();

        System.out.print(" " + res.getStatusCode());
        if (verbose) System.out.print(" " + res.getReason());
        */
    }
}
