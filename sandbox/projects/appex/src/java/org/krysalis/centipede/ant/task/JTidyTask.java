/*****************************************************************************
 * Copyright (C) The Krysalis project. All rights reserved.                  *
 * ------------------------------------------------------------------------- *
 * This software is published under the terms of the Krysalis Patchy         *
 * Software License version 1.1_01, a copy of which has been included        *
 * at the bottom of this file.                                               *
 *****************************************************************************/
package org.krysalis.centipede.ant.task;
 
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.Writer;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;

import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.tidy.Tidy;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.Property;

/**
 * Task to ask property values to the user. Uses current value as default.
 *
 * @author <a href="mailto:barozzi@nicolaken.com">Nicola Ken Barozzi</a>
 * @created 14 January 2002
 */

public class JTidyTask
    extends org.apache.tools.ant.Task
{
    private String src;
    private String dest;
    private String log;
    private Tidy   tidy;
    private String warn    = "false";
    private String summary = "false";
    PrintWriter    pw;

    /**
     * Constructor.
     */

    public JTidyTask()
    {
        super();
    }

    /**
     * Initializes the task.
     */

    public void init()
    {
        super.init();

        // Setup an instance of Tidy.
        tidy = new Tidy();
        tidy.setXmlOut(true);
        tidy.setXHTML(true);
        tidy.setDropFontTags(true);
        tidy.setLiteralAttribs(true);
        tidy.setMakeClean(true);
        tidy.setShowWarnings(Boolean.getBoolean(warn));
        tidy.setQuiet(!Boolean.getBoolean(summary));
    }

    /**
     * Run the task.
     * @exception org.apache.tools.ant.BuildException The exception raised during task execution.
     */

    public void execute()
        throws org.apache.tools.ant.BuildException
    {
        try
        {
            PrintWriter pw = new PrintWriter(new FileWriter(log));

            tidy.setErrout(pw);

            // Extract the document using JTidy and stream it.
            BufferedInputStream  in     =
                new BufferedInputStream(new FileInputStream(src));

            // FileOutputStream out = new FileOutputStream(dest);
            PrintWriter          out    =
                new PrintWriter(new FileWriter(dest));

            // using null as output to get dom so to remove duplicate attributes
            org.w3c.dom.Document domDoc = tidy.parseDOM(in, null);

            domDoc.normalize();
            stripDuplicateAttributes(domDoc, null);
            org.apache.xml.serialize.OutputFormat format =
                new org.apache.xml.serialize.OutputFormat();

            format.setIndenting(true);
            format.setEncoding("ISO-8859-1");
            format.setPreserveSpace(true);
            format.setLineSeparator("\n");
            org.apache.xml.serialize.XMLSerializer serializer =
                new org.apache.xml.serialize.XMLSerializer(out, format);

            serializer.serialize(domDoc);
            out.flush();
            out.close();
            in.close();
            pw.flush();
            pw.close();
        }
        catch (IOException ioe)
        {
            throw new BuildException(ioe);
        }
    }

    public void setSrc(String src)
    {
        this.src = src;
    }

    public void setDest(String dest)
    {
        this.dest = dest;
    }

    public void setLog(String log)
    {
        this.log = log;
    }

    public void setWarn(String warn)
    {
        this.warn = warn;
    }

    public void setSummary(String summary)
    {
        this.summary = summary;
    }

    // using parent because jtidy dom is bugged, cannot get parent or delete child
    public static void stripDuplicateAttributes(Node node, Node parent)
    {

        // The output depends on the type of the node
        switch (node.getNodeType())
        {

            case Node.DOCUMENT_NODE :
            {
                Document doc   = ( Document ) node;
                Node     child = doc.getFirstChild();

                while (child != null)
                {
                    stripDuplicateAttributes(child, node);
                    child = child.getNextSibling();
                }
                break;
            }
            case Node.ELEMENT_NODE :
            {
                Element      elt              = ( Element ) node;
                NamedNodeMap attrs            = elt.getAttributes();
                ArrayList    nodesToRemove    = new ArrayList();
                int          nodesToRemoveNum = 0;

                for (int i = 0; i < attrs.getLength(); i++)
                {
                    Node a = attrs.item(i);

                    for (int j = 0; j < attrs.getLength(); j++)
                    {
                        Node b = attrs.item(j);

                        // if there are two attributes with same name
                        if ((i != j)
                                && (a.getNodeName().equals(b.getNodeName())))
                        {
                            nodesToRemove.add(b);
                            nodesToRemoveNum++;
                        }
                    }
                }
                for (int i = 0; i < nodesToRemoveNum; i++)
                {
                    org.w3c.dom.Attr    nodeToDelete       =
                        ( org.w3c.dom.Attr ) nodesToRemove.get(i);
                    org.w3c.dom.Element nodeToDeleteParent =
                        ( org.w3c.dom
                            .Element ) node;   // nodeToDelete.getParentNode();

                    nodeToDeleteParent.removeAttributeNode(nodeToDelete);
                }
                nodesToRemove.clear();
                Node child = elt.getFirstChild();

                while (child != null)
                {
                    stripDuplicateAttributes(child, node);
                    child = child.getNextSibling();
                }
                break;
            }
            default :

                // do nothing
                break;
        }
    }
}

/*
The Krysalis Patchy Software License, Version 1.1_01
Copyright (c) 2002 Nicola Ken Barozzi.  All rights reserved.

This Licence is compatible with the BSD licence as described and 
approved by http://www.opensource.org/, and is based on the
Apache Software Licence Version 1.1.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

 1. Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in
   the documentation and/or other materials provided with the
   distribution.

3. The end-user documentation included with the redistribution,
   if any, must include the following acknowledgment:
      "This product includes software developed for project 
       Krysalis (http://www.krysalis.org/)."
   Alternately, this acknowledgment may appear in the software itself,
   if and wherever such third-party acknowledgments normally appear.

4. The names "Krysalis" and "Nicola Ken Barozzi" and
   "Krysalis Centipede" must not be used to endorse or promote products
   derived from this software without prior written permission. For
   written permission, please contact krysalis@nicolaken.org.
   
5. Products derived from this software may not be called "Krysalis",
   "Krysalis Centipede", nor may "Krysalis" appear in their name,
   without prior written permission of Nicola Ken Barozzi.

6. This software may contain voluntary contributions made by many 
   individuals, who decided to donate the code to this project in
   respect of this licence.

THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED.  IN NO EVENT SHALL THE KRYSALIS PROJECT OR
ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
SUCH DAMAGE.
====================================================================*/
