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
import org.w3c.dom.DOMException;
import org.w3c.dom.NamedNodeMap;

import javax.xml.parsers.DocumentBuilder; 
import javax.xml.parsers.DocumentBuilderFactory;  
import javax.xml.parsers.FactoryConfigurationError;  
import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;  
import org.xml.sax.SAXParseException;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.Property;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.Property;

import org.apache.tools.ant.Task;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.AntClassLoader;
import org.apache.tools.ant.ProjectHelper;
import org.apache.tools.ant.types.Path;
import org.apache.tools.ant.types.Reference;

/**
 * Task get property values from a valid xml file.
 * Example:
 *   <root-tag>
 *     <inner-tag>Text</inner-tag>
 *     <2><3><4>false</4></3></2>  
 *   </root-tag>
 *    
 *  myattr=true
 *  inner-tag=Text
 *  2.3.4=false
 *  
 * @author <a href="mailto:barozzi@nicolaken.com">Nicola Ken Barozzi</a>
 * @created 14 January 2002
 */

public class XmlPropertyTask extends org.apache.tools.ant.Task
{
    private String src;
    private String prefix = "";
    private boolean keepRoot = true;
    private org.w3c.dom.Document document;
    
    /**
     * Constructor.
     */
    public XmlPropertyTask()
    {
        super();
    }

    /**
     * Initializes the task.
     */

    public void init()
    {
        super.init();
    }

    /**
     * Run the task.
     * @exception org.apache.tools.ant.BuildException The exception raised during task execution.
     */
    public void execute()
        throws org.apache.tools.ant.BuildException
    {   
        BufferedInputStream configurationStream = null;
      
        try
        {
            configurationStream     =
                new BufferedInputStream(new FileInputStream(src));
                
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            
            factory.setValidating(false);
            factory.setNamespaceAware(false);
    
            DocumentBuilder builder = factory.newDocumentBuilder();
            document = builder.parse( configurationStream );

            Element topElement = document.getDocumentElement();
            NodeList topChildren = topElement.getChildNodes();
            int numChildren = topChildren.getLength();

            log("Using prefix: \""+prefix+"\"", Project.MSG_VERBOSE);
                         
            String prefixToUse = "";
            
            if(!(prefix.equals("")))
            {
              prefixToUse = prefix;
            } 

            log("Prefix to use 1: \""+prefixToUse+"\"", Project.MSG_VERBOSE);
            
            if((!(prefix.equals("")))&&keepRoot)
            {
              prefixToUse += ".";            
            }
            
            log("Prefix to use 2: \""+prefixToUse+"\"", Project.MSG_VERBOSE);
            
            if(keepRoot)
            {
              prefixToUse += (topElement.getNodeName());
            } 

            log("Prefix to use 3: \""+prefixToUse+"\"", Project.MSG_VERBOSE);
                        
            for(int i=0; i<numChildren; i++)
            {
              addNodeRecursively(topChildren.item(i),prefixToUse, 0);            
            }
            
        } catch (SAXException sxe) {
           // Error generated during parsing
           Exception  x = sxe;
           if (sxe.getException() != null)
               x = sxe.getException();
           throw new BuildException(x);
    
        } catch (ParserConfigurationException pce) {
           // Parser with specified options can't be built
            throw new BuildException(pce);
        } catch (IOException ioe) {
           // I/O error
            throw new BuildException(ioe);
        }
        finally
        { 
          if(configurationStream!=null){
            try{configurationStream.close();}catch(Exception e){}}
        }
    }

    
    void addNodeRecursively(org.w3c.dom.Node node, String text, int index) {

        if (node.hasAttributes()) {
            org.w3c.dom.NamedNodeMap nodeAttributes = node.getAttributes();
            for (int i = 0; i < nodeAttributes.getLength(); i++) {
               Node attributeNode =  nodeAttributes.item(i);
               String attributeName = text+(text.trim().equals("")?"":".")+node.getNodeName()+"."+attributeNode.getNodeName();
               String attributeValue = attributeNode.getNodeValue();               
               log(attributeName+":"+attributeValue, Project.MSG_VERBOSE);
               project.setUserProperty(attributeName,attributeValue);               
            }
        }
       
        if(node.getNodeType()==Node.TEXT_NODE){
          String nodeText = node.getNodeValue();
          if(nodeText.trim().length()!=0)
          {  
             log(text+":"+nodeText, Project.MSG_VERBOSE);
             if(index==0){
               project.setUserProperty(text,nodeText);
             }
             
             project.setUserProperty(text+"["+String.valueOf(index)+"]",nodeText);             
          }
        }
               
          if (node.hasChildNodes()) {
             text+=((text.trim().equals("")?"":".")+node.getNodeName());
             org.w3c.dom.NodeList nodeChildren = node.getChildNodes();

             int numChildren = nodeChildren.getLength();

             StringBuffer childList = new StringBuffer();
                                
             for (int i = 0; i < numChildren; i++) {
                 if(i!=0){childList.append(",");}         
                 childList.append(node.getNodeName()+"["+String.valueOf(index)+"]");  
                 addNodeRecursively(nodeChildren.item(i), text, i);
             }
             
             project.setUserProperty(text+".list",childList.toString());     
             
         }
    }
    
    public void setFile(String src)
    {
        this.src = src.trim();
    }

    public void setPrefix(String prefix)
    {
          this.prefix = prefix.trim();
    }

    public void setKeeproot(boolean keepRoot)
    {
          this.keepRoot = keepRoot;
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
