/*****************************************************************************
 * Copyright (C) The Krysalis project. All rights reserved.                  *
 * ------------------------------------------------------------------------- *
 * This software is published under the terms of the Krysalis Patchy         *
 * Software License version 1.1_01, a copy of which has been included        *
 * at the bottom of this file.                                               *
 *****************************************************************************/
package org.krysalis.centipede.ant.task;

import com.izforge.izpress.*;

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
 * Task to compress html size.
 *
 * @author <a href="mailto:barozzi@nicolaken.com">Nicola Ken Barozzi</a>
 * @created 14 January 2002
 */
public class IZPressHtmlCompressTask extends org.apache.tools.ant.Task {

  private String src;
  private String dest;
  private Compressor compressor;
  private CompressorConfig conf;
  private String wipeComments = "true";
  private String wipeBorders = "true";
  private String wipeReturns = "true";
  private String wipeSpaces = "true";

   PrintWriter pw;
  /**
   * Constructor.
   */
  public IZPressHtmlCompressTask() {
    super();
  }

  /**
   * Initializes the task.
   */
  public void init() {
    super.init();
    // Setup an instance of IZCompressor.
   conf = 
     new CompressorConfig(false,false,true,true);
  }

  /**
   * Run the task.
   * @exception org.apache.tools.ant.BuildException The exception raised during task execution.
   */
  public void execute() throws org.apache.tools.ant.BuildException {

    try{

      FileInputStream in = new FileInputStream(src);
      FileOutputStream out = new FileOutputStream(dest);

      compressor = new Compressor(in,out,conf);
      
      compressor.compress();

      out.flush();
      in.close();     
      out.close();      

    }
    catch(IOException ioe)
    {
      throw new BuildException(ioe);
    }
  }

  public void setSrc(String src) {
    this.src = src;
  }

  public void setDest(String dest) {
    this.dest = dest;
  }


  public void setWipeComments(String wipeComments) {
    this.wipeComments = wipeComments;
  }


  public void setWipeBorders(String wipeBorders) {
    this.wipeBorders = wipeBorders;
  }
  
    public void setWipeReturns(String wipeReturns) {
    this.wipeReturns = wipeReturns;
  }


  public void setWipeSpaces(String wipeSpaces) {
    this.wipeSpaces = wipeSpaces;
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
