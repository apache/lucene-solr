<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:output method="html" indent="yes" encoding="US-ASCII"/>
<!--
 The Apache Software License, Version 1.1

 Copyright (c) 2000-2002 The Apache Software Foundation.  All rights
 reserved.

 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions
 are met:

 1. Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.

 2. Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in
    the documentation and/or other materials provided with the
    distribution.

 3. The end-user documentation included with the redistribution, if
    any, must include the following acknowlegement:
       "This product includes software developed by the
    Apache Software Foundation (http://www.apache.org/)."
    Alternately, this acknowlegement may appear in the software itself,
    if and wherever such third-party acknowlegements normally appear.

 4. The names "The Jakarta Project", "Ant", and "Apache Software
    Foundation" must not be used to endorse or promote products derived
    from this software without prior written permission. For written
    permission, please contact apache@apache.org.

 5. Products derived from this software may not be called "Apache"
    nor may "Apache" appear in their names without prior written
    permission of the Apache Group.

 THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 SUCH DAMAGE.
 ====================================================================

 This software consists of voluntary contributions made by many
 individuals on behalf of the Apache Software Foundation.  For more
 information on the Apache Software Foundation, please see
 <http://www.apache.org/>.
 -->
 
<!--

  The purpose have this XSL is to provide a nice way to look at the output
  from the Ant XmlLogger (ie: ant -listener org.apache.tools.ant.XmlLogger )
  
  @author <a href="mailto:sbailliez@apache.org">Stephane Bailliez</a>
  
-->
<xsl:decimal-format decimal-separator="." grouping-separator="," />

<xsl:template match="/">
<html>
  <head>
    <style type="text/css">
    .bannercell {
      border: 0px;
      padding: 0px;
    }
    body {
      margin: 0;
      font:normal 100% arial,helvetica,sanserif;
      background-color:#FFFFFF;
      color:#000000;
    }
    table.status {
      font:bold 80% arial,helvetica,sanserif;
      background-color:#525D76;
      color:#ffffff;
    }
    table.log tr td, tr th {
      font-size: 80%;
    }
    .error {
      color:red;
    }
    .warn {
      color:brown;
    }
    .info {
      color:gray;
    }
    .debug{
      color:gray;
    }
    .failed {
      font-size:80%;
      background-color: red;
      color:#FFFFFF;
      font-weight: bold
    }
    .complete {
      font-size:80%;
      background-color: #525D76;
      color:#FFFFFF;
      font-weight: bold
    }
    .a td { 
      background: #efefef;
    }
    .b td { 
      background: #fff;
    }
    th, td {
      text-align: left;
      vertical-align: top;
    }
    th {
      background: #ccc;
      color: black;
    }
    table, th, td {
      border: none
    }
    h3 {
      font:bold 80% arial,helvetica,sanserif;
      background: #525D76;
      color: white;
      text-decoration: none;
      padding: 5px;
      margin-right: 2px;
      margin-left: 2px;
      margin-bottom: 0;
    }
    </style>
  </head>
  <body>
    <!-- jakarta logo -->
    <table border="0" cellpadding="0" cellspacing="0" width="100%">
    <tr>
      <td valign="top" class="bannercell">
        <a href="http://jakarta.apache.org/">
        <img src="http://jakarta.apache.org/images/jakarta-logo.gif" alt="http://jakarta.apache.org" align="left" border="0"/>
        </a>
      </td>
      <td style="text-align:right;vertical-align:bottom">
        <a href="htp://jakarta.apache.org/ant">Jakarta Ant</a>
      </td>
    </tr>
    </table>
      
    <table border="0" width="100%">
    <tr><td><hr noshade="yes" size="1"/></td></tr>
    </table>

    <xsl:apply-templates select="build"/>

    <!-- FOOTER -->
    <table width="100%">
      <tr><td><hr noshade="yes" size="1"/></td></tr>
      <tr><td>
      <div align="center"><font color="#525D76" size="-1"><em>
      Copyright &#169; 2000-2002, Apache Software Foundation
      </em></font></div>
      </td></tr>
    </table>
  </body>
</html>
</xsl:template>

<xsl:template match="build">
  <!-- build status -->
  <table width="100%">
    <xsl:attribute name="class">
      <xsl:if test="@error">failed</xsl:if>
      <xsl:if test="not(@error)">complete</xsl:if>
    </xsl:attribute>
    <tr>
      <xsl:if test="@error">
        <td nowrap="yes">Build Failed</td> 
      </xsl:if>
      <xsl:if test="not(@error)">
        <td nowrap="yes">Build Complete</td>
      </xsl:if>
        <td style="text-align:right" nowrap="yes">Total Time: <xsl:value-of select="@time"/></td>
    </tr>
    <tr>
      <td colspan="2">
        <xsl:if test="@error">
          <tt><xsl:value-of select="@error"/></tt><br/>
          <i style="font-size:80%">See the <a href="#stacktrace" alt="Click for details">stacktrace</a>.</i>
        </xsl:if>
      </td>
    </tr>
  </table>
  <table border="1" cellspacing="2" cellpadding="3" width="100%" style="font-size:80%">
    <tr class="a"><td width="1">ant.file</td><td><xsl:value-of select="substring-after(message[contains(text(),'ant.file')], '->')"/></td></tr>
    <tr class="b"><td width="1">ant.version</td><td><xsl:value-of select="substring-after(message[contains(text(),'ant.version')], '->')"/></td></tr>
    <tr class="a"><td width="1">java.version</td><td><xsl:value-of select="substring-after(message[contains(text(),'java.vm.version')], '->')"/></td></tr>
    <tr class="b"><td width="1">os.name</td><td><xsl:value-of select="substring-after(message[contains(text(),'os.name')], '->')"/></td></tr>
  </table>
  <!-- build information -->
  <h3>Build events</h3>
  <table class="log" border="1" cellspacing="2" cellpadding="3" width="100%">
  <tr>
    <th nowrap="yes" align="left" width="1%">target</th>
    <th nowrap="yes" align="left" width="1%">task</th>
    <th nowrap="yes" align="left">message</th>
  </tr>
  <xsl:apply-templates select=".//message[@priority != 'debug']"/>
  </table>
  <p>
  <!-- stacktrace -->
  <xsl:if test="stacktrace">
  <a name="stacktrace"/>
  <h3>Error details</h3>
  <table width="100%">
    <tr><td>
      <pre><xsl:value-of select="stacktrace"/></pre>
    </td></tr>
  </table>
  </xsl:if>
  </p>
</xsl:template>

<!-- report every message but those with debug priority -->
<xsl:template match="message[@priority!='debug']">
  <tr valign="top">
    <!-- alternated row style -->
    <xsl:attribute name="class">
      <xsl:if test="position() mod 2 = 1">a</xsl:if>
      <xsl:if test="position() mod 2 = 0">b</xsl:if>
    </xsl:attribute>
    <td nowrap="yes" width="1%"><xsl:value-of select="../../@name"/></td>
    <td nowrap="yes" style="text-align:right" width="1%">[ <xsl:value-of select="../@name"/> ]</td>
    <td class="{@priority}" nowrap="yes">
            <xsl:value-of select="text()"/>
    </td>
  </tr>
</xsl:template>

</xsl:stylesheet>
