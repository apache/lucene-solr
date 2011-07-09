<?xml version="1.0" encoding="utf-8"?>
<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->
<!-- $Id$ -->
<!-- $URL$ -->

<xsl:stylesheet
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  version="1.0">


  <xsl:output
    method="html"
    encoding="utf-8"
    media-type="text/html"
    indent="yes"
    doctype-public="-//W3C//DTD HTML 4.01//EN"
    doctype-system="http://www.w3.org/TR/html4/strict.dtd" />


  <xsl:template match="/">
    <html>
      <head>
        <link rel="stylesheet" type="text/css" href="solr-admin.css"></link>
	<link rel="icon" href="favicon.ico" type="image/ico"></link>
	<link rel="shortcut icon" href="favicon.ico" type="image/ico"></link>
        <title>Solr Info</title>
      </head>
      <body>
        <a href=".">
	   <img border="0" align="right" height="78" width="142" src="solr_small.png" alt="Apache Solr">
	   </img>
	</a>
        <h1>Solr Info (<xsl:value-of select="solr/schema" />)</h1>
          <xsl:value-of select="solr/host" />
          <br clear="all" />
        <xsl:apply-templates/>
        <br /><br />
        <a href=".">Return to Admin Page</a>
      </body>
    </html>
  </xsl:template>

  <xsl:template match="solr">
  <table>
    <tr>
      <td>
        <H3>Category</H3>
      </td>
      <td>
        [<a href="#core">Core</a>]
        [<a href="#cache">Cache</a>]
        [<a href="#query">Query</a>]
        [<a href="#update">Update</a>]
        [<a href="#highlighting">Highlighting</a>]
        [<a href="#other">Other</a>]
      </td>
    </tr>
    <tr><td></td>
      <td>Solr Specification Version: 
          <xsl:value-of select="solr-spec-version" />
      </td>
    </tr>
    <tr><td></td>
      <td>Solr Implementation Version: 
          <xsl:value-of select="solr-impl-version" />
      </td>
    </tr>
    <tr><td></td>
      <td>Lucene Specification Version: 
          <xsl:value-of select="lucene-spec-version" />
      </td>
    </tr>
    <tr><td></td>
      <td>Lucene Implementation Version: 
          <xsl:value-of select="lucene-impl-version" />
      </td>
    </tr>
    <tr>
      <td>
      </td>
      <td>
        Current Time: <xsl:value-of select="now" />
      </td>
    </tr>
    <tr>
      <td>
      </td>
      <td>
        Server Start Time:<xsl:value-of select="start" />
      </td>
    </tr>
  </table>
  <xsl:apply-templates/>
  </xsl:template>

  <xsl:template match="solr/*" priority="-1" />

  <xsl:template match="solr/solr-info">
  <xsl:apply-templates/>
  </xsl:template>

  <xsl:template match="solr/solr-info/CORE">
    <br />
    <a name="core"><h2>Core</h2></a>
    <table>
        <tr>
          <td align="right">
            &#xa0;
          </td>
          <td>
          </td>
        </tr>
    <xsl:apply-templates/>
    </table>
  </xsl:template>

  <xsl:template match="solr/solr-info/CORE/entry">
      <xsl:for-each select="*">
        <tr>
          <td align="right">
            <strong><xsl:value-of select="name()"/>:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="."/>&#xa0;</tt>
          </td>
        </tr>
      </xsl:for-each>
        <tr>
          <td align="right">
          </td>
          <td>
          </td>
        </tr>
  </xsl:template>

  <xsl:template match="solr/solr-info/CACHE">
    <br />
    <a name="cache"><h2>Cache</h2></a>
    <table>
        <tr>
          <td align="right">
            &#xa0;
          </td>
          <td>
          </td>
        </tr>
    <xsl:apply-templates/>
    </table>
  </xsl:template>

  <xsl:template match="solr/solr-info/CACHE/entry">
      <xsl:for-each select="*">
        <tr>
          <td align="right">
            <strong><xsl:value-of select="name()"/>:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="."/>&#xa0;</tt>
          </td>
        </tr>
      </xsl:for-each>
        <tr>
          <td align="right">
          </td>
          <td>
          </td>
        </tr>
  </xsl:template>

  <xsl:template match="solr/solr-info/QUERYHANDLER">
    <br />
    <a name="query"><h2>Query Handlers</h2></a>
    <table>
        <tr>
          <td align="right">
            &#xa0;
          </td>
          <td>
          </td>
        </tr>
    <xsl:apply-templates/>
    </table>
  </xsl:template>

  <xsl:template match="solr/solr-info/QUERYHANDLER/entry">
      <xsl:for-each select="*">
        <tr>
          <td align="right">
            <strong><xsl:value-of select="name()"/>:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="."/>&#xa0;</tt>
          </td>
        </tr>
      </xsl:for-each>
        <tr>
          <td align="right">
          </td>
          <td>
          </td>
        </tr>
  </xsl:template>

  <xsl:template match="solr/solr-info/UPDATEHANDLER">
    <br />
    <a name="update"><h2>Update Handlers</h2></a>
    <table>
        <tr>
          <td align="right">
            &#xa0;
          </td>
          <td>
          </td>
        </tr>
    <xsl:apply-templates/>
    </table>
  </xsl:template>

  <xsl:template match="solr/solr-info/UPDATEHANDLER/entry">
      <xsl:for-each select="*">
        <tr>
          <td align="right">
            <strong><xsl:value-of select="name()"/>:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="."/>&#xa0;</tt>
          </td>
        </tr>
      </xsl:for-each>
        <tr>
          <td align="right">
          </td>
          <td>
          </td>
        </tr>
  </xsl:template>
  <xsl:template match="solr/solr-info/HIGHLIGHTING">
    <br />
    <a name="highlighting"><h2>Highlighting</h2></a>
    <table>
        <tr>
          <td align="right">
            &#xa0;
          </td>
          <td>
          </td>
        </tr>
    <xsl:apply-templates/>
    </table>
  </xsl:template>
  <xsl:template match="solr/solr-info/HIGHLIGHTING/entry">
      <xsl:for-each select="*">
        <tr>
          <td align="right">
            <strong><xsl:value-of select="name()"/>:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="."/>&#xa0;</tt>
          </td>
        </tr>
      </xsl:for-each>
        <tr>
          <td align="right">
          </td>
          <td>
          </td>
        </tr>
  </xsl:template>


  <xsl:template match="solr/solr-info/OTHER">
    <br />
    <a name="other"><h2>Other</h2></a>
    <table>
        <tr>
          <td align="right">
            &#xa0;
          </td>
          <td>
          </td>
        </tr>
    <xsl:apply-templates/>
    </table>
  </xsl:template>

  <xsl:template match="solr/solr-info/OTHER/entry">
      <xsl:for-each select="*">
        <tr>
          <td align="right">
            <strong><xsl:value-of select="name()"/>:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="."/>&#xa0;</tt>
          </td>
        </tr>
      </xsl:for-each>
        <tr>
          <td align="right">
          </td>
          <td>
          </td>
        </tr>
  </xsl:template>


</xsl:stylesheet>
