<?xml version="1.0" encoding="utf-8"?>

<!-- $Id: tabular.xsl,v 1.2 2005/05/31 20:35:18 ronp Exp $ -->
<!-- $Source: /cvs/main/searching/org.apache.solrSolarServer/resources/admin/tabular.xsl,v $ -->
<!-- $Name:  $ -->


<xsl:stylesheet
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  version="1.0">


  <xsl:output
    method="html"
    indent="yes"
    doctype-public="-//W3C//DTD HTML 4.01//EN"
    doctype-system="http://www.w3.org/TR/html4/strict.dtd" />


  <xsl:template match="/">
    <html>
      <head>
        <link rel="stylesheet" type="text/css" href="solr-admin.css"></link>
        <link rel="icon" href="/favicon.ico" type="image/ico"></link>
        <link rel="shortcut icon" href="/favicon.ico" type="image/ico"></link>
        <title>SOLR Search Results</title>
      </head>
      <body>
        <a href=".">
           <img border="0" align="right" height="88" width="215" src="solr-head.gif" alt="SOLR">
           </img>
        </a>
        <h1>SOLR Search Results</h1>
          <br clear="all" />
        <xsl:apply-templates/>
        <br /><br />
        <a href=".">Return to Admin Page</a>
      </body>
    </html>
  </xsl:template>


  <xsl:template match="responseHeader">
    <table name="responseHeader">
      <xsl:apply-templates/>
    </table>
  </xsl:template>


  <xsl:template match="status">
    <tr>
      <td name="responseHeader"><strong>Status:&#xa0;</strong></td>
      <td><xsl:value-of select="."></xsl:value-of></td>
    </tr>
  </xsl:template>


  <xsl:template match="numFields">
    <tr>
      <td name="responseHeader"><strong>Number of Fields:&#xa0;</strong></td>
      <td><xsl:value-of select="."></xsl:value-of></td>
    </tr>
  </xsl:template>


  <xsl:template match="numRecords">
    <tr>
      <td name="responseHeader"><strong>Records Returned:&#xa0;</strong></td>
      <td><xsl:value-of select="."></xsl:value-of></td>
    </tr>
  </xsl:template>


  <xsl:template match="numFound">
    <tr>
      <td name="responseHeader"><strong>Records Found:&#xa0;</strong></td>
      <td><xsl:value-of select="."></xsl:value-of></td>
    </tr>
  </xsl:template>


  <xsl:template match="QTime">
    <tr>
      <td name="responseHeader"><strong>Query time:&#xa0;</strong></td>
      <td><xsl:value-of select="."></xsl:value-of>(ms)</td>
    </tr>
  </xsl:template>

  <!-- YCS.. match everything.  How to match only what is not
       matched above???
    -->
  <xsl:template match="responseHeader/*">
    <tr>
      <td name="responseHeader"><strong><xsl:value-of select="name(.)"></xsl:value-of>:&#xa0;</strong></td>
      <td><xsl:value-of select="."></xsl:value-of></td>
    </tr>
  </xsl:template>

  <xsl:template match="responseBody">
    <br></br><br></br>
    <table border="2">

      <!-- table headers -->
      <tr>
        <xsl:for-each select="record[1]/field">
          <th><xsl:value-of select="name"></xsl:value-of></th>
        </xsl:for-each>
      </tr>

      <!-- table rows -->
      <xsl:for-each select="record">
        <tr>
          <xsl:for-each select="field">
            <td><xsl:value-of select="value"></xsl:value-of>&#xa0;</td>
          </xsl:for-each>
        </tr>
      </xsl:for-each>

    </table>


  </xsl:template>


</xsl:stylesheet>
