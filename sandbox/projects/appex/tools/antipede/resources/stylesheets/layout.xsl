<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
   
   <xsl:output indent="yes" method="html"/>

   <xsl:template match="/">
      <html>
         <head>
           <title>Project properties</title>
           <style><![CDATA[
   <!--

td.block
{   display:block; 
    text-align: justify; 
    font-weight:plain;
    padding: 0px;
    background-color: #414681; 
    color: #ffffff; 
    margin: 5px 0px 5px 0px; 
    font-size: 12px; 
    font-family: Verdana, Arial, Helvetica, sans-serif
}

td.comment
{   display:block; 
    text-align: justify; 
    font-weight:plain;
    font-style:italic;   
    padding: 0px;
    background-color: #cfcdc6; 
    color: #000000; 
    margin: 5px 0px 5px 0px; 
    font-size: 12px; 
    font-family: Verdana, Arial, Helvetica, sans-serif
}

td.name {display:block; 
      list-style-type:disc;
      text-align: left; 
      font-weight:plain;
      padding: 0px;
      background-color: #FFFFFF; 
      color: #000000; 
      margin: 0px 0px 0px 40px; 
      font-size: 12px; 
      font-family: Verdana, Arial, Helvetica, sans-serif
    }
    

td.value {
    display:block; 
    text-align: left; 
    font-weight:plain;
    padding: 0px;
    background-color: #ffe3a6; 
    color: #000000; 
    margin: 0px 0px 0px 40px; 
    font-size: 12px; 
    font-family: Verdana, Arial, Helvetica, sans-serif
  }


     //-->]]>

           </style>
         </head>
         <body>
           <table><tr><td>
            <xsl:apply-templates/>
           </td></tr></table> 
         </body>
      </html>
   </xsl:template>

   <xsl:template match="@*">
   </xsl:template>

   <xsl:template match="text()">
      <xsl:value-of select="."/>
   </xsl:template>

   <xsl:template match="comment()">
     <table><tr><td class="comment"><pre>
      <xsl:value-of select="."/>
     </pre></td></tr></table>
   </xsl:template>

   <xsl:template match="*">
     <table><tr><td class="block">
     <xsl:value-of select="name(.)"/>
     <table>
     <xsl:for-each select="@*">
      <tr>
        <td class="name"><xsl:value-of select="name(.)"/></td>
        <td class="value">=<xsl:value-of select="."/></td>
      </tr>      
     </xsl:for-each>
     </table> 
     </td></tr></table>        
   </xsl:template>

   <xsl:template match="*[node()]">
     <table><tr><td class="block">
     <xsl:value-of select="name(.)"/>
     </td></tr></table> 
    
     <table><tr>
     <td width="20px"></td>
     <td class="name">     
     <xsl:apply-templates/>
     </td></tr></table> 
     
   </xsl:template>

   <xsl:template match="*[text() and not (comment() or processing-instruction())]">
      <br/><xsl:value-of select="name(.)"/>
      <xsl:apply-templates select="@*"/>
      <xsl:value-of select="."/>
   </xsl:template>

   <xsl:template name="nbsp-ref">
      <xsl:text>&#160;</xsl:text>
   </xsl:template>

</xsl:stylesheet>