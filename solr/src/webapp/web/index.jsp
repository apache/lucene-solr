<%@ page contentType="text/html; charset=utf-8" pageEncoding="UTF-8"%>
<% request.setCharacterEncoding("UTF-8"); %>

<%@ page import="java.util.List" %>
<%@ page import="java.util.Collection" %>

<% org.apache.solr.core.CoreContainer cores = (org.apache.solr.core.CoreContainer)request.getAttribute("org.apache.solr.CoreContainer"); %>

<html>
<head>
    
    <title>solr-admin</title>
    
    <link rel="stylesheet" type="text/css" href="css/screen.css">
    <link rel="icon" type="image/ico" href="img/favicon.ico">
    
    <script type="text/javascript">
    
    var app_config = {};
    
    app_config.solr_path = '<%= request.getContextPath() %>';
    app_config.core_admin_path = '<%= cores.getAdminPath() %>';
    app_config.zookeeper_path = 'zookeeper.jsp';
    
    </script>
    
</head>
<body>
    
    <div id="wrapper">
    
        <div id="header">
            
            <a href="./" id="solr"><span>Apache SOLR</span></a>

            <div id="wip-notice">
                <p>This interface is work in progress. It works best in Chrome.</p>
                <p><a href="admin">Use the <span>old admin interface</span> if there are problems with this one.</a></p>
                <p><a href="https://issues.apache.org/jira/browse/SOLR-2399">Bugs/Requests/Suggestions: <span>SOLR-2399</span></a></p>
            </div>

            <p id="environment">&nbsp;</p>

        </div>

        <div id="main" class="clearfix">
        
            <div id="content-wrapper">
            <div id="content">
                
                &nbsp;
                
            </div>
            </div>
            
            <div id="menu-wrapper">
            <div id="menu">
                
                <ul>

                    <li id="index" class="global">
                        <p><a href="#/">Dashboard</a></p>
                    </li>

                    <li id="logging" class="global">
                        <p><a href="#/logging">Logging</a></p>
                    </li>

                    <li id="cloud" class="global optional">
                        <p><a href="#/cloud">Cloud</a></p>
                    </li>

                    <li id="cores" class="global optional">
                        <p><a href="#/cores">Core Admin</a></p>
                    </li>

                    <li id="java-properties" class="global">
                        <p><a href="#/java-properties">Java Properties</a>
                    </li>

                    <li id="threads" class="global">
                        <p><a href="#/threads">Thread Dump</a></p>
                    </li>
                    
                </ul>
                
            </div>
            </div>
            
            <div id="meta">
                
                <ul>
                    
                    <li class="documentation"><a href="http://lucene.apache.org/solr/"><span>Documentation</span></a></li>
                    <li class="issues"><a href="http://issues.apache.org/jira/browse/SOLR"><span>Issue Tracker</span></a></li>
                    <li class="irc"><a href="http://webchat.freenode.net/?channels=#solr"><span>IRC Channel</span></a></li>
                    <li class="mailinglist"><a href="http://wiki.apache.org/solr/UsingMailingLists"><span>Community forum</span></a></li>
                    <li class="wiki-query-syntax"><a href="http://wiki.apache.org/solr/SolrQuerySyntax"><span>Solr Query Syntax</span></a></li>
                    
                </ul>
                
            </div>
            
        </div>
    
    </div>
    
    <script type="text/javascript" src="js/0_console.js"></script>
    <script type="text/javascript" src="js/1_jquery.js"></script>
    <script type="text/javascript" src="js/jquery.timeago.js"></script>
    <script type="text/javascript" src="js/jquery.form.js"></script>
    <script type="text/javascript" src="js/jquery.sammy.js"></script>
    <script type="text/javascript" src="js/jquery.sparkline.js"></script>
    <script type="text/javascript" src="js/jquery.jstree.js"></script>
    <script type="text/javascript" src="js/highlight.js"></script>
    <script type="text/javascript" src="js/script.js"></script>
    
</body>
</html>