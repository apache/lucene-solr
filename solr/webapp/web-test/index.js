const proxy = require('express-http-proxy');
const express = require('express');
const app = express();
const port = 9000;


app.use('/solr/admin', proxy('http://localhost:8983', {
	preserveHostHdr: false,
	proxyReqPathResolver: function (req) {
      var updatedPath = '/solr/admin' + req.url;
      console.log(updatedPath);
      return updatedPath;
    }
}));

app.use('/solr', express.static('../web'));

app.listen(port, () => console.log(`Example app listening on port ${port}!`));