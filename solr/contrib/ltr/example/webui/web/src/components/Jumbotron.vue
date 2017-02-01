<template>
  <div class="jumbotron">
    <div class="container">
      <h1>{{msg}}</h1>
      <p>A simple tool to generate training data for Apache Solr</p>
      <pre># To enable LTR</br>> solr start -Dsolr.ltr.enabled=true</pre>
      <a v-show="isShow" class="btn btn-primary" href="./static/conf/solrconfig.xml">solrconfig.xml</a>
    </div>
  </div>
</template>

<script>

export default {
  name: "jumbotron",
  data(){
    return {
    }
  },
  props:{
    'msg':String,
    'isShow':Boolean
  },
  methods: {
  }
}
</script>
