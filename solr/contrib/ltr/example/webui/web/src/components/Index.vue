<template lang="html">
  <div class="index">
    <jumbotron :isShow="true" msg="GenTrainingDataSolr"></jumbotron>
    <gendata></gendata>
  </div>
</template>

<script>
import jumbotron from './Jumbotron'
import gendata from './Gendata'
export default {
  name:'index',
  components: {
    jumbotron,
    gendata
  },
  data(){
    return {
    }
  },
  methods:{
  }
}
</script>
