<template lang="html">
  <div class="clicklogs">
    <jumbotron :isShow="false" msg="Click logs"></jumbotron>
    <genclick></genclick>
  </div>
</template>

<script>
import jumbotron from './Jumbotron'
import genclick from './Genclick'
export default {
  name:'compare',
  components: {
    jumbotron,
    genclick
  },
  data(){
    return {
    }
  },
  methods:{
  }
}
</script>
