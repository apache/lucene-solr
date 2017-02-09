<template lang="html">
  <div class="compare">
    <jumbotron :isShow="false" msg="Compare"></jumbotron>
    <gencompare></gencompare>
  </div>
</template>

<script>
import jumbotron from './Jumbotron'
import gencompare from './Gencompare'
export default {
  name:'compare',
  components: {
    jumbotron,
    gencompare
  },
  data(){
    return {
    }
  },
  methods:{
  }
}
</script>
