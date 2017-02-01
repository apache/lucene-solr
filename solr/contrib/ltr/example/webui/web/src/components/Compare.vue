<template lang="html">
  <div class="compare">
    <jumbotron :isShow="false" msg="Compare"></jumbotron>
    <ltr></ltr>
  </div>
</template>

<script>
import jumbotron from './Jumbotron'
import ltr from './Ltr'
export default {
  name:'compare',
  components: {
    jumbotron,
    ltr
  },
  data(){
    return {
    }
  },
  methods:{
  }
}
</script>
