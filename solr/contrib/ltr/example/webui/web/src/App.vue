<template>
  <div class="container" id="app">
    <top></top>
    <jumbotron/>
    <bot></bot>
  </div>
</template>

<script>
import top from './components/Header'
import jumbotron from './components/Jumbotron'
import index from './components/Index'
import bot from './components/Footer'

export default {
  name: 'app',
  components: {
    top,
    jumbotron,
    index,
    bot
  }
}
</script>

<style>

</style>
