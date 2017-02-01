// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue'
// import App from './App'

import VueResource from 'vue-resource'
import VueRouter from 'vue-router'
import {ClientTable} from 'vue-tables-2'

import Index from './components/Index'
import Compare from './components/Compare'

import top from './components/Header'
import jumbotron from './components/Jumbotron'
import bot from './components/Footer'

Vue.use(VueResource)
Vue.use(VueRouter)
Vue.use(ClientTable)

var router = new VueRouter({
  mode: 'history',
  base: __dirname,
  routes: [
      {path: '/', component: Index},
      {path: '/compare', component: Compare}
  ]
})

// /* eslint-disable no-new */
new Vue({
  router,
  components: {
    top,
    jumbotron,
    Compare,
    Index,
    bot
  },
  template: `
    <div class="container" id="app">
      <top></top>
      <router-view></router-view>
      <bot></bot>
    </div>
  `
}).$mount('#app')
