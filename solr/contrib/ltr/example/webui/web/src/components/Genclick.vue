<template>
  <div class="gendata">
    <div class="form-inline">
      <div class="form-group">
        <label for="url">Solr url : </label>
        <input type="text" class="form-control" id="url" v-model:value='url' @input="setUrl" @keyup.enter="search">
      </div>
      <div class="form-group">
        <label for="query">Query : </label>
        <input type="text" class="form-control" id="query" v-model:value="query" @keyup.enter="search">
      </div>
      <div class="form-group">
        <label for="fl">fl : </label>
        <input type="text" class="form-control" id="fl" v-model:value="fl" @keyup.enter="search">
      </div>
      <div class="form-group">
        <label for="otherParams">Others : </label>
        <input type="text" class="form-control" id="otherParams" v-model:value="otherparams" @keyup.enter="search">
      </div>
      <button @click="search" @keyup.enter="search" id="search" type="button" class="btn btn-primary"><span class="glyphicon glyphicon-search" aria-hidden="true"></span> Search</button>
      <button @click="modal" class="btn btn-success"><span class="glyphicon glyphicon-save" aria-hidden="true"></span> Generate</button>
    </div>

    <modal v-model="showModal" effect="fade">
      <!-- custom header -->
      <div slot="modal-header" class="modal-header">
        <h4 class="modal-title">
          GenTrainingDataSolr
        </h4>
      </div>
      <textarea class="form-control">{{gen}}</textarea>
      <!-- custom buttons -->
      <div slot="modal-footer" class="modal-footer">
        <button type="button" class="btn btn-danger" @click="showModal = false">Close</button>
      </div>
    </modal>

    <hr>
    <p class="alert alert-success">{{url}}/select?indent=on&q={{query}}&fl={{fl}}{{otherparams}}&wt=json</p>
    <p class="alert alert-warning text-center" v-show="!isFinish">No result found</p>
    <table class="table" v-show="isFinish">
      <thead>
        <th v-for="column in columns">{{column}}</th>
        <th v-if="isFinish">Nb click</th>
      </thead>
      <tbody>
        <tr v-for="(doc,index) in docs">
          <td v-for="(k, v) in doc" v-text="k"></td>
          <td>
            <!-- <star :id="item.id" :query="query" :disabled="false"/> -->
            <input class="form-control" @click="add(index)" :id="doc.id" type="number" min="0" name="nbclick" value="0">
          </td>
        </tr>
      </tbody>
    </table>
  </div>

</template>

<script>

import modal from 'vue-strap/src/Modal'

export default {
  name: "gendata",
  components:{
    modal
  },
  data(){
    return {
      url:window.localStorage.getItem('ltr_url')||'http://localhost:8983/solr/corejouve',
      query:'skirt',
      fl:'id,name_txt_en,description_txt_en',
      otherparams:'&rows=100',
      docs:[],
      gen:'',
      columns:[],
      isFinish:false,
      showModal:false,
      nbclick:[],
      tmp:0
    }
  },
  methods: {
    add(index)
    {
        this.nbclick[index]['nbclick']++
    },
    setUrl()
    {
      window.localStorage.setItem('ltr_url',this.url)
    },
    search(){
      window.localStorage.setItem("ltr","")
      window.localStorage.setItem('ltr_url',this.url)
      this.docs=[]
      var fullUrl=this.url+'/select?indent=on&q='+this.query+'&fl='+this.fl+this.otherparams+'&wt=json';
      this.$http.get(fullUrl).then((data) => {
        var json = JSON.parse(data.body).response
        if('undefined'!==json.docs)
        {
          this.docs = json.docs
          if(this.docs.length>0)
          {
            this.columns=Object.keys(this.docs[0])
            this.nbclick=JSON.parse(JSON.stringify(this.docs));
            this.nbclick.forEach(function(e,index){
              e['nbclick']=0
              console.log('ok');
            },this)
            this.isFinish=true
          }else{
            this.isFinish=false
          }
        }
      }, (error) => {
        this.showModal=true
        this.gen="Something was wrong ... Try to start your Apache solr."
      })
    },
    modal()
    {
      this.showModal=true
      var tmp=''
      this.nbclick.forEach(function(e,index){
        tmp+=this.query+'|'+e.id+'|'+e.nbclick+'|CLICK_LOGS\n'
      },this)
      this.gen=tmp
    }
  }
}
</script>

<style>
textarea{
  min-height: 345px;
  border-color: rgba(82, 168, 236, 0.8);
  box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.1), 0 0 8px rgba(82, 168, 236, 0.6);
}
</style>
