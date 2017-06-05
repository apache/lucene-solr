<template>
  <div class="gencompare">
    <div class="form-horizontal">

      <div class="form-group">
        <label class="col-sm-1 control-label" for="url">Solr url : </label>
        <div class="col-sm-3">
          <input  @keyup.enter="search" type="text" class="form-control" id="url" @input="setUrl" v-model:value="url">
        </div>
        <label class="col-sm-1 control-label" for="query">Query : </label>
        <div class="col-sm-3">
          <input  @keyup.enter="search" type="text" class="form-control" v-model:value="query">
        </div>
        <label class="col-sm-1 control-label" for="fl">fl : </label>
        <div class="col-sm-3">
          <input  @keyup.enter="search" type="text" class="form-control" v-model:value="fl">
        </div>
      </div>

      <div class="form-group">
        <label class="col-sm-1 control-label" for="model">model : </label>
        <div class="col-sm-3">
          <input  @keyup.enter="search" type="text" class="form-control" v-model:value="model">
        </div>
        <label class="col-sm-1 control-label" for="reRankDocs">RankDocs: </label>
        <div class="col-sm-3">
          <input @keyup.enter="search" type="number" class="form-control" v-model:value="reRankDocs">
        </div>
        <label class="col-sm-1 control-label" for="search"><span class="glyphicon glyphicon-hand-right" aria-hidden="true"></span> :</label>
        <div class="col-sm-3">
          <button @click="search" type="button" class="btn btn-primary"><span class="glyphicon glyphicon-sort" aria-hidden="true"></span> Compare</button>
        </div>
      </div>
    </div>

    <hr>
    <p class="alert alert-warning text-center" v-show="!isFinish">No result found</p>
    <div class="row" v-show="isFinish">
      <div class="col-xs-4">
        <h4> <span class="label label-success">Apache Solr</span></h4>
        <v-client-table :data="solr" :columns="columns_solr" :options="options"></v-client-table>
      </div>
      <div class="col-xs-8">
        <h4> <span class="label label-info">Apache Solr with LTR</span></h4>
        <v-client-table @row-click="changeData" :data="docs" :columns="columns" :options="options"></v-client-table>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: "ltr",
  data(){
    return {
      url:window.localStorage.getItem('ltr_url')||'http://localhost:8983/solr/corejouve',
      query:'skirt',
      model:'jouvemodel',
      fl:'score,name_txt_en,[features]',
      reRankDocs:'5',
      solr:[],
      docs:[],
      columns:[],
      columns_solr:[],
      isFinish:false,
      options:{
        filterable:false,
        childRowKey:'id',
        // skin:'table-hover'
      }
    }
  },
  computed:{
    rq:function()
    {
      return '{!ltr model='+this.model+' reRankDocs='+this.reRankDocs+'}'
    }
  },
  methods: {
    changeData: function (row) {
        //  console.log(row);
       },
    setUrl()
    {
      window.localStorage.setItem('ltr_url',this.url)
    },
    sortByKey(array, key){
      return array.sort(function(a, b) {
        var x = a[key]; var y = b[key];
        return ((x < y) ? -1 : ((x > y) ? 1 : 0));
      });
    },
    search(){
      this.docs=[]
      this.solr=[]
      this.columns_solr=[]
      this.columns=[]
      window.localStorage.setItem('ltr_url',this.url)
      var fullUrl=this.url+'/select?indent=on&q='+this.query+'&rq='+encodeURI(this.rq)+'&fl='+this.fl+'&wt=json&rows=100';
      this.$http.get(fullUrl).then((data) => {
        var json = JSON.parse(data.body).response
        if('undefined'!==json.docs)
        {
          this.docs = json.docs.slice(0,this.reRankDocs)
          if(this.docs.length>0)
          {
            this.columns=Object.keys(this.docs[0])
            this.columns.unshift('No.')
            this.docs.forEach(function(e,index){

            this.fl.split(",").forEach(function(f){
            if(typeof e[f]==='boolean')
            {
              var tmp = e[f]
                e[f]=tmp.toString()
            }
            },this)
              e['No.']=index+1
            },this)
            // copy array !!!
            this.solr=JSON.parse(JSON.stringify(this.docs));
            this.columns.forEach(function(e){
              if(e!=='[features]')
              {
                this.columns_solr.push(e)
              }
            },this);

            this.solr.forEach(function(e){
              var feature = e['[features]']
              var orignalScore = 0
              feature.split(',').forEach(function(f)
              {
                var tmp =f.split('=')
                  if(tmp[0]==='originalScore')
                  {
                    orignalScore=tmp[1]
                  }
              })
              e['score']=orignalScore
            },this)

            this.sortByKey(this.solr,'socre')

            this.solr.forEach(function(e,index){
              e['No.']=index+1
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
    }
  }
}
</script>

<style>
.VueTables__sortable {
  cursor: pointer;
}
</style>
