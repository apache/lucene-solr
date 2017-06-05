<template>
  <div class="star-rating">
    <label v-for="rating in ratings"
    :class="['star-rating__star',{'is-selected': ((val >= rating) && val != null), 'is-disabled': disabled}]"
    @mouseover="starOver(rating)" @mouseout="starOut">
    <input
    class="star-rating star-rating__checkbox"
    type="radio"
    :name='name' :value='rating' :required='required' :id='id' :disabled='disabled'  @click="setVal(rating)">
    ★
  </label>
</div>
</template>

<script>

import Store from 'src/storage'

export default {
  name:'star',
  data: function() {
    return {
      val: null,
      temp_value: null,
      ratings: [0,1, 2, 3, 4],
      items:[]
    };
  },
  computed:{
    cpt:function()
    {
      var list=['Not relevant', 'Fair', 'Good', 'Excellent', 'Perfect'];
      return list[this.val]
    }
  },
  props: {
    'name': String,
    'rating': null,
    'id': String,
    'disabled': Boolean,
    'required': Boolean,
    'query': String
  },
  methods: {
    starOver: function(index) {
      if (this.disabled) {
        return
      }
      this.temp_value = this.val
      this.val = index
    },
    starOut: function() {
      if (this.disabled) {
        return
      }
      this.val = this.temp_value
    },
    setVal: function(value) {
      if (this.disabled) {
        return
      }
      this.temp_value = value
      this.val = value
      this.items=Store.fetch()
      if(!this.find(this.id))
      {
        this.items.push(
          {
            key:this.id,
            val:this.query+'|'+this.id+'|'+this.val+'|HUMAN_JUDGEMENT\n'
          }
        )
        Store.save(this.items)
      }else {
        var key=this.id
        var val = this.query+'|'+this.id+'|'+this.val+'|HUMAN_JUDGEMENT\n'
        this.modifier(key,val)
      }
    },
    modifier:function(key,val){
      this.items.forEach(function (e) {
        if (e.key === key) {
          e.val = val
        }
      }, this)
      Store.save(this.items)
    },
    find:function(key)
    {
      var tmp=false
      this.items.forEach(function(item,index){
        if(item.key===key)
        {
          tmp=true
        }
      },this)
      return tmp
    }
  }
}
</script>

<style>
.star-rating__checkbox {
  position: absolute;
  overflow: hidden;
  clip: rect(0 0 0 0);
  height: 1px;
  width: 1px;
  margin: -1px;
  padding: 0;
  border: 0;
}

.star-rating__star {
  display: inline-block;
  padding: 3px;
  vertical-align: middle;
  line-height: 1;
  font-size: 1.5em;
  color: #ABABAB;
  transition: color .2s ease-out;
}
.star-rating__star:hover {
  cursor: pointer;
}
.star-rating__star.is-selected {
  color: #FFD700;
}
.star-rating__star.is-disabled:hover {
  cursor: default;
}
</style>
