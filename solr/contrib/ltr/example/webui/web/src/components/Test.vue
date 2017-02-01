<template>
  <div class="container-fluid">
    <table class="table table-bordered">
      <thead>
        <tr>
          <th v-for="(column,index) in columns" @click="sortData(sortColumn[index])" :class="{'cansort': sortColumn[index]}">{{column}}<span class="glyphicon" :class="{'glyphicon-menu-down': sortColumn[index] === sort.key && sort.val === -1,'glyphicon-menu-up': sortColumn[index] === sort.key && sort.val === 1  }"></span></th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="tr in orderedUsers">
        <td v-for="td in tr">{{td}}</td>
      </tr>
    </tbody>
  </table>
</div>
</template>

<script>
export default {
  data () {
    return {
      sort: {
        key: '',
        val: 1
      }
    }
  },
  props: {
    columns: {
      type: Array
    },
    data: {
      type: Array
    },
    record: {
      type: Number,
      default: 10
    },
    sortColumn: {
      type: Array,
      default: () => {
        return []
      }
    }
  },
  methods: {
    sortData (name) {
      if (!this.sortColumn || this.sortColumn.length === 0) return
      if (!name) return
      this.sort = {
        key: name,
        val: -this.sort.val
      }
      console.log(this.sort);
    }
  },
  computed: {
    orderedUsers: function () {
      return _.orderBy(this.data, 'score','desc')
    }
  },
  watch: {
    data () {
      this.sort = {
        key: '',
        val: 1
      }
    }
  }
}
</script>

<style scoped>
.container-fluid {
  position: relative;
}
table {
  background-color: #fff;
}
.cansort {
  cursor: pointer;
}
.pagination {
  position: absolute;
  top: -53px;
  right: 15px;
}
</style>
