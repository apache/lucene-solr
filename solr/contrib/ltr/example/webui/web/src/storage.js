var STORAGE_KEY = 'ltr'

export default {
  fetch: function () {
    return JSON.parse(window.localStorage.getItem(STORAGE_KEY) || '[]')
  },
  save: function (data) {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(data))
  }
}