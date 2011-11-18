///// represents a single document

var heist_document = function() {

  this.locked = false;

};

heist_document.prototype.save = function(data, callback) {
  if (this.locked) {
    return false;
  }

  $.ajax('/documents', {

    type: 'post',
    data: data,
    dataType: 'json',
    
    success: function(res) {
      this.locked = true;
      var high = hljs.highlightAuto(data);
      callback({
        value: high.value,
        uuid: res.uuid,
        language: high.language
      });
    }

  });

};

///// represents the paste application

var heist = function(appName) {

  this.appName = appName;
  this.$textarea = $('textarea');
  this.$box = $('#box');
  this.$code = $('#box code');

  this.configureShortcuts();

};

// Set the page title - include the appName
heist.prototype.setTitle = function(ext) {
  var title = ext ? this.appName + ' - ' + ext : this.appName;
  document.title = title;
};

// Remove the current document (if there is one)
// and set up for a new one
heist.prototype.newDocument = function(ext) {
  this.doc = new heist_document();
  this.$box.hide();
  this.setTitle();
  this.$textarea.val('').show().focus();
}

// Lock the current document
heist.prototype.lockDocument = function() {
  var _this = this;
  this.doc.save(this.$textarea.val(), function(ret) {
    if (ret) {
      _this.$code.html(ret.value);
      _this.setTitle(ret.language + '-' + ret.uuid);
      // TODO add to push state
      _this.$textarea.val('').hide();
      _this.$box.show();
    }
  });
};

// Configure keyboard shortcuts for the textarea
heist.prototype.configureShortcuts = function() {
  var _this = this;
  this.$textarea.keyup(function(evt) {
    // ^L for lock
    if (evt.ctrlKey && evt.keyCode === 76) {
      _this.lockDocument();
    }
    else if (evt.ctrlKey && evt.keyCode === 78) {
      _this.newDocument();
    }
  });
};


// TODO refuse to lock empty documents
// TODO support for browsers without pushstate
// TODO support for push state navigation
// TODO ctrl-d for duplicate

///// Tab behavior in the textarea - 2 spaces per tab

$(function() {

  $('textarea').keydown(function(evt) {
    if (evt.keyCode === 9) {
      evt.preventDefault();
      var myValue = '  ';
      // http://stackoverflow.com/questions/946534/insert-text-into-textarea-with-jquery
      // For browsers like Internet Explorer
      if (document.selection) {
        this.focus();
        sel = document.selection.createRange();
        sel.text = myValue;
        this.focus();
      }
      // Mozilla and Webkit
      else if (this.selectionStart || this.selectionStart == '0') {
        var startPos = this.selectionStart;
        var endPos = this.selectionEnd;
        var scrollTop = this.scrollTop;
        this.value = this.value.substring(0, startPos) + myValue +
          this.value.substring(endPos,this.value.length);
        this.focus();
        this.selectionStart = startPos + myValue.length;
        this.selectionEnd = startPos + myValue.length;
        this.scrollTop = scrollTop;
      }
      else {
        this.value += myValue;
        this.focus();
      }
    }
  });

});
