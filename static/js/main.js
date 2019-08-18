var $TABLE = $('#table');
var $BTN = $('#export-btn');
var $EXPORT = $('#export');

function checkSession(){
  var email = localStorage.getItem("email");
  var session = localStorage.getItem("session");
  url="https://chenguobin.herokuapp.com/sessioncheck"
  $.ajax({
    url: url,
    type: 'GET',
    data:{
      email: email,
      session: session
    },
    success: function (data) {
      var resp=JSON.parse(data);
      var result=resp['result'];
      if (result=="success"){
        loadData();
      }else{
        window.location.href = "https://chenguobin.herokuapp.com";
      }
    },
    error: function(jqxhr, status, exception) {
      alert('Exception:', exception);
      window.location.href = "https://chenguobin.herokuapp.com";
    }
  });
}

function loadData(){
  url="https://chenguobin.herokuapp.com/geturl";
  $.ajax({
    url: url,
    type: 'GET',
    success: function (data) {
      var jidraw=JSON.parse(data);
      var url=jidraw['result'];
      frame=document.getElementById('myframe');
      frame.src = url;
      document.getElementById("loading").style.display="none";
    },
    error: function(jqxhr, status, exception) {
        alert('Exception:', exception);
        document.getElementById("loading").style.display="none";
    }
  });
}

function onload(){
  checkSession();
}
