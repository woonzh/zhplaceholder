var $TABLE = $('#table');
var $BTN = $('#export-btn');
var $EXPORT = $('#export');

var input = document.getElementById("password");
input.addEventListener("keyup", function(event){
  event.preventDefault();
  if(event.keycode===13){
    login();
  }
});

var input=document.getElementById("url");
input.addEventListener("keyup", function(event){
  event.preventDefault();
  if(event.keycode===13){
    seturl();
  }
});

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
        updateURL();
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

function updateURL(){
  url="https://chenguobin.herokuapp.com/geturl";
  $.ajax({
    url: url,
    type: 'GET',
    success: function (data) {
      var jidraw=JSON.parse(data);
      var url=jidraw['result'];
      urlfield=document.getElementById('cururl');
      urlfield.innerHTML="<b>Current URL: </b>" + url;
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

function seturl(){
  document.getElementById("loading").style.display="block";
  newurl=document.getElementById("url").value;
  var fd= new FormData();
  fd.append("url", newurl);
  url="https://chenguobin.herokuapp.com/seturl"
  $.ajax({
    url: url,
    type: "POST",
    data:fd,
    processData: false,
    contentType: false,
    cache: false,
    success: function (data) {
      var jidraw=JSON.parse(data);
      var result=jidraw['result'];
      if (result=="failed"){
        alert("Unable to reset url.");
      }else{
        alert("url reset successful");
        urlfield=document.getElementById('cururl');
        urlfield.innerHTML="<b>Current URL: </b>" + newurl;
        document.getElementById("url").value="";
      }
      document.getElementById("loading").style.display="none";
    },
    error: function(jqxhr, status, exception) {
        alert('Exception:', exception);
        document.getElementById("loading").style.display="none";
    }
  });
}
