

function login(){
  var email=document.getElementById('email').value;
  var password=document.getElementById('password').value;
  var fd= new FormData();
  fd.append("email", email);
  fd.append("password", password);

  var url="https://chenguobin.herokuapp.com/login";
  $.ajax({
    url: url,
    type: 'GET',
    data:{
      email: email,
      password: password
    },
    //data:fd,
    //processData: false,
    //contentType: false,
    //cache: false,
    success: function (data) {
      alert(data);
      var resp=JSON.parse(data);
      var result=resp['result'];
      var msg=resp['msg'];
      if (result==true){
        localStorage.setItem("email", email);
        localStorage.setItem("session", msg);
        window.location.href = "https://chenguobin.herokuapp.com/main";
      }else{
        localStorage.setItem("email", "");
        localStorage.setItem("session", "");
        alert(msg);
      }
    },
    error: function(jqxhr, status, exception) {
        alert('Exception:', exception);
    }
  });
}
