function onload(){
    document.getElementById("loading").style.display="none";
}

function trigger_download(){
  var r = confirm("Confirm download?");
  if (r==true){
    document.getElementById("loading").style.display="block";
    var password=document.getElementById("password").value;
    var country=document.getElementById("country").value;
    var clean=document.getElementById("clean").value;
    url="https://zhplaceholder.herokuapp.com/getAnalytics";
    $.ajax({
      url: url,
      type: 'GET',
      data:{
        pw:password,
        country:country,
        clean: clean
      },
      success: function (data) {
        alert("Success. Results file will be downloaded.");
        csv = 'data:text/csv;charset=utf-8,' + encodeURI(data);
        link = document.createElement('a');
        link.setAttribute('href', csv);
        fname=country+'.csv';
        link.setAttribute('download', fname);
        link.click();
        document.getElementById("loading").style.display="none";
      },
      error: function(jqxhr, status, exception) {
          alert('Exception:', exception);
          document.getElementById("loading").style.display="none";
      }
    });
  }
}

function trigger_hk_update(){
  var r = confirm("Confirm update?");
  if (r==true){
    document.getElementById("loading").style.display="block";
    var quandl=0;
    var password=document.getElementById("password").value;
    url="https://zhplaceholder.herokuapp.com/hkexUpdateBasic";
    $.ajax({
      url: url,
      type: 'GET',
      data:{
        quandl:quandl,
        pw:password
      },
      success: function (data) {
        alert(data);
        document.getElementById("loading").style.display="none";
      },
      error: function(jqxhr, status, exception) {
          alert('Exception:', exception);
          document.getElementById("loading").style.display="none";
      }
    });
  }
}

function trigger_sg_update(){
  var r = confirm("Confirm update?");
  if (r==true){
    document.getElementById("loading").style.display="block";
    url="https://zhplaceholder.herokuapp.com/sgxUpdate";
    var password=document.getElementById("password").value;
    $.ajax({
      url: url,
      type: 'GET',
      data:{
        pw:password
      },
      success: function (data) {
        alert(data);
        document.getElementById("loading").style.display="none";
      },
      error: function(jqxhr, status, exception) {
          alert('Exception:', exception);
          document.getElementById("loading").style.display="none";
      }
    });
  }
}
