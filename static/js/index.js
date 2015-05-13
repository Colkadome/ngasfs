$(function(){

	/*
		Create FS button.
		Will send POST request to server.
		Logs reply.
	*/
	$("#create_fs").click(function(){
		var fsName = $("#fsName_input").val();
		if(fsName) {
			$.post("create_fs", {fsName:fsName}, function(data){
				$("#console").append("<p>"+data+"</p>");
			})
			.fail(function(data){
				$("#console").append("<p>Error "+data.status+": "+data.statusText+"</p>");
			})
		}
	});

	/*
		Mount FS button.
		Will send POST request to server.
		Logs reply.
	*/
	$("#mount_fs").click(function(){
		var fsName = $("#fsName_input").val();
		if(fsName) {
			$.post("mount_fs", {fsName:fsName}, function(data){
				$("#console").append("<p>"+data+"</p>");
			})
			.fail(function(data){
				$("#console").append("<p>Error "+data.status+": "+data.statusText+"</p>");
			})
		}
	});

});