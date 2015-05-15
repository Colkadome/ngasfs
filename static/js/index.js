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

	/*
		Download Files button.
		Will send POST request to server.
		Logs reply.
	*/
	$("#get_files").click(function(){
		var fsName = $("#fsName_input").val();
		var sLoc = $("#sLoc_input").val();
		var patterns = $("#patterns_input").val();
		if(fsName && sLoc && patterns) {
			$.post("get_files", {sLoc:sLoc, fsName:fsName, patterns:patterns}, function(data){
				$("#console").append("<p>"+data+"</p>");
			})
			.fail(function(data){
				$("#console").append("<p>Error "+data.status+": "+data.statusText+"</p>");
			})
		}
	});

	/*
		Download Files button.
		Will send POST request to server.
		Logs reply.
	*/
	$("#get_fs").click(function(){
		var fsName = $("#fsName_input").val();
		var sLoc = $("#sLoc_input").val();
		if(fsName && sLoc) {
			$.post("get_fs", {sLoc:sLoc, fsName:fsName}, function(data){
				$("#console").append("<p>"+data+"</p>");
			})
			.fail(function(data){
				$("#console").append("<p>Error "+data.status+": "+data.statusText+"</p>");
			})
		}
	});

});