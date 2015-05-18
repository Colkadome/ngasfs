$(function(){

	/*
		Function to log to console
	*/
	function logToConsole(m) {
		$("#console").append("<p>"+m+"</p>");
		$('#console').scrollTop($('#console')[0].scrollHeight);
	}

	/*
		Window resize event
	*/
	$(window).resize(function(){
        var windowH = $(window).height();
        $('#console').css('height', (windowH - 330)+'px');
    })

	// Trigger resize to resize console
    $(window).trigger('resize');

	/*
		Create FS button.
		Will send POST request to server.
		Logs reply.
	*/
	$("#create_fs").click(function(){
		var fsName = $("#fsName_input").val();
		if(fsName) {
			$.post("create_fs", {fsName:fsName}, function(data){
				logToConsole(data);
			})
			.fail(function(data){
				logToConsole("Error "+data.status+": "+data.statusText);
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
				logToConsole(data);
			})
			.fail(function(data){
				logToConsole("Error "+data.status+": "+data.statusText);
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
			logToConsole("Getting files for " + fsName + " with pattern(s) " + patterns + "...");
			$.post("get_files", {sLoc:sLoc, fsName:fsName, patterns:patterns}, function(data){
				logToConsole(data);
			})
			.fail(function(data){
				logToConsole("Error "+data.status+": "+data.statusText);
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
			logToConsole("Getting FS " + fsName + "...");
			$.post("get_fs", {sLoc:sLoc, fsName:fsName}, function(data){
				logToConsole(data);
			})
			.fail(function(data){
				logToConsole("Error "+data.status+": "+data.statusText);
			})
		}
	});

	/*
		Upload Files button.
		Will send POST request to server.
		Logs reply.
	*/
	$("#post_files").click(function(){
		var fsName = $("#fsName_input").val();
		var sLoc = $("#sLoc_input").val();
		var patterns = $("#patterns_input").val();
		if(fsName && sLoc && patterns) {
			logToConsole("Posting files from " + fsName + " with pattern(s) " + patterns + "...");
			$.post("post_files", {sLoc:sLoc, fsName:fsName, patterns:patterns}, function(data){
				logToConsole(data);
			})
			.fail(function(data){
				logToConsole("Error "+data.status+": "+data.statusText);
			})
		}
	});

	/*
		Upload FS button.
		Will send POST request to server.
		Logs reply.
	*/
	$("#post_fs").click(function(){
		var fsName = $("#fsName_input").val();
		var sLoc = $("#sLoc_input").val();
		if(fsName && sLoc) {
			logToConsole("Posting " + fsName + " to " + sLoc + "...");
			$.post("post_fs", {sLoc:sLoc, fsName:fsName}, function(data){
				logToConsole(data);
			})
			.fail(function(data){
				logToConsole("Error "+data.status+": "+data.statusText);
			})
		}
	});

});