$(function(){

	// Init stuff

	$(".fs_reliant").attr("disabled", true);
	$(".server_reliant").attr("disabled", true);

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
		FS files list dropdown button
    */
    $(".dropdown_button").click(function(){
    	$("." + $(this).attr("target")).toggle(100);
    	$(this).toggleClass("dropdown");
    	$(this).toggleClass("dropup");
    });

    /*
		Code for dealing with activating buttons
    */
    var fs_good = false;
    var last_fs = "";
    var server_good = false;
    var last_server = "";
    function enableFStools() {
    	fs_good = true;
    	last_fs = $("#fsName_input").val();
    	$(".fs_reliant").each(function(i) {
    		if($(this).hasClass("server_reliant")) {
    			if(server_good) {
    				$(this).attr("disabled", false);
    			}
    		}
    		else {
    			$(this).attr("disabled", false);
    		}
    	});
    }
    function disableFStools() {
    	fs_good = false;
    	$(".fs_reliant").attr("disabled", true);
    }
    function enableServerTools() {
    	server_good = true;
    	last_server = $("#sLoc_input").val();
    	$(".server_reliant").each(function(i) {
    		if($(this).hasClass("fs_reliant")) {
    			if(fs_good) {
    				$(this).attr("disabled", false);
    			}
    		}
    		else {
    			$(this).attr("disabled", false);
    		}
    	});
    }
    function disableServertools() {
    	server_good = false;
    	$(".server_reliant").attr("disabled", true);
    }
    $("#fsName_input").on("input", function(data) {
    	var fsName = $("#fsName_input").val();
    	if(fs_good && fsName != last_fs) {
    		disableFStools();
    	}
    	else if(fsName == last_fs) {
    		enableFStools();
    	}
    });
    $("#sLoc_input").on("input", function(data) {
    	var sLoc = $("#sLoc_input").val();
    	if(server_good && sLoc != last_server) {
    		disableServertools();
    	}
    	else if(sLoc == last_server) {
    		enableServerTools();
    	}
    });

	/*
		functions to check arguments
    */
    function getsLoc() {
    	var sLoc =  $("#sLoc_input").val();
    	if(sLoc) {
    		if(server_good) {
    			return sLoc;
    		}
    		else {
    			logToConsole("Please Check Connection");
    		}
    	}
    	else {
    		logToConsole("Please Specify Server Location");
    	}
    	return null;
    }
    function getFS() {
    	var fsName =  $("#fsName_input").val();
    	if(fsName) {
    		if(fs_good) {
    			return fsName;
    		}
    		else {
    			logToConsole("Please Set File System");
    		}
    	}
    	else {
    		logToConsole("Please Specify File System Name");
    	}
    	return null;
    }
    function getPatterns() {
    	var patterns = $("#patterns_input").val();
    	if(patterns) {
    		return patterns;
    	}
    	logToConsole("Please Specify SQL Patterns to search for files");
    	return null;
    }

    /*
    	Search functionality
    */
    var server_files = [];
    var fs_files = [];
    $("#search_button").click(function(){
    	var fsName = getFS();
    	var sLoc = getsLoc();
    	var patterns = getPatterns();

    	if(patterns) {
    		if(fsName) {
    			logToConsole("Searching File System...");
	    		$.get("search_fs", {fsName:fsName, patterns:patterns}, function(data){
	    			// get data
					logToConsole(data.statusText);
					console.log(data.L);
				})
				.fail(function(data){
					logToConsole("Error "+data.status+": "+data.statusText);
				})
	    	}
	    	if(sLoc) {
	    		logToConsole("Searching server...");
	    		$.get("search_server", {sLoc:sLoc, patterns:patterns}, function(data){
	    			// get data
	    			console.log(data.T);
					logToConsole(data.statusText);
				})
				.fail(function(data){
					logToConsole("Error "+data.status+": "+data.statusText);
				})
	    	}
    	}
    })

	/*
		Create FS button.
		Will send POST request to server.
		Logs reply.
	*/
	$("#create_fs").click(function(){
		var fsName = $("#fsName_input").val();
		if(fsName) {
			$.post("create_fs", {fsName:fsName}, function(data){
				if(status==0) {
					enableFStools();
				}
				logToConsole(data.statusText);
			})
			.fail(function(data){
				logToConsole("Error "+data.status+": "+data.statusText);
			})
		}
		else {
			logToConsole("Please Specify File System Name");
		}
	});

	/*
		Mount FS button.
		Will send POST request to server.
		Logs reply.
	*/
	$("#mount_fs").click(function(){
		var fsName = getFS();
		if(fsName) {
			$.post("mount_fs", {fsName:fsName}, function(data){
				logToConsole(data.statusText);
			})
			.fail(function(data){
				logToConsole("Error "+data.status+": "+data.statusText);
			})
		}
	});

	/*
		Check Connection button.
		Will send POST request to server.
		Logs reply.
	*/
	$("#check_server").click(function(){
		var sLoc = $("#sLoc_input").val();
		if(sLoc) {
			logToConsole("Checking server...");
			$.get("check_server", {sLoc:sLoc}, function(data){
				logToConsole(data.status + ": " + data.statusText);
				if(data.status == 200) {
					enableServerTools();
				}
			})
			.fail(function(data){
				logToConsole("Error "+data.status+": "+data.statusText);
			})
		}
		else {
			logToConsole("Please Specify Server Location");
		}
	});

	/*
		Download Files button.
		Will send POST request to server.
		Logs reply.
	*/
	$("#get_files").click(function(){
		var fsName = getFS();
		var sLoc = getsLoc();
		var patterns = getPatterns();
		if(fsName && sLoc && patterns) {
			logToConsole("Getting files with pattern(s) " + patterns + "...");
			$.post("get_files", {sLoc:sLoc, fsName:fsName, patterns:patterns}, function(data){
				logToConsole(data.statusText);
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
		var fsName = getFS();
		var sLoc = getsLoc();
		if(fsName && sLoc) {
			logToConsole("Downloading " + fsName + "...");
			$.post("get_fs", {sLoc:sLoc, fsName:fsName}, function(data){
				logToConsole(data.statusText);
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
		var fsName = getFS();
		var sLoc = getsLoc();
		var patterns = getPatterns();
		var force = $("#post_files_force").is(':checked')?1:0;
		if(fsName && sLoc && patterns) {
			logToConsole("Uploading files with pattern(s) " + patterns + "...");
			$.post("post_files", {sLoc:sLoc, fsName:fsName, patterns:patterns, force:force, keep:keep}, function(data){
				logToConsole(data.statusText);
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
		var fsName = getFS();
		var sLoc = getsLoc();
		if(fsName && sLoc) {
			logToConsole("Uploading " + fsName + "...");
			$.post("post_fs", {sLoc:sLoc, fsName:fsName}, function(data){
				logToConsole(data.statusText);
			})
			.fail(function(data){
				logToConsole("Error "+data.status+": "+data.statusText);
			})
		}
	});

});