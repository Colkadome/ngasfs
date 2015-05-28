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
    	Search functionality
    */
    var server_files = [];
    var fs_files = [];
    $("#search_button").click(function(){
    	var fsName = $("#fsName_input").val();
    	var sLoc = $("#sLoc_input").val();
    	if(fsName) {
    		$.get("search_fs", {fsName:fsName}, function(data){
    			// get data
				logToConsole(data.statusText);
			})
			.fail(function(data){
				logToConsole("Error "+data.status+": "+data.statusText);
			})
    	}
    	if(sLoc) {
    		$.get("search_server", {sLoc:sLoc}, function(data){
    			// get data
				logToConsole(data.statusText);
			})
			.fail(function(data){
				logToConsole("Error "+data.status+": "+data.statusText);
			})
    	}
    	if(!(fsName || sLoc)) {
    		logToConsole("Please specify either File System Name or Server Location");
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
			logToConsole("Please specify File System Name");
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
				logToConsole(data.statusText);
			})
			.fail(function(data){
				logToConsole("Error "+data.status+": "+data.statusText);
			})
		}
		else {
			logToConsole("Please specify File System Name");
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
			logToConsole("Please specify Server Location");
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
			logToConsole("Getting files with pattern(s) " + patterns + "...");
			$.post("get_files", {sLoc:sLoc, fsName:fsName, patterns:patterns}, function(data){
				logToConsole(data.statusText);
			})
			.fail(function(data){
				logToConsole("Error "+data.status+": "+data.statusText);
			})
		}
		else if (!fsName) {
			logToConsole("Please specify File System Name");
		}
		else if (!sLoc) {
			logToConsole("Please specify Server Location");
		}
		else if (!patterns) {
			logToConsole("Please specify SQL patterns to match files for download (eg. 'file%' to download all files starting with 'file')");
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
			logToConsole("Downloading " + fsName + "...");
			$.post("get_fs", {sLoc:sLoc, fsName:fsName}, function(data){
				logToConsole(data.statusText);
			})
			.fail(function(data){
				logToConsole("Error "+data.status+": "+data.statusText);
			})
		}
		else if(!fsName) {
			logToConsole("Please specify File System Name");
		}
		else if(!sLoc) {
			logToConsole("Please specify Server Location");
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
		var force = $("#post_files_force").is(':checked')?1:0;
		var keep = $("#post_files_keep").is(':checked')?1:0;
		if(fsName && sLoc && patterns) {
			logToConsole("Uploading files with pattern(s) " + patterns + "...");
			$.post("post_files", {sLoc:sLoc, fsName:fsName, patterns:patterns, force:force, keep:keep}, function(data){
				logToConsole(data.statusText);
			})
			.fail(function(data){
				logToConsole("Error "+data.status+": "+data.statusText);
			})
		}
		else if(!fsName) {
			logToConsole("Please specify File System Name");
		}
		else if(!sLoc) {
			logToConsole("Please specify Server Location");
		}
		else if(!patterns) {
			logToConsole("Please specify SQL patterns to match files for upload (eg. 'file%' to upload all files starting with 'file')");
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
			logToConsole("Uploading " + fsName + "...");
			$.post("post_fs", {sLoc:sLoc, fsName:fsName}, function(data){
				logToConsole(data.statusText);
			})
			.fail(function(data){
				logToConsole("Error "+data.status+": "+data.statusText);
			})
		}
		else if(!fsName) {
			logToConsole("Please specify File System Name");
		}
		else if(!sLoc) {
			logToConsole("Please specify Server Location");
		}
	});

});