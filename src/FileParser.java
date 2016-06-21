/*
 * FileParser parses the files in /spec and /data directories and populates the tables 
 * */

public class FileParser {
	public static void main(String[] args) {
		// load existing files in /spec and /data
		WatcherService.loadExistingFiles();
		// start watching /data and /specs for new files
		WatcherService.loadNewFiles();
	}

}