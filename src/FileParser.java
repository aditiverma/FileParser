
/*
 * FileParser parses the files in /spec and /data directories and populates the tables 
 * */
public class FileParser {
	public static void main(String[] args) {

		WatcherService.loadExistingFiles();
		
		// start watching /data and /specs for new files
		WatcherService.loadNewFiles();

	}

}