import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;


public class ReadFiles {
	static List<String> getSpecContents(File file) {
		List<String> specLines = new ArrayList<String>();
		BufferedReader in;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF8"));
			String str;
			int lineNo = 1;
			while ((str = in.readLine()) != null) {
				if (lineNo > 1) {
					specLines.add(str);
				}
				lineNo++;
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return specLines;
	}
}
