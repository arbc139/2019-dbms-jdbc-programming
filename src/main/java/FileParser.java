import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;

public class FileParser {
    public void open(String path) throws FileNotFoundException {
        File file = new File(path);
        scanner = new Scanner(file);
    }

    public void close() {
        scanner.close();
    }

    private Map<String, String> parse(String token) throws RuntimeException {
        Map<String, String> result = new HashMap<String, String>();
        while (scanner.hasNextLine()) {
            String[] entry = scanner.nextLine().split(token);
            if (entry.length != 2) {
                Main.LOG.log(Level.SEVERE, String.format("Invalid file format: %s", token));
                throw new RuntimeException("[Error][FileParser] Invalid file format!");
            }
            String key = entry[0].trim();
            String value = entry[1].trim();
            result.put(key, value);
        }
        scanner.close();
        return result;
    }

    public Map<String, String> parseTxt() {
        return parse(":");
    }

    public Map<String, String> parseCsv() {
        return parse(",");
    }

    private Scanner scanner;
}
