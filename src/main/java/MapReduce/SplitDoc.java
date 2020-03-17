package MapReduce;

import java.io.File;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SplitDoc {
    public static final Pattern HEAD_PATTERN = Pattern.compile("^\\[{2}.*\\]{2}");
    public static final Pattern NONASCII = Pattern.compile("[^\\x20-\\x7e]");
    public static final Pattern articlePattern = Pattern.compile("\\[{2}.*\\].*");

    public static void main(String[] args) throws Exception {
        TextPreprocess tp = new TextPreprocess();
        File temp_file = tp.textCleaner("/home/molin/Documents/Data/input/sample.txt");
        String sample_path = temp_file.getAbsolutePath();
        File sample_file = new File(sample_path);
        Scanner reader = new Scanner(sample_file);
        int counter = 0;
        while (reader.hasNextLine()) {
            String data = reader.nextLine();
            Matcher articleMatcher = articlePattern.matcher(data);
            Matcher titleMatcher = HEAD_PATTERN.matcher(data);
            Matcher nonasciiMatcher = NONASCII.matcher(data);

            if (articleMatcher.find()) {
                System.out.println(articleMatcher.group(0));
            }

        }
        System.out.println(counter);
    }
}
