package MapReduce;

import java.io.File;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SplitDoc {
    public static final Pattern HEAD_PATTERN = Pattern.compile("\\[{2}.*\\]{2}");
    public static final Pattern NONASCII = Pattern.compile("[^\\x20-\\x7e]");
    public static final Pattern articlePattern = Pattern.compile("\\[{2}.*\\].*");

    public static void main(String[] args) throws Exception {
        String inputfile_path = "src/main/resources/sample.txt";
        TextPreprocess tp = new TextPreprocess();
        File temp_file = tp.textCleaner(inputfile_path);
        String sample_path = temp_file.getAbsolutePath();
        File sample_file = new File(sample_path);
        Scanner reader = new Scanner(sample_file);
        int counter = 0;
        while (reader.hasNextLine()) {
            String data = reader.nextLine();
            Matcher articleMatcher = articlePattern.matcher(data);
            Matcher titleMatcher = HEAD_PATTERN.matcher(data);
            Matcher nonasciiMatcher = NONASCII.matcher(data);
            if (titleMatcher.find()){
                String title = titleMatcher.group(0);
                if (title.length()>50){
                    counter+=1;
                    System.out.println(title);
                }
            }
        }
        System.out.println(counter);
    }
}
