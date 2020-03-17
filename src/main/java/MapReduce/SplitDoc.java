package MapReduce;

import java.io.File;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SplitDoc {
    public static final Pattern HEAD_PATTERN = Pattern.compile("^\\[{2}.*\\]{2}");
    public static void main(String[] args) throws Exception{
        String sample_path = "/home/molin/Documents/Data/input/sample.txt";
        File sample_file = new File(sample_path);
        Scanner reader = new Scanner(sample_file);
        int counter = 0;
        while(reader.hasNextLine()){
            String data = reader.nextLine();
            Matcher titleMatcher = HEAD_PATTERN.matcher(data);
            if(titleMatcher.find()){
                counter+=1;
                System.out.println(titleMatcher.group(0));
            }
        }
        System.out.println(counter);
    }
}
