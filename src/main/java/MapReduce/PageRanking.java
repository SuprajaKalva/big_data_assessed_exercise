package MapReduce;

import java.io.*;
import java.util.Scanner;
import java.util.StringTokenizer;

public class PageRanking {
    private static String queryText="";
    private String queryFile = "src/main/resources/temp_query.txt";


    public void QueryInput() throws IOException {
        System.out.println("Query:");
        //BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        //String s = br.readLine();
        String s = "wotanism details troth";
        this.queryText = s;
        StringTokenizer itr = new StringTokenizer(this.queryText);


        BufferedWriter writer = new BufferedWriter(new FileWriter(this.queryFile));
        while(itr.hasMoreTokens()){
            writer.write(itr.nextToken()+"\n");
        }
        writer.close();
    }

    //public static class PRMapper extends Mapper<>
    public static void main(String [] args) throws IOException{

        PageRanking pr = new PageRanking();
        pr.QueryInput();
    }
}
