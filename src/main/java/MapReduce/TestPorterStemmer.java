package MapReduce;


import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author ksr
 */
public class TestPorterStemmer{

    private static final Logger logger = LoggerFactory.getLogger(TestPorterStemmer.class);

    @Test
    public void testStemming() throws Exception {
        List<String> words = Arrays.asList("eating", "going", "done", "eaten");
        MapReduce.PorterStemmer porterStemmer = new MapReduce.PorterStemmer();

        List<String> ported_words = new ArrayList<>();

        words.forEach(word -> {
            ported_words.add(porterStemmer.stem(word));
//            logger.info("{} -> {}", stem, word);
        });

        String line = String.join(" ", ported_words);

        System.out.println(ported_words);
        System.out.println(line);

    }

}
