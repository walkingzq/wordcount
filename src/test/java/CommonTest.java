import org.junit.Test;

import java.util.StringTokenizer;

/**
 * Create by Zhao Qing on 2018/4/8
 */
public class CommonTest {
    @Test
    public void stringTokenizerTest(){
        StringBuilder sb = new StringBuilder();
        String str = "hello world   hello" +
                " hadoop!";
        StringTokenizer st = new StringTokenizer(str);
        while (st.hasMoreTokens()){
            System.out.println(st.nextToken());
        }
    }

}
