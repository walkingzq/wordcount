import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
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

    @Test
    public void comm(){
        String timestamp = "CreateTime:1524449914670";
        System.out.println(timestamp.split(":")[1]);
    }

    @Test
    public void time(){
        System.out.println(new Date().getTime());
        System.out.println(System.currentTimeMillis() / 100L);
        System.out.println(Calendar.getInstance().getTimeInMillis());
    }

}
