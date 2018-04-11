package flinkWordCount;

/**
 * Create by Zhao Qing on 2018/4/11
 */
public class CmdArgsTest {
    public static void main(String[] args){
        System.out.println("length: " + args.length);
        for (String s : args){
            System.out.println(s);
        }
    }
}
