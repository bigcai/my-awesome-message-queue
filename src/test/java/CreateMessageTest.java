import io.openmessaging.Message;
import io.openmessaging.demobackup.DefaultBytesMessage;

/**
 * @author jason.shang
 */
public class CreateMessageTest {

    public static void main(String[] args){

        // 创建100W个Message对象测试耗时

        long start = System.currentTimeMillis();
        for(int i = 0; i < 10_000_000; i++){
            Message message = new DefaultBytesMessage(null);
        }
        System.out.println("elapsed: " + (System.currentTimeMillis() - start) + " ms");


        // 测试instanceof 性能
        // 10亿次instanceof测试耗时
        Message message = new DefaultBytesMessage(null);
        boolean result;
        start = System.currentTimeMillis();
        for(int i = 0; i < 1_000_000_000; i++){
            result = message instanceof Message;
        }
        System.out.println("instanceof elapsed: " + (System.currentTimeMillis() - start) + " ms");
    }

}
