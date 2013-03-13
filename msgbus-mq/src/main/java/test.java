import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.client.netty.HedwigPublisher;
import org.apache.hedwig.exceptions.PubSubException;

import com.google.protobuf.ByteString;
import com.yonyou.msgbus.client.MsgBusClient;
public class test {

	/**
	 * @param args
	 */

	public static void main(String[] args) throws Exception {
		final String queueName = "t_test";

		// 获取消息队列客户端
		String path = "F:/Java Projects2/msgbus/hw_client.conf";
		final MsgBusClient client = new MsgBusClient(path);		
		
		/*((HedwigPublisher)client.publisher).createQueue(ByteString.copyFromUtf8(queueName),new Callback<ResponseBody>(){

			@Override
			public void operationFinished(Object ctx, ResponseBody resultOfOperation) {
				// TODO Auto-generated method stub
				System.out.println("queue is created.");
			}

			@Override
			public void operationFailed(Object ctx, PubSubException exception) {
				// TODO Auto-generated method stub
				System.out.println("Operation failed.");
			}
			
		},null);*/
		
		((HedwigPublisher)client.publisher).deleteQueue(ByteString.copyFromUtf8(queueName),new Callback<ResponseBody>(){

			@Override
			public void operationFinished(Object ctx, ResponseBody resultOfOperation) {
				// TODO Auto-generated method stub
				System.out.println("queue is deleted.");
			}

			@Override
			public void operationFailed(Object ctx, PubSubException exception) {
				// TODO Auto-generated method stub
				System.out.println("Operation failed.");
			}
			
		},null);
		Thread.sleep(5000);
		client.close();
	}
}
