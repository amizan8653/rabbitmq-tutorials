package TutorialSix;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class RPCClient {

    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";
    private String replyQueueName;

    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        connection = factory.newConnection();
        channel = connection.createChannel();

        // you only declare the rpc queue to push calls into in the server thread.
        // the client thread will only define the reply queue that the server sends to.
        // the reply queue is a non durable queue t
        replyQueueName = channel.queueDeclare().getQueue();
    }

    public String call(String message) throws IOException, InterruptedException {
        final String corrId = UUID.randomUUID().toString();

        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        // publish to the task queue that the server is listening on
        channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));

        // constuct a java blocking queue to block until you hear a response back.
        final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);

        // for the client specific reply queue, listen for and consume a response.
        channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                // only try to consume the message if the correlation id matches the one that you randomly generated.
                if (properties.getCorrelationId().equals(corrId)) {
                    response.offer(new String(body, "UTF-8"));
                }
            }
        });

        // pop the head of the queue, blocking of necessary
        // blocking should not be necessary at this point, and if you block here, this will be a deadlock.
        return response.take();
    }

    public void close() throws IOException {
        connection.close();
    }

    public static void main(String[] argv) {
        RPCClient fibonacciRpc = null;
        String response = null;
        try {
            fibonacciRpc = new RPCClient();

            System.out.println(" [x] Requesting fib(30)");
            response = fibonacciRpc.call("30");
            System.out.println(" [.] Got '" + response + "'");
        }
        catch(IOException e){
            e.printStackTrace();
        } catch (TimeoutException e){
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        }
        finally {
            if (fibonacciRpc!= null) {
                try {
                    fibonacciRpc.close();
                }
                catch (IOException _ignore) {}
            }
        }
    }
}