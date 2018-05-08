package DirectReplyTo;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class DirectReplyToRPCClient {

    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";
    private final String REPLY_TO_QUEUE = "amq.rabbitmq.reply-to";

    public DirectReplyToRPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        connection = factory.newConnection();
        channel = connection.createChannel();

        // don't create a one time use queue.
    }

    public String call(String message) throws IOException, InterruptedException {

        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .replyTo(REPLY_TO_QUEUE)
                .build();

        // for some reason, make absolute sure that the basic.consume definition is before the basic.publish
        // otherwise you'll get a channel closed error for some reason.

        // constuct a java blocking queue to block until you hear a response back.
        final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);

        // for the client, listen to the pseudo queue amq.rabbitmq.reply-to in no-ack mode
        channel.basicConsume(REPLY_TO_QUEUE, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                response.offer(new String(body, "UTF-8"));

            }
        });

        // publish to the task queue that the server is listening on
        channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));

        // pop the head of the queue, blocking of necessary
        // blocking should not be necessary at this point, and if you block here, this will be a deadlock.
        return response.take();
    }

    public void close() throws IOException {
        connection.close();
    }

    public static void main(String[] argv) {
        DirectReplyToRPCClient fibonacciRpc = null;
        String response = null;
        try {
            fibonacciRpc = new DirectReplyToRPCClient();

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