package DirectReplyTo;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class DirectReplyToRPCServer {

    private static final String RPC_QUEUE_NAME = "rpc_queue";

    private static int fib(int n) {
        if (n ==0) return 0;
        if (n == 1) return 1;
        return fib(n-1) + fib(n-2);
    }

    public static void main(String[] argv) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = null;
        try {
            connection      = factory.newConnection();
            final Channel channel = connection.createChannel();

            // you only declare the rpc queue to push calls into in the server thread.
            // the client thread will only define the reply queue that the server sends to.
            channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);

            channel.basicQos(1);

            System.out.println(" [x] Awaiting RPC requests");

            // this consumer will handle requests from the client.
            // I think this default consumer is an async thread.
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                            .Builder()
                            .build();

                    // response to send back to RCPClient. this is the answer to fib(number)
                    // this needs to be declared outside of the try catch finally block so that it can
                    // be referred to both the try block and the finally block.
                    String response = "";

                    try {
                        String message = new String(body,"UTF-8");
                        int n = Integer.parseInt(message);

                        System.out.println(" [.] fib(" + message + ")");
                        response += fib(n);
                    }
                    catch (RuntimeException e){
                        System.out.println(" [.] " + e.toString());
                    }
                    finally {
                        // publish parameters: exchange name, routing key, reply properties, and actual binary msg.
                        channel.basicPublish( "", properties.getReplyTo(), replyProps, response.getBytes("UTF-8"));

                        // DO NOT FORGET THIS
                        // the ack is how the worker tells the caller that the works is done for sure.
                        // without the ack, the client can't know that the work is actually done.
                        // forgetting this causes rabbitmq to keep resending messages when you close a worker thread that
                        // has received it, and no messages are ever dropped as none of them ever get ack'ed.
                        channel.basicAck(envelope.getDeliveryTag(), false);
                        // RabbitMq consumer worker thread notifies the RPC server owner thread
                        synchronized(this) {
                            // the consumer
                            // this = consumer. consumer.notify() will trigger RPCserver.java who is waiting on consumer.
                            System.out.println("about to call consumer.notify()...");
                            this.notify();
                            System.out.println("called consumer.notify()...");
                        }
                    }
                }
            };

            channel.basicConsume(RPC_QUEUE_NAME, false, consumer);
            // Wait and be prepared to consume the message from RPC client.
            while (true) {
                // purpose of synchronized(consumer):
                // if there are multiple RPCServer instances running, only one can interface with the consumer at a time.
                synchronized(consumer) {
                    try {
                        // wait for a consumer.notify(), which the consumer instance will do upon task completion.
                        System.out.println("about to call consumer.wait()");
                        consumer.wait();
                        System.out.println("called consumer.wait()");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e){
            e.printStackTrace();
        }
        finally {
            if (connection != null)
                try {
                    connection.close();
                } catch (IOException _ignore) {}
        }
    }
}
