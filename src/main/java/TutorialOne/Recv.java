package TutorialOne;
import com.rabbitmq.client.*;

import java.io.IOException;


public class Recv {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws java.io.IOException, java.util.concurrent.TimeoutException {


        // just like in the sender, setup the connection and channel
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // just like in the sender,
        // declare the queue. it will only be created it it doesn't already exist.
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        // note, since the consumer and sender as asynchronous threads, you need to give the channel a callback
        // function to buffer the message bytes until the receiver is ready to consume that.
        // you can do that by providing an object of type Consumer that has the appropriate functions
        // for consuming the msg.
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
            }
        };


        // consume a message, storing the message in the consumer object.
        channel.basicConsume(QUEUE_NAME, true, consumer);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // Receiver still runs. I guess the main method exits, but there's an asynch process running which
        // usees the consumer to keep consuming new messages.

    }
}
