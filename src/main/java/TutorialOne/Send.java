package TutorialOne;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

public class Send {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws java.io.IOException, java.util.concurrent.TimeoutException {

        // connection factory abstracts away all the socket stuff where 2 end points have to authenticate a channel with each other
        ConnectionFactory factory = new ConnectionFactory();

        // the client that's trying to connect to the rabbitmq server is localhost. If it were something else, you'd specify the url here.
        factory.setHost("localhost");


        Connection connection = factory.newConnection();

        // declare a channel, where most of the rabbitmq api resides.
        Channel channel = connection.createChannel();

        // declare a queue that you can send a message to
        // delcaring a queue is idempotent: a queue with the name described wil only be created if it doesn't already exist
        // a string is being sent here, but really anything can be sent since the object is encoded into a byte array.
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        String message = "Hello World! This is some Message.";
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");

        // close the channel and close the connection:
        channel.close();
        connection.close();

    }

}
