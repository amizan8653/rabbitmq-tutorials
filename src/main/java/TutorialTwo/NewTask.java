package TutorialTwo;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class NewTask {
    private final static String QUEUE_NAME = "task_queue";

    private static String getMessage(String[] strings){
        if (strings.length < 1)
            return "Hello World!";
        return joinStrings(strings, " ");
    }

    private static String joinStrings(String[] strings, String delimiter) {
        int length = strings.length;
        if (length == 0) return "";
        StringBuilder words = new StringBuilder(strings[0]);
        for (int i = 1; i < length; i++) {
            words.append(delimiter).append(strings[i]);
        }
        return words.toString();
    }

    public static void main(String[] argv) throws java.io.IOException, java.util.concurrent.TimeoutException {

        // connection factory abstracts away all the socket stuff where 2 end points have to authenticate a channel with each other
        ConnectionFactory factory = new ConnectionFactory();

        // the client that's trying to connect to the rabbitmq server is localhost. If it were something else, you'd specify the url here.
        factory.setHost("localhost");


        Connection connection = factory.newConnection();

        // declare a channel, where most of the rabbitmq api resides.
        Channel channel = connection.createChannel();

        // tell rabbitmq to give the task to worker threads if they are free, instead of blindly doing a round
        // robin dispatch. A thread is free if it has sent an ack for a task it was given.
        int prefetchCount = 1;
        channel.basicQos(prefetchCount);

        boolean durable = true;

        // declare a queue that you can send a message to
        // delcaring a queue is idempotent: a queue with the name described wil only be created if it doesn't already exist
        // a string is being sent here, but really anything can be sent since the object is encoded into a byte array.
        channel.queueDeclare(QUEUE_NAME, durable, false, false, null);

        String message = getMessage(argv);

        //  MessageProperties.PERSISTENT_TEXT_PLAIN makes the messages persistent.
        //  now when the rabbitmq server stops, and message was in the queue, it might stay in the queue.
        //  there are chances of failure: rabbitmq doesn't do fsync(2) for every message, so it's possible that
        //  the message is in the cache and the the hard drive.
        //  also, there's a short period of time where a message is accepted to rabbitmq, and the message is
        //  saved to the hard drive.
        channel.basicPublish("", QUEUE_NAME,  MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");

        // close the channel and close the connection:
        channel.close();
        connection.close();

    }

}
