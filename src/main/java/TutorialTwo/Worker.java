package TutorialTwo;
import com.rabbitmq.client.*;

import java.io.IOException;


public class Worker {
    private final static String TASK_QUEUE_NAME = "task_queue";

    private static void doWork(String task) throws InterruptedException {
        for (char ch: task.toCharArray()) {
            if (ch == '.') Thread.sleep(1000);
        }
    }

    public static void main(String[] argv) throws IOException, java.util.concurrent.TimeoutException {


        // just like in the sender, setup the connection and channel
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        // tell rabbitmq to give the task to worker threads if they are free, instead of blindly doing a round
        // robin dispatch. A thread is free if it has sent an ack for a task it was given.
        int prefetchCount = 1;
        channel.basicQos(prefetchCount);

        boolean durable = true;

        // just like in the sender,
        // declare the queue. it will only be created it it doesn't already exist.
        // durable = true means that queue isn't deleted when the rabbitmq server stops.
        // by default, durable is set to false.
        channel.queueDeclare(TASK_QUEUE_NAME, durable, false, false, null);

        // note, since the consumer and sender as asynchronous threads, you need to give the channel a callback
        // function to buffer the message bytes until the receiver is ready to consume that.
        // you can do that by providing an object of type Consumer that has the appropriate functions
        // for consuming the msg.
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");

                System.out.println(" [x] Received '" + message + "'");
                try {
                    doWork(message);
                } catch (InterruptedException e) {
                    System.out.println("Interrupt exception thrown");
                    e.printStackTrace();
                } finally {
                    // if a worker thread dies before processing is complete, no acknowledgement is sent
                    // and rabbitmq can then assign another worker / consumer the task.
                    System.out.println(" [x] Done");

                    // DO NOT FORGET THIS
                    // forgetting this causes rabbitmq to keep resending messages when you close a worker thread that
                    // has received it, and no messages are ever dropped as none of them ever get ack'ed.
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };

        // an acknowledgement is sent by a consumer to let rabbitmq know that consumer has received and processed
        // a message or task, and that rabbitmq is free to delete the message/task from the queue.

        // with autoack true, we're turning off the default manual acknowledgement.

        // set autoAck to false so that a worker can send a proper acknowledgement when a task is complete.

        // acknowledgments need to be sent on the same channels that deliveries were received on.

        boolean autoAck = false;
        channel.basicConsume(TASK_QUEUE_NAME, autoAck, consumer);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // Receiver still runs. I guess the main method exits, but there's an asynch process running which
        // usees the consumer to keep consuming new messages.

    }
}
