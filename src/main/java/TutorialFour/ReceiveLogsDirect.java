package TutorialFour;

import com.rabbitmq.client.*;

import java.io.IOException;

public class ReceiveLogsDirect {

    private static final String EXCHANGE_NAME = "direct_logs";

    // args: info warn error
    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "direct");
        String queueName = channel.queueDeclare().getQueue();

        if (argv.length < 1){
            System.err.println("Usage: ReceiveLogsDirect [info] [warning] [error]");
            System.exit(1);
        }

        // treat each severity as a binding key for the queues.
        // a queue can have multiple binding keys
        // different queues can share the same binding key
        // so binding keys to queues is a many to many relationship basically.
        // the exchange will forward a message to a queue if it has a binding key that matches the message routing key.
        // this is how it all works for DIRECT exchanges. FANOUT exchanges ignore binding keys / routing keys.
        for(String severity : argv){
            channel.queueBind(queueName, EXCHANGE_NAME, severity);
        }
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + envelope.getRoutingKey() + "':'" + message + "'");
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }
}
