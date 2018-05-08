package TutorialFive;

import com.rabbitmq.client.*;

public class EmitLogTopic {

    private static final String EXCHANGE_NAME = "topic_logs";

    // arg: cute.orange.rabbit
    private static String getRouting(String [] args){
        if(args.length == 0){
            return "*.orange.*";
        } else {
            return args[0];
        }
    }

    private static String getSeverity(String[] strings){
        if (strings.length == 1)
            return "error";
        return strings[0];
    }

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

    public static void main(String[] argv)
            throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "topic");

        String routingKey = getRouting(argv);
        String message = getMessage(argv);

        channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
        System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");

        connection.close();
    }
    //...
}