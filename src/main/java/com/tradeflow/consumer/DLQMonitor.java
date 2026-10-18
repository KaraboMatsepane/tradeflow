package com.tradeflow.consumer;

import com.tradeflow.config.JNDIContextFactory;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class DLQMonitor {


    public static void main(String[] args) {
        try {
            InitialContext ctx = getContext();
            ConnectionFactory cf = getConnectionFactory(ctx);
            Queue DLQ = getQueue(ctx, "ActiveMQ.DLQ");

            try(Connection conn = cf.createConnection()){
                conn.start();
                Session session = createSession(conn);

                MessageConsumer consumer = createMessageConsumer(session, DLQ);

                receiveMessage(consumer);

            } catch (JMSException e){
                throw new RuntimeException(e);
            }
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }




    public static InitialContext getContext() throws NamingException {
        return JNDIContextFactory.getContext();
    }

    public static ConnectionFactory getConnectionFactory(InitialContext ctx) throws NamingException {

        return (ConnectionFactory) ctx.lookup("ConnectionFactory");
    }

    public static Queue getQueue(InitialContext ctx, String queue) throws NamingException {
        return (Queue) ctx.lookup(queue);
    }

    public static MessageConsumer createMessageConsumer(Session session, Queue queue) throws JMSException {
        return session.createConsumer(queue);
    }


    public static Session createSession(Connection conn) throws JMSException {
        return conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }


    public static void receiveMessage(MessageConsumer msgConsumer) throws JMSException {
        while (true) {
            MapMessage msg = (MapMessage) msgConsumer.receive(2000);
            if (msg == null) break;

            System.out.println(
                    "Delivery count = " + msg.getIntProperty("JMSXDeliveryCount") +
                            ", Original Destination = " + msg.getStringProperty("originalDestination") +
                            "Trade ID=" + msg.getString("tradeId") +
                            ", Status=" + msg.getString("tradeStatus") +
                            ", Value=" + msg.getDouble("tradeValue") +
                            ", Symbol=" + msg.getString("symbol")
            );
        }
    }
}
