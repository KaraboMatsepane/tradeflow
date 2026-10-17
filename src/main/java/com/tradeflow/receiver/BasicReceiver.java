package com.tradeflow.receiver;

import com.tradeflow.config.JNDIContextFactory;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class BasicReceiver {
    public static void main(String[] args) {
        //Looks up ConnectionFactory and PENDING.TRADES via JNDI

        try{
            InitialContext ctx = getContext();
            ConnectionFactory cf = getConnectionFactory(ctx);
            Queue pendingTrades = getQueue(ctx, "PENDING.TRADES");

            // Creates Connection → calls conn.start() → creates Session → creates MessageConsumer
            try (Connection conn =createConnection(cf)){
                conn.start();
                Session session = createSession(conn);

                MessageConsumer msgConsumer = createMessageConsumer(session, pendingTrades);

                //Loops, calling consumer.receive(2000) — breaks when result is null
                //Casts each message to MapMessage and logs tradeId, tradeStatus, tradeValue, symbol
                receiveMessage(msgConsumer);

            } catch (JMSException e){
                throw new RuntimeException(e);
            }
        }catch (NamingException  e){
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

    public static Connection createConnection(ConnectionFactory cf) throws JMSException {
        return cf.createConnection();
    }

    public static Session createSession(Connection conn) throws JMSException {
        return conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    public static MessageConsumer createMessageConsumer(Session session, Queue queue) throws JMSException {
        return session.createConsumer(queue);
    }

    public static void receiveMessage(MessageConsumer msgConsumer) throws JMSException {
        while (true) {
            MapMessage msg = (MapMessage) msgConsumer.receive(2000);
            if (msg == null) break;

            System.out.println(
                    "Trade received: ID=" + msg.getString("tradeId") +
                            ", Status=" + msg.getString("tradeStatus") +
                            ", Value=" + msg.getDouble("tradeValue") +
                            ", Symbol=" + msg.getString("symbol")
            );
        }
    }
}
