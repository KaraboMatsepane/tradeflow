package com.tradeflow.consumer;

import com.tradeflow.config.JNDIContextFactory;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class RiskValidator {

    public static void main(String[] args) {

        try {
            //Looks up ConnectionFactory, PENDING.TRADES, and RISK.APPROVED via JNDI
            InitialContext ctx = getContext();
            ConnectionFactory cf = getConnectionFactory(ctx);
            Queue pendingTrades = getQueue(ctx, "PENDING.TRADES");
            Queue riskApproved = getQueue(ctx, "RISK.APPROVED");


            try(Connection conn = createConnection(cf)){
                conn.start();

                //Creates a SESSION_TRANSACTED session
                Session session = createSession(conn);


                //Creates both a MessageConsumer (from PENDING.TRADES) and a MessageProducer (to RISK.APPROVED) on the same session
                MessageProducer msgProducer = createMessageProducer(session, riskApproved);

                MessageConsumer msgConsumer = createMessageConsumer(session, pendingTrades);

                //Loops with receive(2000), breaks on null
                processMessage(session, msgConsumer, msgProducer);

            } catch (JMSException e){
                throw new RuntimeException(e);
            }


        } catch(NamingException e){
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

    public static Session createSession(Connection conn) throws JMSException {
        return conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
    }

    public static Connection createConnection(ConnectionFactory cf) throws JMSException {
        return cf.createConnection();
    }

    public static MessageConsumer createMessageConsumer(Session session, Queue queue) throws JMSException {
        return session.createConsumer(queue);
    }

    public static MessageProducer createMessageProducer(Session session, Queue queue) throws JMSException {
        return session.createProducer(queue);
    }

    public static void processMessage(Session session, MessageConsumer consumer, MessageProducer producer) throws JMSException {

        while (true) {
            MapMessage msg = (MapMessage) consumer.receive(2000);

            if (msg == null) break;

            if (Math.random() <= 0.3) {

                System.out.println(
                        "Failure sending trade: ID=" + msg.getString("tradeId") +
                                ", Value=" + msg.getDouble("tradeValue") +
                                ", Symbol=" + msg.getString("symbol")
                );

                session.rollback();
            } else {
                MapMessage message = session.createMapMessage();
                message.setString("tradeId", msg.getString("tradeId"));
                message.setString("tradeStatus", "APPROVED");
                message.setDouble("tradeValue", msg.getDouble("tradeValue"));
                message.setString("symbol", msg.getString("symbol"));

                producer.send(message);
                session.commit();

                System.out.println(
                        "Trade sent: ID=" + message.getString("tradeId") +
                                ", Status=" + message.getString("tradeStatus") +
                                ", Value=" + msg.getDouble("tradeValue") +
                                ", Symbol=" + msg.getString("symbol")
                );
            }
        }
    }
}
