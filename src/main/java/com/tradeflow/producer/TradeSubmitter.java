package com.tradeflow.producer;

import com.tradeflow.config.JNDIContextFactory;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class TradeSubmitter {

    public static void main (String[] args) {



        try{
            //Gets an InitialContext via JNDIContextFactory
            InitialContext ctx = getContext();

            //Looks up ConnectionFactory and the PENDING.TRADES queue
            ConnectionFactory cf = (ConnectionFactory) ctx.lookup("ConnectionFactory");
            Queue pendingTrades = getQueue(ctx);

            //Creates a Connection → Session → MessageProducer
            try (Connection conn = getConnection(cf)){
                Session session = getSession(conn);
                MessageProducer msgProducer = getProducer(session, pendingTrades);

                // sends 5 map messages, each with tradeId, tradeStatus("PENDING"), tradeValue, symbol
                // At least one trade must have tradeValue > 50000
                int tradeId = 0;
                String symbol = "AAL";
                double tradeValue = 49997;
                for (int i = 0; i < 5; i++){
                    MapMessage msg = createMapMessage(session, String.valueOf(++tradeId), "PENDING",tradeValue++, symbol);
                    msgProducer.send(msg);

                    System.out.println(
                            "Trade sent: ID=" + msg.getString("tradeId") +
                                    ", Status=" + msg.getString("tradeStatus") +
                                    ", Value=" + msg.getDouble("tradeValue") +
                                    ", Symbol=" + msg.getString("symbol")
                    );
                }

            } catch (JMSException e) {
                throw new RuntimeException(e);
            }

        } catch (NamingException e){
            throw new RuntimeException(e);
        }
    }

    public static InitialContext getContext() throws NamingException {
       return JNDIContextFactory.getContext();
    }

    public static Queue getQueue(InitialContext ctx) throws NamingException {
        return (Queue) ctx.lookup("PENDING.TRADES");
    }

    public static MessageProducer getProducer(Session session, Queue queue) throws JMSException {

        return session.createProducer(queue);
    }

    public static Connection getConnection(ConnectionFactory cf) throws JMSException {
        return cf.createConnection();
    }

    public static Session getSession(Connection conn) throws JMSException {
        return conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    public static MapMessage createMapMessage(Session session, String tradeId, String tradeStatus, double tradeValue, String symbol) throws JMSException {
        MapMessage msg = session.createMapMessage();

        msg.setString("tradeId",tradeId);
        msg.setString("tradeStatus", tradeStatus);
        msg.setDouble("tradeValue", tradeValue);
        msg.setString("symbol", symbol);

        return msg;
    }
}