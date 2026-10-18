package com.tradeflow.producer;

import com.tradeflow.config.JNDIContextFactory;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * JMS producer that submits trades to the {@code PENDING.TRADES} queue.
 *
 * <p>When run, it sends five {@link MapMessage} trades with status {@code PENDING}
 * and an incrementing trade value, so downstream consumers such as
 * {@link com.tradeflow.consumer.RiskValidator} have messages to process.</p>
 *
 * <p>Each message carries the following map entries: {@code tradeId},
 * {@code tradeStatus}, {@code tradeValue} and {@code symbol}.</p>
 */
public class TradeSubmitter {

    /**
     * Looks up the JMS resources via JNDI, then sends five pending trades to
     * {@code PENDING.TRADES} and logs each one to standard output.
     *
     * @param args unused
     * @throws RuntimeException if a JNDI lookup or a JMS operation fails
     */
    public static void main (String[] args) {

        try{
            InitialContext ctx = getContext();

            ConnectionFactory cf = getConnectionFactory(ctx);
            Queue pendingTrades = getQueue(ctx, "PENDING.TRADES");

            try (Connection conn = getConnection(cf)){
                Session session = getSession(conn);
                MessageProducer msgProducer = getProducer(session, pendingTrades);

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

    /**
     * Obtains the JNDI context used to look up JMS resources.
     *
     * @return the JNDI {@link InitialContext}
     * @throws NamingException if the context cannot be created
     */
    public static InitialContext getContext() throws NamingException {
       return JNDIContextFactory.getContext();
    }

    /**
     * Looks up a queue by its JNDI name.
     *
     * @param ctx   the JNDI context
     * @param queue the JNDI name of the queue
     * @return the {@link Queue} bound to that name
     * @throws NamingException if the name is not bound
     */
    public static Queue getQueue(InitialContext ctx, String queue) throws NamingException {
        return (Queue) ctx.lookup(queue);
    }

    /**
     * Creates a producer that sends messages to the given queue.
     *
     * @param session the session that owns the producer
     * @param queue   the destination queue
     * @return a new {@link MessageProducer}
     * @throws JMSException if the producer cannot be created
     */
    public static MessageProducer getProducer(Session session, Queue queue) throws JMSException {

        return session.createProducer(queue);
    }

    /**
     * Opens a connection to the JMS provider.
     *
     * @param cf the connection factory
     * @return a new {@link Connection}
     * @throws JMSException if the connection cannot be created
     */
    public static Connection getConnection(ConnectionFactory cf) throws JMSException {
        return cf.createConnection();
    }

    /**
     * Creates a non-transacted session with automatic acknowledgement.
     *
     * @param conn the connection that owns the session
     * @return a new {@link Session}
     * @throws JMSException if the session cannot be created
     */
    public static Session getSession(Connection conn) throws JMSException {
        return conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    /**
     * Builds a trade message.
     *
     * @param session     the session used to create the message
     * @param tradeId     the unique trade identifier
     * @param tradeStatus the trade status, for example {@code PENDING}
     * @param tradeValue  the monetary value of the trade
     * @param symbol      the traded instrument symbol
     * @return a {@link MapMessage} populated with the trade fields
     * @throws JMSException if the message cannot be created or populated
     */
    public static MapMessage createMapMessage(Session session, String tradeId, String tradeStatus, double tradeValue, String symbol) throws JMSException {
        MapMessage msg = session.createMapMessage();

        msg.setString("tradeId",tradeId);
        msg.setString("tradeStatus", tradeStatus);
        msg.setDouble("tradeValue", tradeValue);
        msg.setString("symbol", symbol);

        return msg;
    }

    /**
     * Looks up the JMS {@link ConnectionFactory} bound as {@code ConnectionFactory}.
     *
     * @param ctx the JNDI context
     * @return the connection factory
     * @throws NamingException if the name is not bound
     */
    public static ConnectionFactory getConnectionFactory(InitialContext ctx) throws NamingException {

        return (ConnectionFactory) ctx.lookup("ConnectionFactory");
    }
}
