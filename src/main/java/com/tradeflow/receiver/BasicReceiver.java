package com.tradeflow.receiver;

import com.tradeflow.config.JNDIContextFactory;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * Simple JMS consumer that reads trades from the {@code PENDING.TRADES} queue
 * and prints them to standard output.
 *
 * <p>It uses a non-transacted, auto-acknowledge session and stops once no
 * message arrives within the receive timeout.</p>
 */
public class BasicReceiver {

    /**
     * Looks up the JMS resources via JNDI and prints every pending trade
     * until a 2 second receive times out.
     *
     * @param args unused
     * @throws RuntimeException if a JNDI lookup or a JMS operation fails
     */
    public static void main(String[] args) {

        try{
            InitialContext ctx = getContext();
            ConnectionFactory cf = getConnectionFactory(ctx);
            Queue pendingTrades = getQueue(ctx, "PENDING.TRADES");

            try (Connection conn =createConnection(cf)){
                conn.start();
                Session session = createSession(conn);

                MessageConsumer msgConsumer = createMessageConsumer(session, pendingTrades);

                receiveMessage(msgConsumer);

            } catch (JMSException e){
                throw new RuntimeException(e);
            }
        }catch (NamingException  e){
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
     * Looks up the JMS {@link ConnectionFactory} bound as {@code ConnectionFactory}.
     *
     * @param ctx the JNDI context
     * @return the connection factory
     * @throws NamingException if the name is not bound
     */
    public static ConnectionFactory getConnectionFactory(InitialContext ctx) throws NamingException {

        return (ConnectionFactory) ctx.lookup("ConnectionFactory");
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
     * Opens a connection to the JMS provider.
     *
     * @param cf the connection factory
     * @return a new {@link Connection}
     * @throws JMSException if the connection cannot be created
     */
    public static Connection createConnection(ConnectionFactory cf) throws JMSException {
        return cf.createConnection();
    }

    /**
     * Creates a non-transacted session with automatic acknowledgement.
     *
     * @param conn the connection that owns the session
     * @return a new {@link Session}
     * @throws JMSException if the session cannot be created
     */
    public static Session createSession(Connection conn) throws JMSException {
        return conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    /**
     * Creates a consumer that receives messages from the given queue.
     *
     * @param session the session that owns the consumer
     * @param queue   the queue to consume from
     * @return a new {@link MessageConsumer}
     * @throws JMSException if the consumer cannot be created
     */
    public static MessageConsumer createMessageConsumer(Session session, Queue queue) throws JMSException {
        return session.createConsumer(queue);
    }

    /**
     * Receives {@link MapMessage} trades and prints their {@code tradeId},
     * {@code tradeStatus}, {@code tradeValue} and {@code symbol}. Returns when a
     * 2 second receive yields no message.
     *
     * @param msgConsumer the consumer to read from
     * @throws JMSException if receiving or reading a message fails
     * @throws ClassCastException if a received message is not a {@link MapMessage}
     */
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
