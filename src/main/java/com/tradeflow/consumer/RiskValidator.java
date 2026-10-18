package com.tradeflow.consumer;

import com.tradeflow.config.JNDIContextFactory;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * Transactional JMS consumer/producer that performs a simulated risk check.
 *
 * <p>Trades are consumed from {@code PENDING.TRADES}. Each trade has roughly a
 * 30% chance of failing validation, in which case the session is rolled back and
 * the message is redelivered (subject to the provider's redelivery policy).
 * Otherwise an {@code APPROVED} copy is published to {@code RISK.APPROVED} and
 * the session is committed.</p>
 *
 * <p>Consuming and producing share one {@link Session#SESSION_TRANSACTED
 * transacted} session, so the receive and the send succeed or fail together.</p>
 */
public class RiskValidator {

    /**
     * Looks up the JMS resources via JNDI and validates pending trades until no
     * message arrives within the receive timeout.
     *
     * @param args unused
     * @throws RuntimeException if a JNDI lookup or a JMS operation fails
     */
    public static void main(String[] args) {

        try {
            InitialContext ctx = getContext();
            ConnectionFactory cf = getConnectionFactory(ctx);
            Queue pendingTrades = getQueue(ctx, "PENDING.TRADES");
            Queue riskApproved = getQueue(ctx, "RISK.APPROVED");

            try(Connection conn = createConnection(cf)){
                conn.start();

                Session session = createSession(conn);

                MessageProducer msgProducer = createMessageProducer(session, riskApproved);

                MessageConsumer msgConsumer = createMessageConsumer(session, pendingTrades);

                processMessage(session, msgConsumer, msgProducer);

            } catch (JMSException e){
                throw new RuntimeException(e);
            }

        } catch(NamingException e){
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
     * Creates a transacted session. Work done on it only takes effect after
     * {@link Session#commit()}, and is undone by {@link Session#rollback()}.
     *
     * @param conn the connection that owns the session
     * @return a new transacted {@link Session}
     * @throws JMSException if the session cannot be created
     */
    public static Session createSession(Connection conn) throws JMSException {
        return conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
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
     * Creates a producer that sends messages to the given queue.
     *
     * @param session the session that owns the producer
     * @param queue   the destination queue
     * @return a new {@link MessageProducer}
     * @throws JMSException if the producer cannot be created
     */
    public static MessageProducer createMessageProducer(Session session, Queue queue) throws JMSException {
        return session.createProducer(queue);
    }

    /**
     * Receives trades and validates them until a 2 second receive times out.
     *
     * <p>A failed validation rolls the session back so the trade is redelivered;
     * a successful one sends an {@code APPROVED} trade and commits the session.</p>
     *
     * @param session  the transacted session shared by the consumer and producer
     * @param consumer the consumer reading pending trades
     * @param producer the producer publishing approved trades
     * @throws JMSException if receiving, sending, committing or rolling back fails
     */
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
