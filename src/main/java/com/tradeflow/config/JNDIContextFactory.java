package com.tradeflow.config;

import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * Utility factory for obtaining JNDI {@link InitialContext} instances.
 *
 * <p>The returned context is configured through the {@code jndi.properties} file
 * on the classpath, so that resources like JMS connection factories and queues
 * can be looked up by name.</p>
 *
 * <p>This class is not meant to be instantiated.</p>
 */
public class JNDIContextFactory {

    /**
     * Prevents instantiation of this utility class.
     */
    private JNDIContextFactory(){}

    /**
     * Creates a new JNDI initial context using the environment configured for
     * the application.
     *
     * @return a new {@link InitialContext}
     * @throws NamingException if the context cannot be created, for example
     *                         because the JNDI environment is misconfigured
     */
    public static InitialContext getContext() throws NamingException {
        return new InitialContext();
    }
}
