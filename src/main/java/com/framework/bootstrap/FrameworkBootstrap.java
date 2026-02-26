package com.framework.bootstrap;

import com.framework.broker.core.MessageBroker;
import com.framework.broker.dispatcher.DispatcherConfig;
import com.framework.broker.dispatcher.EventDispatcher;
import com.framework.eventbus.*;
import com.framework.gateway.*;
import com.framework.registry.*;
import com.framework.resilience.*;
import com.framework.tracing.EventTracer;

public class FrameworkBootstrap {

    private final MessageBroker broker;
    private final EventDispatcher dispatcher;
    private final EventBus eventBus;

    private final ServiceRegistry serviceRegistry;
    private final HeartbeatManager heartbeatManager;

    private final ApiGateway apiGateway;

    private final CircuitBreaker circuitBreaker;
    private final RetryExecutor retryExecutor;
    private final TimeoutExecutor timeoutExecutor;

    public FrameworkBootstrap() {

        // 1️⃣ Broker Layer
        broker = new MessageBroker(1000);

        DispatcherConfig dispatcherConfig =
                new DispatcherConfig(4, 5000);

        dispatcher = new EventDispatcher(dispatcherConfig);

        // 2️⃣ EventBus Layer
        BrokerAdapter adapter =
                new BrokerAdapter(broker, dispatcher);

        EventSerializer serializer =
                new EventSerializer();

        eventBus = new EventBus(adapter, serializer);

        // 3️⃣ Registry Layer
        serviceRegistry = new ServiceRegistry();

        heartbeatManager =
                new HeartbeatManager(serviceRegistry,
                        15000, // TTL
                        5000   // Cleanup interval
                );

        // 4️⃣ Resilience Layer
        circuitBreaker =
                new CircuitBreaker(3, 10000);

        retryExecutor =
                new RetryExecutor(3, 500);

        timeoutExecutor =
                new TimeoutExecutor();

        // 5️⃣ Gateway Layer
        LoadBalancer loadBalancer =
                new LoadBalancer();

        RateLimiter rateLimiter =
                new RateLimiter(100, 60000);

        apiGateway =
                new ApiGateway(serviceRegistry,
                        loadBalancer,
                        rateLimiter);

        EventTracer.trace("Framework initialized.");
    }

    // ===============================
    // Public Accessors
    // ===============================

    public EventBus getEventBus() {
        return eventBus;
    }

    public ApiGateway getApiGateway() {
        return apiGateway;
    }

    public ServiceRegistry getServiceRegistry() {
        return serviceRegistry;
    }

    public CircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    public RetryExecutor getRetryExecutor() {
        return retryExecutor;
    }

    public TimeoutExecutor getTimeoutExecutor() {
        return timeoutExecutor;
    }

    // ===============================
    // Shutdown Lifecycle
    // ===============================

    public void shutdown() {

        EventTracer.trace("Shutting down framework...");

        dispatcher.shutdown();
        heartbeatManager.shutdown();
        timeoutExecutor.shutdown();

        EventTracer.trace("Framework shutdown complete.");
    }
}