package io.orderedoutbox.outbox;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

@ApplicationScoped
public class OutboxRelay {

    private static final Logger LOG = Logger.getLogger(OutboxRelay.class);

    @Inject
    OutboxSettings settings;

    @Inject
    OutboxConfig config;

    @Inject
    NaivePublisher naive;

    @Inject
    SafePublisher safe;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile ReentrantLock[] locks;

    void onStart(@Observes StartupEvent event) {
        if (config.publisher().autoStart()) {
            start();
        }
    }

    void onStop(@Observes ShutdownEvent event) {
        running.set(false);
    }

    public synchronized void start() {
        if (running.get()) {
            return;
        }
        int workers = settings.workers();
        ReentrantLock[] created = new ReentrantLock[workers];
        for (int i = 0; i < workers; i++) {
            created[i] = new ReentrantLock();
        }
        locks = created;
        running.set(true);
        for (int i = 0; i < workers; i++) {
            int workerId = i;
            Thread.ofVirtual().name("outbox-relay-" + workerId).start(() -> loop(workerId));
        }
        LOG.infof("outbox relay started mode=%s workers=%d shards=%d", settings.mode(), workers, settings.shards());
    }

    /**
     * Stops new ticks and waits until every worker has left its current one.
     * Callers then write a complete aggregate history before {@link #resume()}.
     */
    public void pauseAndAwait() {
        settings.pause();
        ReentrantLock[] current = locks;
        if (current == null) {
            return;
        }
        for (ReentrantLock lock : current) {
            lock.lock();
            lock.unlock();
        }
    }

    public void resume() {
        settings.resume();
    }

    private void loop(int workerId) {
        ReentrantLock lock = locks[workerId];
        while (running.get()) {
            boolean worked = false;
            lock.lock();
            try {
                if (running.get() && !settings.paused()) {
                    int published = switch (settings.mode()) {
                        case NAIVE -> naive.publishTick(workerId);
                        case SAFE -> safe.publishTick(workerId);
                    };
                    worked = published > 0;
                }
            } catch (RuntimeException exception) {
                LOG.errorf(exception, "outbox worker %d failed", workerId);
            } finally {
                lock.unlock();
            }
            if (!worked) {
                sleep(settings.pollInterval());
            }
        }
    }

    private void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            running.set(false);
        }
    }
}
