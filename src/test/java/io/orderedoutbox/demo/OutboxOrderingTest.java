package io.orderedoutbox.demo;

import io.orderedoutbox.api.Views.OrderView;
import io.orderedoutbox.api.Views.ProjectionView;
import io.orderedoutbox.domain.EventPayload;
import io.orderedoutbox.messaging.ConsumptionCounter;
import io.orderedoutbox.messaging.EventPayloadCodec;
import io.orderedoutbox.messaging.NaiveAsyncApplier;
import io.orderedoutbox.messaging.OrderEventSender;
import io.orderedoutbox.outbox.OutboxMode;
import io.orderedoutbox.outbox.OutboxSettings;
import io.orderedoutbox.projection.ProjectionService;
import io.orderedoutbox.query.OrderQuery;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Timeout(value = 4, unit = TimeUnit.MINUTES)
class OutboxOrderingTest {

    @Inject
    OrderQuery query;

    @Inject
    DemoService demo;

    @Inject
    OutboxSettings settings;

    @Inject
    ProjectionService projection;

    @Inject
    NaiveAsyncApplier naiveAsync;

    @Inject
    OrderEventSender sender;

    @Inject
    EventPayloadCodec codec;

    @Inject
    ConsumptionCounter consumed;

    @Inject
    DataSource dataSource;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrap;

    @Test
    @Order(1)
    void restHappyPathReachesShippedInOrder() {
        settings.mode(OutboxMode.SAFE);
        String id = given()
                .contentType(ContentType.JSON)
                .body(Map.of("customerRef", "ada"))
                .post("/orders")
                .then()
                .statusCode(201)
                .body("status", equalTo("CREATED"))
                .body("version", equalTo(1))
                .extract()
                .path("id");

        given()
                .contentType(ContentType.JSON)
                .body(Map.of("trackingNumber", "too-soon"))
                .post("/orders/{id}/shipment", id)
                .then()
                .statusCode(409);

        given()
                .contentType(ContentType.JSON)
                .body(Map.of("paymentRef", "pay-ada"))
                .post("/orders/{id}/payment", id)
                .then()
                .statusCode(200)
                .body("status", equalTo("PAYMENT_CONFIRMED"));

        given()
                .contentType(ContentType.JSON)
                .body(Map.of("trackingNumber", "track-ada"))
                .post("/orders/{id}/shipment", id)
                .then()
                .statusCode(200)
                .body("status", equalTo("SHIPPED"));

        given()
                .get("/orders/{id}", UUID.randomUUID())
                .then()
                .statusCode(404);

        UUID orderId = UUID.fromString(id);
        OrderView view = awaitSettled(orderId);
        assertEquals(List.of(1L, 2L, 3L), view.relayVersions(), view.toString());
        assertEquals(List.of(1L, 2L, 3L), appliedVersions(view), view.toString());
        assertEquals("SHIPPED", view.projectionStatus(), view.toString());
        assertFalse(view.regressed(), view.toString());
        assertEquals(List.of(orderId.toString(), orderId.toString(), orderId.toString()), view.relayKeys());
        assertTrue(view.written().stream().allMatch(event -> event.sourceCommitTime() != null));
    }

    @Test
    @Order(2)
    void safeConsumerParksGapsAndIgnoresDuplicates() {
        UUID orderId = UUID.randomUUID();
        Instant now = Instant.now();
        EventPayload created = event(orderId, 1, "OrderCreated", "gap-customer", null, null, now);
        EventPayload paid = event(orderId, 2, "PaymentConfirmed", null, "pay-gap", null, now);
        EventPayload shipped = event(orderId, 3, "OrderShipped", null, null, "track-gap", now);

        projection.applySafely(created);
        ProjectionView afterCreate = query.projection(orderId);
        assertEquals(List.of(1L), afterCreate.appliedVersions());
        assertEquals("CREATED", afterCreate.status());
        assertTrue(afterCreate.parkedVersions().isEmpty());

        projection.applySafely(shipped);
        ProjectionView parked = query.projection(orderId);
        assertEquals(List.of(1L), parked.appliedVersions());
        assertEquals(List.of(3L), parked.parkedVersions());
        assertEquals("CREATED", parked.status());
        assertFalse(parked.regressed());

        projection.applySafely(shipped);
        assertEquals(List.of(3L), query.projection(orderId).parkedVersions());

        projection.applySafely(paid);
        ProjectionView drained = query.projection(orderId);
        assertEquals(List.of(1L, 2L, 3L), drained.appliedVersions());
        assertEquals("SHIPPED", drained.status());
        assertEquals(3L, drained.appliedVersion());
        assertTrue(drained.parkedVersions().isEmpty());
        assertFalse(drained.regressed());

        projection.applySafely(created);
        projection.applySafely(paid);
        assertEquals(List.of(1L, 2L, 3L), query.projection(orderId).appliedVersions());
    }

    @Test
    @Order(3)
    void generatedIdIsNotCommitOrder() throws Exception {
        UUID firstEvent = UUID.randomUUID();
        UUID secondEvent = UUID.randomUUID();
        UUID firstAggregate = UUID.randomUUID();
        UUID secondAggregate = UUID.randomUUID();
        try (Connection earlier = dataSource.getConnection();
             Connection later = dataSource.getConnection();
             Connection reader = dataSource.getConnection()) {
            earlier.setAutoCommit(false);
            later.setAutoCommit(false);
            reader.setAutoCommit(true);
            long firstId = insertPublished(earlier, firstEvent, firstAggregate);
            long secondId = insertPublished(later, secondEvent, secondAggregate);
            later.commit();

            assertTrue(secondId > firstId, "identity values increase with the insert, not the commit");
            assertFalse(visible(reader, firstEvent), "the uncommitted lower id must stay invisible");
            assertTrue(visible(reader, secondEvent), "the committed higher id is already visible");

            earlier.commit();
            assertTrue(visible(reader, firstEvent));
            assertTrue(visible(reader, secondEvent));
        }
    }

    @Test
    @Order(4)
    void duplicateAggregateVersionIsRejected() throws Exception {
        UUID aggregateId = UUID.randomUUID();
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(true);
            insertPublished(connection, UUID.randomUUID(), aggregateId);
            SQLException failure = assertThrows(SQLException.class,
                    () -> insertPublished(connection, UUID.randomUUID(), aggregateId));
            assertEquals("23505", failure.getSQLState());
        }
    }

    @Test
    @Order(5)
    void naiveModeAppliesEventsOutOfOrder() {
        settings.mode(OutboxMode.NAIVE);
        List<UUID> ids = demo.burst(8, "naive");
        for (UUID id : ids) {
            OrderView view = awaitSettled(id);
            assertEquals(List.of(3L, 2L, 1L), view.relayVersions(), view.toString());
            assertEquals(List.of(3L, 2L, 1L), appliedVersions(view), view.toString());
            assertEquals("CREATED", view.projectionStatus(), view.toString());
            assertTrue(view.regressed(), view.toString());
            assertEquals(3, new HashSet<>(view.relayKeys()).size(), view.toString());
            for (String key : view.relayKeys()) {
                assertNotEquals(id.toString(), key, view.toString());
            }
        }
    }

    @Test
    @Order(6)
    void safeModeStaysOrderedUnderConcurrentPublishers() {
        settings.mode(OutboxMode.SAFE);
        List<UUID> ids = demo.burst(12, "safe");
        for (UUID id : ids) {
            OrderView view = awaitSettled(id);
            assertEquals(List.of(1L, 2L, 3L), view.relayVersions(), view.toString());
            assertEquals(List.of(1L, 2L, 3L), appliedVersions(view), view.toString());
            assertEquals("SHIPPED", view.projectionStatus(), view.toString());
            assertFalse(view.regressed(), view.toString());
            assertEquals(List.of(id.toString(), id.toString(), id.toString()), view.relayKeys(), view.toString());
            assertTrue(view.parkedVersions().isEmpty(), view.toString());
        }

        Map<UUID, List<ConsumerRecord<String, String>>> records = readTopic(new HashSet<>(ids));
        for (UUID id : ids) {
            List<ConsumerRecord<String, String>> rows = records.get(id);
            assertEquals(3, rows.size(), "missing kafka records for " + id);
            int partition = rows.get(0).partition();
            List<Long> versions = new ArrayList<>();
            for (ConsumerRecord<String, String> row : rows) {
                assertEquals(partition, row.partition(), "aggregate " + id + " split across partitions");
                assertEquals(id.toString(), row.key());
                versions.add(codec.decode(row.value()).aggregateVersion());
            }
            assertEquals(List.of(1L, 2L, 3L), versions);
        }

        UUID duplicateTarget = ids.get(0);
        long before = consumed.get();
        sender.send(duplicateTarget.toString(), query.payload(duplicateTarget, 2), null);
        await().atMost(Duration.ofSeconds(20)).until(() -> consumed.get() > before);
        OrderView afterDuplicate = query.inspect(duplicateTarget);
        assertEquals(List.of(1L, 2L, 3L), appliedVersions(afterDuplicate), afterDuplicate.toString());
        assertEquals("SHIPPED", afterDuplicate.projectionStatus());
        assertFalse(afterDuplicate.regressed());
    }

    @Test
    @Order(7)
    void asyncConsumerReordersAnAlreadyOrderedBatch() {
        UUID orderId = UUID.randomUUID();
        Instant now = Instant.now();
        naiveAsync.accept(event(orderId, 1, "OrderCreated", "async", null, null, now));
        naiveAsync.accept(event(orderId, 2, "PaymentConfirmed", null, "pay-async", null, now));
        naiveAsync.accept(event(orderId, 3, "OrderShipped", null, null, "track-async", now));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            ProjectionView view = query.projection(orderId);
            assertEquals(List.of(3L, 2L, 1L), view.appliedVersions(), view.toString());
            assertEquals("CREATED", view.status(), view.toString());
            assertTrue(view.regressed(), view.toString());
        });
    }

    private OrderView awaitSettled(UUID id) {
        await().atMost(Duration.ofSeconds(45)).pollInterval(Duration.ofMillis(50)).untilAsserted(() -> {
            OrderView view = query.inspect(id);
            assertEquals(0L, view.pendingOutbox(), view.toString());
            assertEquals(3, view.relayVersions().size(), view.toString());
            assertEquals(3, view.applied().size(), view.toString());
            assertEquals(0, naiveAsync.inFlight(), view.toString());
        });
        return query.inspect(id);
    }

    private static List<Long> appliedVersions(OrderView view) {
        return view.applied().stream().map(event -> event.version()).toList();
    }

    private static EventPayload event(
            UUID orderId,
            long version,
            String type,
            String customerRef,
            String paymentRef,
            String trackingNumber,
            Instant now) {
        return new EventPayload(UUID.randomUUID(), orderId, version, type, now, now, customerRef, paymentRef, trackingNumber);
    }

    private long insertPublished(Connection connection, UUID eventId, UUID aggregateId) throws SQLException {
        Instant now = Instant.now();
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO outbox_event (
                    event_id, aggregate_id, aggregate_version, event_type, payload, shard,
                    recorded_at, source_commit_time, published_at
                ) VALUES (?, ?, 1, 'OrderCreated', '{}', 0, ?, ?, ?)
                RETURNING id
                """)) {
            statement.setObject(1, eventId);
            statement.setObject(2, aggregateId);
            statement.setTimestamp(3, Timestamp.from(now));
            statement.setTimestamp(4, Timestamp.from(now));
            statement.setTimestamp(5, Timestamp.from(now));
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new SQLException("insert did not return an id");
                }
                return rows.getLong(1);
            }
        }
    }

    private static boolean visible(Connection connection, UUID eventId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM outbox_event WHERE event_id = ?")) {
            statement.setObject(1, eventId);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    private Map<UUID, List<ConsumerRecord<String, String>>> readTopic(Set<UUID> ids) {
        Map<UUID, List<ConsumerRecord<String, String>>> found = new HashMap<>();
        try (KafkaConsumer<String, String> consumer = openConsumer()) {
            int partitions = consumer.partitionsFor("order-events").size();
            assertTrue(partitions >= 2,
                    "expected multiple partitions so the key is what co-locates an aggregate, found " + partitions);
            consumer.subscribe(List.of("order-events"));
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (System.nanoTime() < deadline && !complete(found, ids)) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(400))) {
                    EventPayload payload = codec.decode(record.value());
                    if (ids.contains(payload.aggregateId())) {
                        found.computeIfAbsent(payload.aggregateId(), ignored -> new ArrayList<>()).add(record);
                    }
                }
            }
        }
        return found;
    }

    private KafkaConsumer<String, String> openConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "ordering-it-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(properties);
    }

    private static boolean complete(Map<UUID, List<ConsumerRecord<String, String>>> found, Set<UUID> ids) {
        return ids.stream().allMatch(id -> found.getOrDefault(id, List.of()).size() >= 3);
    }
}
