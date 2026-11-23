package io.orderedoutbox.domain;

public final class OrderStatuses {

    private OrderStatuses() {
    }

    public static OrderStatus after(String eventType) {
        return switch (eventType) {
            case "OrderCreated" -> OrderStatus.CREATED;
            case "PaymentConfirmed" -> OrderStatus.PAYMENT_CONFIRMED;
            case "OrderShipped" -> OrderStatus.SHIPPED;
            default -> throw new IllegalArgumentException("Unknown event type " + eventType);
        };
    }

    public static String typeOf(OrderEvent event) {
        return switch (event) {
            case OrderCreated ignored -> "OrderCreated";
            case PaymentConfirmed ignored -> "PaymentConfirmed";
            case OrderShipped ignored -> "OrderShipped";
        };
    }
}
