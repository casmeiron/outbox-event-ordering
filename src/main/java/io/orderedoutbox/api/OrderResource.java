package io.orderedoutbox.api;

import io.orderedoutbox.api.Views.OrderView;
import io.orderedoutbox.domain.OrderService;
import io.orderedoutbox.domain.OrderState;
import io.orderedoutbox.query.OrderQuery;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.UUID;

@Path("/orders")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class OrderResource {

    @Inject
    OrderService orders;

    @Inject
    OrderQuery query;

    @POST
    public Response create(CreateOrderRequest request) {
        OrderState created = orders.create(request == null ? null : request.customerRef());
        return Response.status(Response.Status.CREATED).entity(query.inspect(created.id())).build();
    }

    @POST
    @Path("/{id}/payment")
    public OrderView confirmPayment(@PathParam("id") UUID id, PaymentRequest request) {
        orders.confirmPayment(id, request == null ? null : request.paymentRef());
        return query.inspect(id);
    }

    @POST
    @Path("/{id}/shipment")
    public OrderView ship(@PathParam("id") UUID id, ShipmentRequest request) {
        orders.ship(id, request == null ? null : request.trackingNumber());
        return query.inspect(id);
    }

    @GET
    @Path("/{id}")
    public OrderView get(@PathParam("id") UUID id) {
        return query.inspect(id);
    }

    public record CreateOrderRequest(String customerRef) {
    }

    public record PaymentRequest(String paymentRef) {
    }

    public record ShipmentRequest(String trackingNumber) {
    }
}
