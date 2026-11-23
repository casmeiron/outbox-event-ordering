package io.orderedoutbox.api;

import io.orderedoutbox.api.Views.BurstRequest;
import io.orderedoutbox.api.Views.BurstResponse;
import io.orderedoutbox.demo.DemoService;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/demo")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DemoResource {

    @Inject
    DemoService demo;

    @POST
    @Path("/burst")
    public BurstResponse burst(BurstRequest request) {
        if (request == null) {
            throw new ApiException.BadRequest("orders must be between 1 and 500");
        }
        return new BurstResponse(demo.burst(request.orders(), request.customerPrefix()));
    }
}
