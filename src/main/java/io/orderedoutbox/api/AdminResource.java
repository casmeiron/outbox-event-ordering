package io.orderedoutbox.api;

import io.orderedoutbox.api.Views.ModeRequest;
import io.orderedoutbox.api.Views.SettingsView;
import io.orderedoutbox.api.Views.Summary;
import io.orderedoutbox.outbox.OutboxMode;
import io.orderedoutbox.outbox.OutboxRelay;
import io.orderedoutbox.outbox.OutboxSettings;
import io.orderedoutbox.query.OrderQuery;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/admin")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class AdminResource {

    @Inject
    OutboxSettings settings;

    @Inject
    OutboxRelay relay;

    @Inject
    OrderQuery query;

    @GET
    @Path("/settings")
    public SettingsView settings() {
        return view();
    }

    @PUT
    @Path("/mode")
    public SettingsView mode(ModeRequest request) {
        if (request == null || request.mode() == null || request.mode().isBlank()) {
            throw new ApiException.BadRequest("mode must be naive or safe");
        }
        settings.mode(parse(request.mode()));
        return view();
    }

    @POST
    @Path("/publishers/pause")
    public SettingsView pause() {
        relay.pauseAndAwait();
        return view();
    }

    @POST
    @Path("/publishers/resume")
    public SettingsView resume() {
        relay.resume();
        return view();
    }

    @GET
    @Path("/summary")
    public Summary summary() {
        return query.summary();
    }

    private SettingsView view() {
        return new SettingsView(
                settings.mode().name().toLowerCase(),
                settings.paused(),
                settings.workers(),
                settings.shards(),
                settings.stallLowerVersions(),
                settings.naiveAsyncConsumer());
    }

    private static OutboxMode parse(String mode) {
        try {
            return OutboxMode.valueOf(mode.trim().toUpperCase());
        } catch (IllegalArgumentException exception) {
            throw new ApiException.BadRequest("mode must be naive or safe");
        }
    }
}
