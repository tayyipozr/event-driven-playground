using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using OrderService;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<OrdersDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Orders")));

builder.Services.Configure<CheckoutOptions>(builder.Configuration.GetSection("Checkout"));

builder.Services.AddHttpClient<PaymentClient>(client =>
{
    client.BaseAddress = new Uri(builder.Configuration["PaymentService:BaseUrl"]
        ?? throw new InvalidOperationException("PaymentService:BaseUrl is not configured"));
    // No timeout set on purpose: HttpClient falls back to its 100-second default.
    // Experiment 2 shows what that means when Payment becomes slow.
});

builder.Services.AddScoped<CheckoutHandler>();

var app = builder.Build();

// Resolving the options here fails fast on an invalid Checkout__Strategy value and logs the active strategy.
var strategy = app.Services.GetRequiredService<IOptions<CheckoutOptions>>().Value.Strategy;
app.Logger.LogInformation("Checkout strategy: {Strategy}", strategy);

// Demo shortcut: create the schema on startup. Production code would use EF Core migrations.
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();
    await db.Database.EnsureCreatedAsync();
}

app.MapPost("/orders", async (CreateOrderRequest request, CheckoutHandler handler, CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(request.CustomerId) || request.Amount <= 0)
    {
        return Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            title: "customerId is required and amount must be positive");
    }

    var result = await handler.CheckoutAsync(request, ct);

    return result.Outcome switch
    {
        CheckoutOutcome.Paid => Results.Created($"/orders/{result.Order.Id}", OrderResponse.From(result.Order)),
        CheckoutOutcome.Declined => Results.Problem(
            statusCode: StatusCodes.Status422UnprocessableEntity,
            title: "Payment declined",
            extensions: OrderInfo(result)),
        _ => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Payment service unavailable",
            detail: result.Error, // Demo only: exposing exception messages leaks internals in production.
            extensions: OrderInfo(result))
    };
});

// Has nothing to do with Payment. Experiment 2 checks whether it survives when Payment is slow.
app.MapGet("/orders/{id:guid}", async (Guid id, OrdersDbContext db, CancellationToken ct) =>
    await db.Orders.AsNoTracking().FirstOrDefaultAsync(o => o.Id == id, ct) is { } order
        ? Results.Ok(OrderResponse.From(order))
        : Results.NotFound());

app.Run();

// "status" is reserved by ProblemDetails (RFC 9457) for the HTTP status code,
// so the order status gets its own key.
static Dictionary<string, object?> OrderInfo(CheckoutResult result) => new()
{
    ["orderId"] = result.Order.Id,
    ["orderStatus"] = result.OrderSaved ? result.Order.Status.ToString() : "NotSaved"
};

public record CreateOrderRequest(string CustomerId, decimal Amount);

public record OrderResponse(Guid Id, string CustomerId, decimal Amount, string Status, string? FailureReason)
{
    public static OrderResponse From(Order order) =>
        new(order.Id, order.CustomerId, order.Amount, order.Status.ToString(), order.FailureReason);
}