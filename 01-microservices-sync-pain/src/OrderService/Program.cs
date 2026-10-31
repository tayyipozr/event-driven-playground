using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Http.Resilience;
using Microsoft.Extensions.Options;
using OrderService;
using Polly;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<OrdersDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Orders")));

builder.Services.Configure<CheckoutOptions>(builder.Configuration.GetSection("Checkout"));

// Payment normally answers in tens of milliseconds. In production these values come from measured p99 latency.
var attemptTimeout = TimeSpan.FromSeconds(2);
// Upper bound for the whole call: 3 attempts x 2 s + backoff delays fits inside it.
var totalTimeout = TimeSpan.FromSeconds(8);

builder.Services.AddHttpClient<PaymentClient>(client =>
{
    client.BaseAddress = new Uri(builder.Configuration["PaymentService:BaseUrl"]
        ?? throw new InvalidOperationException("PaymentService:BaseUrl is not configured"));
    // HttpClient.Timeout keeps its 100-second default. It is only a last-resort safety net now,
    // because the resilience pipeline below cancels the call much earlier.
})
    .AddResilienceHandler("payment", (pipeline, context) =>
    {
        var logger = context.ServiceProvider.GetRequiredService<ILoggerFactory>().CreateLogger("PaymentResilience");

        // Strategies run outer to inner in the order they are added.
        // Same order as AddStandardResilienceHandler(): total timeout -> retry -> circuit breaker -> attempt timeout.

        // 1. Total timeout: caps all attempts plus the delays between them.
        pipeline.AddTimeout(totalTimeout);

        // 2. Retry: by default handles HttpRequestException, TimeoutRejectedException, 408, 429 and 5xx.
        //    422 (declined) is a business answer and is never retried.
        //    Retrying POST /payments is safe only because PaymentClient sends an Idempotency-Key (the order id).
        //    Without the key, a timed-out attempt that already charged would be charged again (experiment 3).
        //    Without an idempotency key, production code would call DisableForUnsafeHttpMethods().
        pipeline.AddRetry(new HttpRetryStrategyOptions
        {
            MaxRetryAttempts = 2,
            Delay = TimeSpan.FromMilliseconds(200),
            BackoffType = DelayBackoffType.Exponential,
            UseJitter = true, // spreads retries so many clients don't hit a recovering service at the same moment
            OnRetry = args =>
            {
                logger.LogWarning("Retrying Payment call: retry {RetryNumber} in {DelayMs:F0} ms, reason {Reason}",
                    args.AttemptNumber + 1, args.RetryDelay.TotalMilliseconds, Describe(args.Outcome));
                return default;
            }
        });

        // 3. Circuit breaker: inside the retry, so every single attempt is counted.
        //    When open, calls fail in milliseconds with BrokenCircuitException and never reach Payment.
        //    The state lives in this process only: each Order replica would have its own breaker.
        //    Demo values. Production values come from real traffic (standard handler: 0.1 / 100 / 30 s / 5 s).
        pipeline.AddCircuitBreaker(new HttpCircuitBreakerStrategyOptions
        {
            FailureRatio = 0.5,                          // open when at least half of the attempts fail...
            MinimumThroughput = 4,                       // ...and at least 4 attempts were seen...
            SamplingDuration = TimeSpan.FromSeconds(10), // ...within the last 10 seconds
            BreakDuration = TimeSpan.FromSeconds(15),    // stay open long enough to observe it by hand
            OnOpened = args =>
            {
                logger.LogWarning("Circuit OPENED for {BreakSeconds:F0} s, last outcome {Outcome}",
                    args.BreakDuration.TotalSeconds, Describe(args.Outcome));
                return default;
            },
            OnHalfOpened = _ =>
            {
                logger.LogWarning("Circuit HALF-OPEN: the next call is a trial");
                return default;
            },
            OnClosed = _ =>
            {
                logger.LogWarning("Circuit CLOSED: calls to Payment flow again");
                return default;
            }
        });

        // 4. Attempt timeout: each single try gets 2 s, so a timed-out attempt can be retried
        //    and also counts as a failure for the circuit breaker.
        pipeline.AddTimeout(attemptTimeout);
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

// Short description of an attempt result for logs: exception type or HTTP status code.
static string Describe(Outcome<HttpResponseMessage> outcome) =>
    outcome.Exception?.GetType().Name ?? $"HTTP {(int?)outcome.Result?.StatusCode}";

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