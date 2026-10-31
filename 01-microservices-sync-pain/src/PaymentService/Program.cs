using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using PaymentService;

const decimal DeclineAbove = 1000m;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<PaymentsDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Payments")));
builder.Services.AddSingleton<ChaosSettings>();

var app = builder.Build();

// Demo shortcut: create the schema on startup. Production code would use EF Core migrations.
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();
    await db.Database.EnsureCreatedAsync();
}

// HttpContext.RequestAborted is deliberately NOT used in this handler.
// Like a real payment provider, the server finishes the charge even if the caller has already given up.
app.MapPost("/payments", async (
    CreatePaymentRequest request,
    [FromHeader(Name = "Idempotency-Key")] string? idempotencyKey,
    PaymentsDbContext db,
    ChaosSettings chaos,
    ILogger<Program> logger) =>
{
    // Malformed requests are rejected first. They never reach chaos or the database.
    if (string.IsNullOrWhiteSpace(idempotencyKey) || idempotencyKey.Length > Payment.IdempotencyKeyMaxLength)
    {
        return Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            title: $"The Idempotency-Key header is required (max {Payment.IdempotencyKeyMaxLength} characters)");
    }

    if (request.Amount <= 0)
    {
        return Results.Problem(statusCode: StatusCodes.Status400BadRequest, title: "Amount must be positive");
    }

    // The column is numeric(18,2). Without this check PostgreSQL would round 250.555 to 250.56,
    // and an honest retry of the same request would later look like a different amount (409).
    if (decimal.Round(request.Amount, 2) != request.Amount)
    {
        return Results.Problem(statusCode: StatusCodes.Status400BadRequest, title: "Amount can have at most 2 decimal places");
    }

    var config = chaos.Current;

    if (Random.Shared.NextDouble() < config.FailureRate)
    {
        logger.LogWarning("Chaos: simulated failure for order {OrderId}", request.OrderId);
        return Results.Problem(statusCode: StatusCodes.Status500InternalServerError, title: "Simulated failure");
    }

    if (request.Amount > DeclineAbove)
    {
        logger.LogInformation("Payment declined for order {OrderId}, amount {Amount}", request.OrderId, request.Amount);
        return Results.Problem(
            statusCode: StatusCodes.Status422UnprocessableEntity,
            title: "Payment declined",
            detail: $"Amounts above {DeclineAbove} are declined.");
    }

    var payment = new Payment
    {
        Id = Guid.NewGuid(),
        OrderId = request.OrderId,
        Amount = request.Amount,
        CreatedAtUtc = DateTime.UtcNow,
        IdempotencyKey = idempotencyKey
    };
    db.Payments.Add(payment);

    try
    {
        await db.SaveChangesAsync();
    }
    catch (DbUpdateException ex) when (ex.InnerException is PostgresException
    {
        SqlState: PostgresErrorCodes.UniqueViolation,
        ConstraintName: Payment.IdempotencyKeyIndex
    })
    {
        // This key was used before, either by an earlier attempt or by a concurrent request that won the race.
        // The failed entity is still tracked as Added. Clear it so no later SaveChanges retries the insert.
        db.ChangeTracker.Clear();

        var existing = await db.Payments.AsNoTracking().SingleAsync(p => p.IdempotencyKey == idempotencyKey);

        // Same key with a different payload is a client bug, not a retry.
        // 409 instead of 422: in this API 422 already means "declined", and Order would misread it.
        if (existing.OrderId != request.OrderId || existing.Amount != request.Amount)
        {
            logger.LogWarning("Idempotency key {IdempotencyKey} reused with a different payload", idempotencyKey);
            return Results.Problem(
                statusCode: StatusCodes.Status409Conflict,
                title: "Idempotency key reused with a different request",
                detail: "Use a new key for a new payment.");
        }

        // Replay: no new charge and no chaos delay. The caller gets the original result immediately.
        logger.LogInformation("Replay: payment {PaymentId} for order {OrderId} returned for idempotency key {IdempotencyKey}",
            existing.Id, existing.OrderId, idempotencyKey);
        return Results.Ok(new PaymentResponse(existing.Id, existing.OrderId, existing.Amount));
    }

    logger.LogInformation("Payment {PaymentId} captured for order {OrderId}", payment.Id, payment.OrderId);

    // The delay comes AFTER the commit: the money is already taken, only the response is late.
    if (config.DelayMs > 0)
    {
        await Task.Delay(config.DelayMs);
    }

    return Results.Created($"/payments/{payment.Id}", new PaymentResponse(payment.Id, payment.OrderId, payment.Amount));
});

// The only supported way for other services to read payment data (no shared database).
app.MapGet("/payments", async (Guid orderId, PaymentsDbContext db) =>
    await db.Payments
        .AsNoTracking()
        .Where(p => p.OrderId == orderId)
        .OrderBy(p => p.CreatedAtUtc)
        .Select(p => new PaymentResponse(p.Id, p.OrderId, p.Amount))
        .ToListAsync());

app.MapGet("/admin/chaos", (ChaosSettings chaos) => chaos.Current);

app.MapPost("/admin/chaos", (ChaosConfig request, ChaosSettings chaos, ILogger<Program> logger) =>
{
    if (request.DelayMs < 0 || request.FailureRate is < 0 or > 1)
    {
        return Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            title: "delayMs must be >= 0 and failureRate must be between 0 and 1");
    }

    chaos.Current = request;
    logger.LogWarning("Chaos settings changed: delay {DelayMs} ms, failure rate {FailureRate}", request.DelayMs, request.FailureRate);
    return Results.Ok(request);
});

app.Run();

public record CreatePaymentRequest(Guid OrderId, decimal Amount);

public record PaymentResponse(Guid Id, Guid OrderId, decimal Amount);

public record ChaosConfig(int DelayMs, double FailureRate);

// Shared across all requests. The whole config is swapped as one immutable object,
// so a request never sees a half-updated combination (new delay with old failure rate).
public class ChaosSettings
{
    private volatile ChaosConfig _current = new(0, 0);

    public ChaosConfig Current
    {
        get => _current;
        set => _current = value;
    }
}