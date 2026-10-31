using System.Net;
using Microsoft.Extensions.Options;
using Polly.CircuitBreaker;
using Polly.Timeout;

namespace OrderService;

public enum CheckoutStrategy
{
    CommitThenCall,
    TransactionAroundCall
}

public class CheckoutOptions
{
    public CheckoutStrategy Strategy { get; set; } = CheckoutStrategy.CommitThenCall;
}

public enum CheckoutOutcome
{
    Paid,
    Declined,
    PaymentFailed
}

// OrderSaved is false when the order row was rolled back (TransactionAroundCall after a failed call).
public record CheckoutResult(CheckoutOutcome Outcome, Order Order, bool OrderSaved, string? Error = null);

public class CheckoutHandler(
    OrdersDbContext db,
    PaymentClient payments,
    IOptions<CheckoutOptions> options,
    ILogger<CheckoutHandler> logger)
{
    public Task<CheckoutResult> CheckoutAsync(CreateOrderRequest request, CancellationToken ct) =>
        options.Value.Strategy switch
        {
            CheckoutStrategy.TransactionAroundCall => TransactionAroundCallAsync(request, ct),
            _ => CommitThenCallAsync(request, ct)
        };

    // Naive strategy 1: commit the order, then call Payment over HTTP.
    // Failure mode: if the call fails, the order stays Pending and nothing ever fixes it.
    private async Task<CheckoutResult> CommitThenCallAsync(CreateOrderRequest request, CancellationToken ct)
    {
        var order = NewPendingOrder(request);
        db.Orders.Add(order);
        await db.SaveChangesAsync(ct);
        // COMMIT is done. EF Core has already returned the connection to the pool,
        // so no DB connection is held during the HTTP call below.

        PaymentResult payment;
        try
        {
            payment = await payments.ChargeAsync(order.Id, order.Amount, ct);
        }
        catch (Exception ex) when (IsTechnicalFailure(ex, ct))
        {
            // Marking the order as failed would be a guess: after a timeout the charge may have succeeded.
            // No retry and no background job, so this order stays Pending forever (modules 07 and 08 fix this).
            logger.LogError(ex, "Payment call failed for order {OrderId}, order stays Pending", order.Id);
            return new CheckoutResult(CheckoutOutcome.PaymentFailed, order, OrderSaved: true, ex.Message);
        }

        // A crash right here leaves a captured payment next to a Pending order.
        ApplyPaymentResult(order, payment);

        // CancellationToken.None: the money has already moved, so record the result
        // even if the client has disconnected in the meantime.
        await db.SaveChangesAsync(CancellationToken.None);

        logger.LogInformation("Order {OrderId} finished with status {Status}", order.Id, order.Status);
        return Finished(order);
    }

    // Naive strategy 2: keep a DB transaction open around the HTTP call, then commit or roll back.
    // Failure modes: the connection is held for the whole call (pool exhaustion when Payment is slow),
    // and a rollback after a timeout can leave a "ghost payment": money taken, no order.
    private async Task<CheckoutResult> TransactionAroundCallAsync(CreateOrderRequest request, CancellationToken ct)
    {
        var order = NewPendingOrder(request);

        // BEGIN takes a connection from the pool and keeps it until COMMIT or ROLLBACK.
        // If an exception escapes this method, disposing the transaction rolls it back.
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        db.Orders.Add(order);
        await db.SaveChangesAsync(ct);
        // The row is written but invisible to other connections until COMMIT (READ COMMITTED).

        PaymentResult payment;
        try
        {
            payment = await payments.ChargeAsync(order.Id, order.Amount, ct);
        }
        catch (Exception ex) when (IsTechnicalFailure(ex, ct))
        {
            await tx.RollbackAsync(CancellationToken.None);
            logger.LogError(ex, "Payment call failed for order {OrderId}, order rolled back. A charge may still exist in Payment", order.Id);
            return new CheckoutResult(CheckoutOutcome.PaymentFailed, order, OrderSaved: false, ex.Message);
        }

        // A crash right here: PostgreSQL rolls back the open transaction when the connection drops,
        // so a captured payment is left without any order.
        ApplyPaymentResult(order, payment);
        await db.SaveChangesAsync(CancellationToken.None);
        await tx.CommitAsync(CancellationToken.None);

        logger.LogInformation("Order {OrderId} finished with status {Status}", order.Id, order.Status);
        return Finished(order);
    }

    private static Order NewPendingOrder(CreateOrderRequest request)
    {
        var now = DateTime.UtcNow;
        return new Order
        {
            Id = Guid.NewGuid(),
            CustomerId = request.CustomerId,
            Amount = request.Amount,
            Status = OrderStatus.Pending,
            CreatedAtUtc = now,
            UpdatedAtUtc = now
        };
    }

    private static void ApplyPaymentResult(Order order, PaymentResult payment)
    {
        var paid = payment == PaymentResult.Captured;
        order.Status = paid ? OrderStatus.Paid : OrderStatus.PaymentDeclined;
        order.FailureReason = paid ? null : "Declined by payment service";
        order.UpdatedAtUtc = DateTime.UtcNow;
    }

    private static CheckoutResult Finished(Order order) =>
        new(order.Status == OrderStatus.Paid ? CheckoutOutcome.Paid : CheckoutOutcome.Declined, order, OrderSaved: true);

    // HttpRequestException: connection refused, DNS failure, or a non-success status thrown by PaymentClient.
    // TaskCanceledException while the client is still connected: HttpClient.Timeout (100 s safety net) expired.
    // TimeoutRejectedException: the Polly timeout in the resilience pipeline fired. It does NOT derive from
    // OperationCanceledException, so without this case it would escape the catch block and become a 500.
    // BrokenCircuitException: the circuit breaker is open and the call was never sent to Payment.
    // Without this case an open circuit would surface as an empty 500 instead of a 503.
    private static bool IsTechnicalFailure(Exception ex, CancellationToken ct) =>
        !ct.IsCancellationRequested
        && ex is HttpRequestException or TaskCanceledException or TimeoutRejectedException or BrokenCircuitException;
}

public enum PaymentResult
{
    Captured,
    Declined
}

// Typed HttpClient. The only way Order talks to Payment.
public class PaymentClient(HttpClient http)
{
    public const string IdempotencyKeyHeader = "Idempotency-Key";

    public async Task<PaymentResult> ChargeAsync(Guid orderId, decimal amount, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/payments")
        {
            Content = JsonContent.Create(new ChargeRequest(orderId, amount))
        };

        // One key per business operation: the order id, not a new Guid per attempt.
        // The header is set here, before the request enters the resilience pipeline,
        // so every retry carries the same key and Payment can recognize a repeated request.
        // The same key also protects a later recovery job that retries this order (modules 07 and 08).
        request.Headers.Add(IdempotencyKeyHeader, orderId.ToString());

        using var response = await http.SendAsync(request, ct);

        // 201 (new charge) and 200 (replay of an earlier charge) both mean the money was taken once.
        if (response.IsSuccessStatusCode)
        {
            return PaymentResult.Captured;
        }

        // 422 is a business answer ("card declined"), not a technical failure. Retrying it would not help.
        if (response.StatusCode == HttpStatusCode.UnprocessableEntity)
        {
            return PaymentResult.Declined;
        }

        // 400 and 409 are contract bugs on our side. They still end up as a 503 here (known limitation, see README).
        throw new HttpRequestException($"Payment service returned {(int)response.StatusCode}", inner: null, response.StatusCode);
    }

    private sealed record ChargeRequest(Guid OrderId, decimal Amount);
}