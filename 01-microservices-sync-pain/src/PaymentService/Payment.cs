using Microsoft.EntityFrameworkCore;

namespace PaymentService;

public class Payment
{
    public const int IdempotencyKeyMaxLength = 255;
    public const string IdempotencyKeyIndex = "ux_payments_idempotency_key";

    public Guid Id { get; set; }
    public Guid OrderId { get; set; }
    public decimal Amount { get; set; }
    public DateTime CreatedAtUtc { get; set; }

    // Chosen by the caller, one per business operation (not per HTTP attempt).
    // Every retry of the same operation must carry the same key.
    public string IdempotencyKey { get; set; } = string.Empty;
}

public class PaymentsDbContext(DbContextOptions<PaymentsDbContext> options) : DbContext(options)
{
    public DbSet<Payment> Payments => Set<Payment>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Payment>(e =>
        {
            e.ToTable("payments");
            e.HasKey(p => p.Id);
            e.Property(p => p.Id).HasColumnName("id");
            e.Property(p => p.OrderId).HasColumnName("order_id");
            e.Property(p => p.Amount).HasColumnName("amount").HasPrecision(18, 2);
            e.Property(p => p.CreatedAtUtc).HasColumnName("created_at_utc");
            e.Property(p => p.IdempotencyKey)
                .HasColumnName("idempotency_key")
                .HasMaxLength(Payment.IdempotencyKeyMaxLength)
                .IsRequired();

            // The database is the referee for "have we seen this request before?".
            // A check-then-insert in code would race; a unique index cannot.
            e.HasIndex(p => p.IdempotencyKey)
                .IsUnique()
                .HasDatabaseName(Payment.IdempotencyKeyIndex);

            // Still non-unique on purpose. "Same request" is decided by the idempotency key.
            // "Only one payment per order" would be a separate business rule (design A in the README).
            e.HasIndex(p => p.OrderId);
        });
    }
}