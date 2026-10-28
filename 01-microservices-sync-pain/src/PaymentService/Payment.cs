using Microsoft.EntityFrameworkCore;

namespace PaymentService;

public class Payment
{
    public Guid Id { get; set; }
    public Guid OrderId { get; set; }
    public decimal Amount { get; set; }
    public DateTime CreatedAtUtc { get; set; }
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

            // Plain (non-unique) index on purpose.
            // Experiment 3 shows what goes wrong when the same order can be paid twice.
            e.HasIndex(p => p.OrderId);
        });
    }
}