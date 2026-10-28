using Microsoft.EntityFrameworkCore;

namespace OrderService;

public enum OrderStatus
{
    Pending,
    Paid,
    PaymentDeclined
}

public class Order
{
    public Guid Id { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public OrderStatus Status { get; set; }
    public string? FailureReason { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
}

public class OrdersDbContext(DbContextOptions<OrdersDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(e =>
        {
            e.ToTable("orders");
            e.HasKey(o => o.Id);
            e.Property(o => o.Id).HasColumnName("id");
            e.Property(o => o.CustomerId).HasColumnName("customer_id").HasMaxLength(64);
            e.Property(o => o.Amount).HasColumnName("amount").HasPrecision(18, 2);
            // Stored as text so psql shows 'Paid' instead of 1.
            e.Property(o => o.Status).HasColumnName("status").HasConversion<string>().HasMaxLength(32);
            e.Property(o => o.FailureReason).HasColumnName("failure_reason").HasMaxLength(256);
            e.Property(o => o.CreatedAtUtc).HasColumnName("created_at_utc");
            e.Property(o => o.UpdatedAtUtc).HasColumnName("updated_at_utc");
        });
    }
}