using Order.API.Extensions;
using Order.API.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddSingleton<IServiceBus, ServiceBus>();
builder.Services.AddScoped<OrderService>(); // OrderService içerisinde Db işlemleri olduğu için scoped olarak ekliyoruz.


var app = builder.Build();

// Uygulama ayağa kalkar kalkmaz belirttiğimiz Topic'in oluşturulması için;
await app.CreateTopicsOrQueuesAsync();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
