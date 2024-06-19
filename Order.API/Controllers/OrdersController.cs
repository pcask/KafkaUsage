using Microsoft.AspNetCore.Mvc;
using Order.API.Dtos;
using Order.API.Services;

namespace Order.API.Controllers;

[Route("api/[controller]")]
[ApiController]
public class OrdersController(OrderService orderService) : ControllerBase
{
    [HttpPost]
    public async Task<IActionResult> Create(OrderCreateRequestDto request)
    {
        var result = await orderService.Create(request);

        return Ok(result);
    }
}
