using Microsoft.AspNetCore.Mvc;

namespace Distributed.Store.ReadApi.Controllers
{
    [Route("api/product")]
    [ApiController]
    public class ProductReadController : ControllerBase
    {
        public ProductReadController()
        {

        }
        [Route("getProduct")]
        [HttpGet]
        public IActionResult GetProduct(int id)
        {
            return Ok();
        }
    }
}
