using Microsoft.AspNetCore.Mvc;

namespace Distributed.Store.WriteApi.Controllers
{
    [Route("api/product")]
    [ApiController]
    public class ProductWriteController : ControllerBase
    {
        public ProductWriteController()
        {

        }
        [HttpPost]
        [Route("addProduct")]
        public ActionResult AddProduct()
        {
            return Ok(new { ProductId = 1 });
        }
    }
}
