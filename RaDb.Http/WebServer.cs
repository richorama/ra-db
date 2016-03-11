using Microsoft.Owin;
using Owin;
using System;
using System.Threading.Tasks;

namespace RaDb.Http
{
    public class WebServer
    {
        public WebServer(Router router)
        {
            this.Router = router;
        }

        public Router Router { get; private set; }

        async Task HandleRequest(IOwinContext context, Func<Task> func)
        {
            var result = this.Router.Match(context.Request.Path.Value);
            if (null != result)
            {
                try
                {
                    await result(context);
                    return;
                }
                catch (Exception ex)
                {
                    await context.ReturnError(ex);
                }
            }

            context.Response.StatusCode = 404;
        }


        public void Configure(IAppBuilder app)
        {
            app.Use(HandleRequest);
        }
    }
}