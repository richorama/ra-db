using Owin;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web;

namespace RaDb.Http
{
    public class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            var path = AppDomain.CurrentDomain.SetupInformation.ApplicationBase;

            var database = new Database<Entity>(Path.Combine(path, "database"));

            var router = new Router();
            new Controller(router, database);
            new WebServer(router).Configure(app);
        }
    }
}