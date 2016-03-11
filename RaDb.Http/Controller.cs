using Microsoft.Owin;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace RaDb.Http
{
    public class Controller
    {
        Database<Entity> db;

        public Controller(Router router, Database<Entity> database)
        {
            this.db = database;

            Action<string, Func<IOwinContext, IDictionary<string, string>, Task>> add = router.Add;
            add("/", Index);
            add("/key/:key", Key);
            add("/search/:from/:to", Search);
        }

        Task Index(IOwinContext context, IDictionary<string, string> parameters)
        {
            return context.ReturnJson(new { hello = "world" });
        }

        Task Key(IOwinContext context, IDictionary<string, string> parameters)
        {
            var key = parameters["key"];
            switch (context.Request.Method)
            {
                case "GET":
                    var entity = this.db.Get(key);
                    return context.ReturnJsonString(entity.Value);
                case "POST":
                case "PUT":
                    using (var reader = new StreamReader(context.Request.Body))
                    {
                        var newEntity = new Entity { Value = reader.ReadToEnd() };
                        this.db.Set(key, newEntity);
                        return context.ReturnJsonString(newEntity.Value);
                    }
                case "DEL":
                    this.db.Del(key);
                    return Task.FromResult(0);
            }
            throw new ArgumentException("verb not supported");
        }

        Task Search(IOwinContext context, IDictionary<string, string> parameters)
        {
            var from = parameters["from"];
            var to = parameters["to"];
            IEnumerable<KeyValue<Entity>> search = this.db.Between(from, to).OrderBy(x => x.Key);
            if (null != context.Request.Query["top"])
            {
                search = search.Take(int.Parse(context.Request.Query["top"]));
            }

            context.Response.Write("[");
            var first = true;
            foreach (var item in search)
            {
                var firstCharacter = first ? "" : ",";
                context.Response.Write($"{firstCharacter}{{\"Key\":\"{item.Key}\",\"Value\":{item.Value}}}");
                first = false;
            }
            context.Response.Write("]");
            return Task.FromResult(0);
        }
    }
}