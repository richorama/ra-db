using Microsoft.Owin;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Web;

namespace RaDb.Http
{
    public static class ExtensionMethods
    {
        public static async Task ReturnFile(this IOwinContext context, string name, string contentType)
        {
            context.Response.ContentType = contentType;
            var assembly = Assembly.GetExecutingAssembly();
            using (var stream = assembly.GetManifestResourceStream($"RaDb.Http.{name}"))
            using (var reader = new StreamReader(stream))
            {
                var content = await reader.ReadToEndAsync();
                await context.Response.WriteAsync(content);
            }
        }

        public static Task ReturnJson(this IOwinContext context, object value)
        {
            context.Response.ContentType = "application/json";
            return context.Response.WriteAsync(JsonConvert.SerializeObject(value));
        }

        public static Task ReturnJsonString(this IOwinContext context, string value)
        {
            context.Response.ContentType = "application/json";
            return context.Response.WriteAsync(value);
        }

        public static Task ReturnUnauthorised(this IOwinContext context)
        {
            context.Response.StatusCode = 401;
            context.Response.ReasonPhrase = "Unauthorized";
            context.Response.Headers.Add("WWW-Authenticate", new string[] { "Basic realm=\"RaDb\"" });
            return Task.FromResult(0);
        }

        public static Task ReturnError(this IOwinContext context, Exception ex)
        {
            context.Response.ContentType = "application/json";
            context.Response.StatusCode = 500;
            return context.Response.WriteAsync(ex.ToString());
        }
    }
}